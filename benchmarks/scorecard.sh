#!/usr/bin/env bash
#
# scorecard.sh -- Three-tier scoring for Golars benchmark runs.
#
# Tier 1 (Correctness):  go test ./... pass/fail
# Tier 2 (Runability):   benchmarks complete without panic (-benchtime=1x)
# Tier 3 (Performance):  ns/op, B/op, allocs/op vs reference baseline
#
# Output: scorecard.json in the current directory.
#
# Usage:
#   bash benchmarks/scorecard.sh               # run from repo root
#   GOLARS_BENCH_PACKAGES="./benchmarks/agg/"  # restrict to one package
#   GOLARS_BENCH_TIME="5x"                     # override T3 benchtime
#   GOLARS_BASELINE="benchmarks/reference_baseline.json"  # baseline path

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

BENCH_PACKAGES="${GOLARS_BENCH_PACKAGES:-./benchmarks/...}"
BENCH_TIME="${GOLARS_BENCH_TIME:-3x}"
BASELINE="${GOLARS_BASELINE:-benchmarks/reference_baseline.json}"

TMPDIR_SC="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_SC"' EXIT

# --------------------------------------------------------------------------
# Tier 1: Correctness
# --------------------------------------------------------------------------

echo "=== Tier 1: Correctness ===" >&2

BUILD_STATUS="pass"
if ! go build ./... >/dev/null 2>&1; then
    BUILD_STATUS="fail"
fi

RACE_STATUS="skip"
if [ "$BUILD_STATUS" = "pass" ]; then
    if go test -race -count=1 ./... >"$TMPDIR_SC/race.txt" 2>&1; then
        RACE_STATUS="pass"
    else
        RACE_STATUS="fail"
    fi
fi

T1_PASS=0; T1_FAIL=0; T1_SKIP=0
if [ "$BUILD_STATUS" = "pass" ]; then
    go test -json -count=1 ./... >"$TMPDIR_SC/test.json" 2>&1 || true

    # Count test-level (not package-level) results.
    # grep -c returns 0 (count) on no match with exit code 1; handle gracefully.
    T1_PASS=$(grep '"Test":' "$TMPDIR_SC/test.json" | grep -c '"Action":"pass"' || true)
    T1_FAIL=$(grep '"Test":' "$TMPDIR_SC/test.json" | grep -c '"Action":"fail"' || true)
    T1_SKIP=$(grep '"Test":' "$TMPDIR_SC/test.json" | grep -c '"Action":"skip"' || true)

    # Package-level (lines without "Test" key)
    PKG_PASS=$(grep -v '"Test":' "$TMPDIR_SC/test.json" | grep -c '"Action":"pass"' || true)
    PKG_FAIL=$(grep -v '"Test":' "$TMPDIR_SC/test.json" | grep -c '"Action":"fail"' || true)
else
    PKG_PASS=0; PKG_FAIL=0
fi

T1_TOTAL=$((T1_PASS + T1_FAIL + T1_SKIP))
if [ "$T1_TOTAL" -gt 0 ]; then
    T1_RATE=$(awk "BEGIN {printf \"%.1f\", ($T1_PASS / $T1_TOTAL) * 100}")
else
    T1_RATE="0.0"
fi

echo "  build=$BUILD_STATUS race=$RACE_STATUS tests=$T1_PASS/$T1_TOTAL ($T1_RATE%)" >&2

# --------------------------------------------------------------------------
# Tier 2: Benchmark Runability  (-benchtime=1x, fast)
# --------------------------------------------------------------------------

echo "=== Tier 2: Benchmark Runability ===" >&2

T2_PASS=0; T2_FAIL=0; T2_SKIP=0

if [ "$BUILD_STATUS" = "pass" ]; then
    # Run benchmarks with -benchtime=1x (single iteration, fast).
    # -run=^$ skips unit tests. Capture both stdout and stderr.
    go test -bench=. -benchtime=1x -run='^$' -json $BENCH_PACKAGES \
        >"$TMPDIR_SC/bench_run.json" 2>&1 || true

    # go test -json does not emit Action=pass for individual benchmarks.
    # Instead, each benchmark gets Action=run, and failures get Action=fail.
    # Count run events as "attempted", fail events as "failed".
    T2_RUN=$(grep '"Test":"Benchmark' "$TMPDIR_SC/bench_run.json" | grep -c '"Action":"run"' || true)
    T2_FAIL=$(grep '"Test":"Benchmark' "$TMPDIR_SC/bench_run.json" | grep -c '"Action":"fail"' || true)

    # Skipped benchmarks: benchmarks that were skipped via b.Skip()
    T2_SKIP=$(grep '"Test":"Benchmark' "$TMPDIR_SC/bench_run.json" | grep -c '"Action":"skip"' || true)

    T2_PASS=$((T2_RUN - T2_FAIL - T2_SKIP))
fi

T2_TOTAL=$((T2_PASS + T2_FAIL + T2_SKIP))
if [ "$T2_TOTAL" -gt 0 ]; then
    T2_RATE=$(awk "BEGIN {printf \"%.1f\", ($T2_PASS / $T2_TOTAL) * 100}")
else
    T2_RATE="0.0"
fi

echo "  bench_pass=$T2_PASS bench_fail=$T2_FAIL bench_skip=$T2_SKIP ($T2_RATE%)" >&2

# --------------------------------------------------------------------------
# Tier 3: Performance  (-benchtime=Nx, parsed)
# --------------------------------------------------------------------------

echo "=== Tier 3: Performance ===" >&2

T3_JSON="[]"
T3_GEO_MEAN="null"

if [ "$BUILD_STATUS" = "pass" ] && [ "$T2_PASS" -gt 0 ]; then
    # Run benchmarks with timing. -benchmem gives B/op and allocs/op.
    go test -bench=. -benchtime="$BENCH_TIME" -benchmem -run='^$' $BENCH_PACKAGES \
        >"$TMPDIR_SC/bench_perf.txt" 2>&1 || true

    # Parse "BenchmarkXxx-N  <iters>  <ns/op>  <B/op>  <allocs/op>" lines.
    # Format: BenchmarkName-P   N   ns/op   B/op   allocs/op
    grep '^Benchmark' "$TMPDIR_SC/bench_perf.txt" \
        | grep 'ns/op' \
        | awk '{
            name = $1;
            sub(/-[0-9]+$/, "", name);  # strip -P (GOMAXPROCS) suffix
            ns = "null"; bop = "null"; allocs = "null";
            for (i = 3; i <= NF; i++) {
                if ($(i) == "ns/op")     ns = $(i-1);
                if ($(i) == "B/op")      bop = $(i-1);
                if ($(i) == "allocs/op") allocs = $(i-1);
            }
            printf "%s\t%s\t%s\t%s\n", name, ns, bop, allocs;
        }' >"$TMPDIR_SC/bench_parsed.tsv"

    # Load reference baseline if available.
    if [ -f "$BASELINE" ]; then
        HAS_BASELINE=true
    else
        HAS_BASELINE=false
    fi

    # Build JSON array of T3 results and compute geometric mean ratio.
    T3_JSON="["
    first=true
    log_sum=0
    ratio_count=0

    while IFS=$'\t' read -r name ns bop allocs; do
        [ -z "$name" ] && continue

        vs_ref="null"
        if $HAS_BASELINE && [ "$ns" != "null" ]; then
            # Look up reference ns/op. jq is commonly available.
            ref_ns=$(jq -r --arg n "$name" '.[$n].ns_op // empty' "$BASELINE" 2>/dev/null || echo "")
            if [ -n "$ref_ns" ] && [ "$ref_ns" != "null" ]; then
                vs_ref=$(awk "BEGIN {r = $ns / $ref_ns; printf \"%.3f\", r}")
                log_sum=$(awk "BEGIN {printf \"%.10f\", $log_sum + log($vs_ref)}")
                ratio_count=$((ratio_count + 1))
            fi
        fi

        if ! $first; then T3_JSON+=","; fi
        first=false

        T3_JSON+=$(printf '\n    {"name":"%s","ns_op":%s,"b_op":%s,"allocs_op":%s,"vs_reference":%s}' \
            "$name" "$ns" "$bop" "$allocs" "$vs_ref")
    done <"$TMPDIR_SC/bench_parsed.tsv"

    T3_JSON+=$'\n  ]'

    if [ "$ratio_count" -gt 0 ]; then
        T3_GEO_MEAN=$(awk "BEGIN {printf \"%.3f\", exp($log_sum / $ratio_count)}")
    fi
fi

echo "  geometric_mean_ratio=$T3_GEO_MEAN" >&2

# --------------------------------------------------------------------------
# Emit scorecard.json
# --------------------------------------------------------------------------

cat > scorecard.json <<ENDJSON
{
  "branch": "$BRANCH",
  "sha": "$SHA",
  "timestamp": "$TIMESTAMP",
  "tiers": {
    "t1_correctness": {
      "build": "$BUILD_STATUS",
      "race": "$RACE_STATUS",
      "tests": {
        "pass": $T1_PASS,
        "fail": $T1_FAIL,
        "skip": $T1_SKIP,
        "total": $T1_TOTAL
      },
      "packages": {
        "pass": $PKG_PASS,
        "fail": $PKG_FAIL
      },
      "pass_rate": $T1_RATE
    },
    "t2_runability": {
      "benchmarks": {
        "pass": $T2_PASS,
        "fail": $T2_FAIL,
        "skip": $T2_SKIP,
        "total": $T2_TOTAL
      },
      "run_rate": $T2_RATE
    },
    "t3_performance": {
      "benchmarks": $T3_JSON,
      "geometric_mean_ratio": $T3_GEO_MEAN
    }
  }
}
ENDJSON

echo "" >&2
echo "Scorecard written to scorecard.json" >&2
echo "  T1 pass_rate=${T1_RATE}%  T2 run_rate=${T2_RATE}%  T3 geo_mean=${T3_GEO_MEAN}" >&2
