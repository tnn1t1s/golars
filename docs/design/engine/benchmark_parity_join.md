# Join Benchmark Parity Audit

Scope: join-related benchmarks used for Golars vs Polars comparisons.

## Sources Reviewed

- Golars cross-engine runner: `benchmarks/compare/run_golars_suite.go`
- Polars cross-engine runner: `benchmarks/compare/run_polars_suite.py`
- Shared suite definition: `benchmarks/compare/suite.json`
- Golars-only micro/Go benchmarks: `benchmarks/join/join_test.go`

## Parity (Matches)

Cross-engine suite (`benchmarks/compare`):
- Same dataset file path and SHA-256 reported in both outputs (via suite JSON).
- Same join spec in `suite.json`:
  - `how`: inner/left
  - `left_on`/`right_on`: `["id1"]` and `["id1","id2"]`
  - `right_rows`: 5000
- Same right table construction:
  - Polars: `right = df.head(right_rows)`
  - Golars: `right = df.Head(right_rows)`
- Same warmup + N-iteration timing loop and averaged ms result.

## Mismatches / Gaps

- Golars Go micro benchmarks (`benchmarks/join`) have no Polars equivalent:
  - They build a *unique* right table via `GroupBy(...).Count()` to avoid duplicate keys.
  - They include `medium-safe` dataset benchmarks, while `suite.json` join queries only target `small`.
  - These results are useful for Golars regression checks but are **not comparable** to Polars unless a matching Polars bench is added.

## Next Steps (Parity Improvements)

- Extend `benchmarks/compare/suite.json` with `medium-safe` join queries if we want cross-engine parity at larger scale.
- Add a "unique-right" variant in the compare suite so Golars and Polars run the same many-to-one join workload.
