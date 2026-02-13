# Golars

A benchmark for evaluating how agent swarms implement a real Go library from
its test suite and type signatures alone.

## The Challenge

This repository contains the complete API surface of a DataFrame library for
Go: type definitions, interfaces, struct fields, constants, and 77 test files
covering every package. Every function and method body has been replaced with
`panic("not implemented")`.

Your task: **make the test suite pass.**

```bash
# Current state: everything compiles, nothing passes
go build ./...   # succeeds
go test ./...    # every test package panics with "not implemented"
```

The reference implementation (before stubbing) is tagged at
`v0-reference-implementation`.

## What You Get

- Full type signatures and interfaces (the "spec")
- All test files, unmodified (the acceptance criteria)
- Test utilities (`testutil/`) for assertions, fixtures, and helpers
- Expression AST package (`expr/`) with working tests (pure data structures)
- Data type definitions (`internal/datatypes/`) with working tests
- Parallel execution utilities (`internal/parallel/`)
- Apache Arrow Go bindings (`arrow-go/`)
- Design notes for every package ([design/](design/README.md))

## What You Implement

Every `panic("not implemented")` body across these packages:

| Package | Description | Approx. stubs |
|---------|-------------|---------------|
| `internal/chunked/` | Columnar storage (ChunkedArray, Builder) | 26 |
| `series/` | Column abstraction, aggregations, sorting, casting | 131 |
| `internal/group/` | GroupBy engine | 79 |
| `internal/window/` | Window functions (ranking, aggregates, partitioning) | 141 |
| `internal/datetime/` | Temporal operations | 228 |
| `internal/strings/` | String operations | 102 |
| `frame/` | DataFrame operations (filter, join, sort, pivot, stats) | 205 |
| `io/` | CSV, Parquet, JSON I/O | 127 |
| `lazy/` | Query planner, optimizer, compiler | 183 |
| Root | Auto-constructors (dataframe_auto.go, series_auto.go) | 15 |

Total: ~1,237 function/method stubs across ~50 source files.

## Architecture

The dependency graph flows bottom-up:

```
expr (pure AST, no stubs)
  |
internal/chunked (storage layer)
  |
series (column abstraction)
  |
internal/group, internal/window, internal/datetime, internal/strings
  |
frame (DataFrame: the main workhorse)
  |
io (CSV, Parquet, JSON)
  |
lazy (query planner + optimizer)
  |
golars.go, lazy_api.go (re-exports, no stubs)
```

## Running the Suite

```bash
# Build check (should always pass)
go build ./...

# Run all tests
go test ./...

# Run a specific package
go test ./series/ -v

# Run with race detector
go test -race ./...
```

## Scoring

Runs are evaluated across three tiers:

| Tier | Dimension | Weight | What It Measures |
|------|-----------|--------|-----------------|
| T1 | Build | Required | `go build ./...` succeeds |
| T1 | Correctness | 40% | Fraction of unit tests passing |
| T1 | Race Safety | 5% | Tests pass under `-race` |
| T2 | Bench Runability | 30% | Fraction of benchmarks that complete without panic |
| T3 | Performance | 25% | Geometric mean of (agent/reference) ns/op ratios |

**T3 and the reference baseline.** Performance ratios are computed against
`benchmarks/reference_baseline.json`, generated from the
`v0-reference-implementation` tag. The reference implementation is built on
Apache Arrow's compute engine, which does not support every operation. Three
H2O.ai groupby queries (Q7, Q8, Q9) use compound expressions, top-k, and
correlation that Arrow's grouped aggregation kernel cannot handle. These
benchmarks have **no reference entry**, and that is by design.

When a benchmark has no reference entry, its T3 ratio is null and it is
excluded from the geometric mean. This means:

- An agent that only implements what Arrow supports will match the reference
  and score a geometric mean near 1.0.
- An agent that implements its own groupby engine and passes Q7/Q8/Q9 gets
  T2 credit (runability) for those benchmarks, even though there is no T3
  ratio to compare against.
- **No reference is not a penalty.** It simply means the reference could not
  run that benchmark either.

Run `bash benchmarks/scorecard.sh` locally to generate a full three-tier
scorecard. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Project Structure

```
golars/
  golars.go              # Public API re-exports (not stubbed)
  lazy_api.go            # Lazy API re-exports (not stubbed)
  dataframe_auto.go      # Auto-constructors (stubbed)
  series_auto.go         # Auto-constructors (stubbed)
  expr/                  # Expression AST (not stubbed, tests pass)
  series/                # Series interface + stubs
  frame/                 # DataFrame stubs
  io/                    # I/O stubs (csv/, parquet/, json/)
  lazy/                  # Query planner stubs
  internal/
    chunked/             # Storage layer stubs
    datatypes/           # Type definitions (not stubbed, tests pass)
    datetime/            # Temporal stubs
    group/               # GroupBy stubs
    parallel/            # Parallel utilities (not stubbed, tests pass)
    strings/             # String operation stubs
    window/              # Window function stubs
  testutil/              # Test helpers (not stubbed)
  arrow-go/              # Apache Arrow Go (dependency)
  benchmarks/            # Performance benchmarks
```

## License

MIT. See [LICENSE](LICENSE).
