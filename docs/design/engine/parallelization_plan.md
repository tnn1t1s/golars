# Parallelization Plan (Polars-Informed)

## Polars Model (Observed)

Sources:
- `../polars/crates/polars-core/src/lib.rs`
- `../polars/crates/polars-mem-engine/src/executors/projection.rs`
- `../polars/crates/polars-mem-engine/src/executors/stack.rs`
- `../polars/crates/polars-plan/src/plans/options.rs`
- `../polars/crates/polars-plan/src/dsl/options/mod.rs`
- `../polars/crates/polars-io/src/parquet/read/options.rs`
- `../polars/crates/polars/src/lib.rs`

Key patterns:
- Single global thread pool (rayon) with a thin wrapper (`POOL`) exposing `install`, `join`,
  `scope`, `spawn`, and `current_num_threads`. Thread count is controlled by `POLARS_MAX_THREADS`.
- Thread-local gate (`ALLOW_RAYON_THREADS`) to disable parallelism on specific threads and
  avoid nested parallel deadlocks. (Golars: TODO, currently uses a global env gate.)
- Plan-level flags:
  - `ProjectionOptions.run_parallel` gates expression evaluation parallelism.
  - `JoinOptions { allow_parallel, force_parallel }` allows/forces parallel join strategies.
- Operator heuristics choose vertical vs horizontal parallelism:
  - Vertical: split by chunks when data is large and chunked (e.g., `height > threads*2`).
  - Horizontal: evaluate expressions in parallel across columns.
- IO uses explicit parallel strategies (e.g., Parquet `ParallelStrategy::Columns/RowGroups/Prefiltered/Auto`).
- A separate pool exists for specific tasks (e.g., mmap unmap), showing targeted pool usage.

## Implications for Golars

We need a consistent parallel control surface that is used by both eager and lazy execution,
plus operator-level heuristics that choose how to parallelize work without oversubscription.

## Proposed Golars Parallel Architecture

### 1) Global Pool + Execution Context

- Add a small `internal/parallel` package that wraps a shared worker pool.
- Implementation detail: default to a bounded goroutine semaphore, with an `ants`-backed pool
  available behind a build tag (`-tags ants`) once the dependency is available.
- Config:
  - `GOLARS_MAX_THREADS` (default `runtime.GOMAXPROCS(0)`).
  - `GOLARS_NO_PARALLEL` to disable worker dispatch globally.
- API sketch:
  - `parallel.Join(fnA, fnB)`
  - `parallel.For(n, fn)`
- `parallel.Scope(fn)` for fan-out. (TODO)
- `parallel.Allow()` / `parallel.Disallow()` per goroutine via context token. (TODO)
- Goal: a single place to decide "should this run in parallel" and to avoid nested parallelism.

### 2) Plan Options and Execution Flags

- Add run-parallel flags to lazy physical operators similar to Polars:
  - `ProjectionOptions.RunParallel` (TODO)
  - `JoinOptions.AllowParallel` / `ForceParallel` (TODO)
  - `GroupByOptions.AllowParallel` (TODO)
- Make `LazyFrame.Collect` accept an execution context (or options) that sets defaults.

### 3) Operator-Level Parallelization

Start with the biggest wins:

- Projection / WithColumns:
  - Horizontal: evaluate expressions in parallel when `len(exprs)` is large.
  - Vertical: if data has many chunks and is large, split by chunks and evaluate in parallel.
- Filter:
  - Parallel evaluate predicate + filtering per chunk.
  - Merge chunk results into a new DataFrame.
- GroupBy:
  - Partition keys by hash into N partitions; build per-partition groups in parallel.
  - Merge group states; avoid locks on hot paths.
- Join:
  - Partition build/probe sides by hash; parallel build and probe per partition.
  - Avoid parallelism if `current_thread_has_pending_tasks` is true.
- IO:
  - Parquet: allow parallel over row groups or columns; add `ParallelStrategy` options similar
    to Polars (None/Columns/RowGroups/Prefiltered/Auto).
  - CSV: parallel parsing in two passes (line offsets + parse) when input is large.

### 4) Heuristics and Safety

- Parallel only if: `rows > threads*2` or `chunks > threads`.
- Avoid nested parallelism: if running inside the pool, default to sequential unless
  `ForceParallel` is set.
- Keep per-operator thresholds to avoid overhead on small frames.

## Proposed Milestones

1) Build `internal/parallel` and add `ExecutionOptions` for eager/lazy. (partial)
2) Wire projection and filter to use horizontal parallelism; add vertical chunk parallelism. (partial)
3) Add partitioned group-by and hash-join parallel implementations. (prototype)
4) Add IO parallel strategy options (Parquet and CSV). (TODO)

## Notes

- This plan intentionally separates "parallel policy" (flags + heuristics) from "parallel
  mechanism" (pool + helpers). That mirrors Polars' design and reduces ad-hoc goroutine usage.
