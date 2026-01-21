# Current Engine Inventory

Scope: This captures the current golars execution engine and data model, focusing on how data
flows through eager and lazy paths, where Arrow is used, and what parallelism/memory behavior
exists today.

## Core Data Model

- DataFrame (`frame/dataframe.go`): holds `[]series.Series`, a `Schema`, `height`, and an `RWMutex`.
  Most operations produce new DataFrames by allocating new Series; there is no global execution
  context or scheduler for eager ops.
- Series (`series/series.go`): type-erased interface implemented by `TypedSeries[T]` backed by
  `chunked.ChunkedArray[T]`. Many operations are per-element loops; some reuse Arrow arrays.
- ChunkedArray (`internal/chunked/chunked_array.go`): holds `[]arrow.Array` chunks plus metadata.
  `AppendArray` retains Arrow arrays (zero-copy if types match). `AppendSlice` builds new Arrow
  arrays via builders on `memory.NewGoAllocator`.
- Data types (`internal/datatypes/*`): maps golars types to Arrow types. String maps to Arrow UTF8.
  Large UTF8 is not a native type; it is converted during Parquet reads.

## Execution Paths

### Eager execution

- DataFrame ops (filter, select, groupby, joins, etc.) are executed immediately using Go loops
  and per-row access via `Series.Get` or per-value builders.
- Expression evaluation lives in `frame/eval.go` and walks `expr.Expr` trees, evaluating via
  `evaluateExpr` into new Series. Window functions use group-by-like partitioning and then
  merge results by row index.
- Compute kernels in `internal/compute/kernels.go` are simple scalar loops (no SIMD/bitmaps).

### Lazy execution

- Lazy engine uses an arena-based expression AST (`lazy/arena.go`, `lazy/expr.go`) and logical
  plans (`lazy/plan.go`). Optimizers exist (predicate/projection pushdown, CSE, simplify, etc.).
- Physical plans (`lazy/physical.go`) compile to eager DataFrame operations, so execution is
  still row/Series based and not streaming.

## IO Boundaries

- CSV (`io/csv/*`) and JSON (`io/json/*`) use custom readers/writers.
- Parquet (`io/parquet/*`) uses Arrow Go:
  - Write: DataFrame -> Arrow table -> `pqarrow` file writer (`io/parquet/writer.go`).
  - Read: memory-map support + record-batch scan via `pqarrow.FileReader.GetRecordReader`.
    Batches are appended to per-column chunked arrays directly (no intermediate table).
    Large UTF8 is converted to UTF8 during read.

## Parallelism and Memory

- There is no shared execution scheduler or thread pool for eager operations.
- Group-by (`internal/group/*`) builds hash groups per row; no parallel build. Uses `fnv` hash
  and row indices, with values stored as `interface{}`.
- Window evaluation uses partitioning and per-row merges; no vectorized merges.
- Parquet read uses Arrow's parallel column read (default) and mmap (default) but the rest of the
  pipeline remains per-element for Series operations.

## Current Engine Properties (Summary)

- Columnar storage exists via Arrow arrays inside ChunkedArray, but most compute paths are
  scalar Go loops, not vectorized kernels or batch-oriented execution.
- Lazy engine has a planner/optimizer but physical execution is still eager (materialized
  DataFrames between operators).
- IO is the main place Arrow is used directly; other operations largely ignore Arrow compute.
- No unified memory pool or buffer reuse strategy across operators.

## Gaps vs Polars/DataFusion Patterns (Observed)

- No column-chunk decoding pipeline (Polars uses mmap + page decoding directly into arrays).
- No row-group or column-level scan scheduling beyond the Arrow reader defaults.
- Predicate and projection pushdown are only available in the lazy planner, not in IO scan
  (Parquet read uses selected columns/row groups but no predicate pushdown).
- Limited use of bitmaps/SIMD and no vectorized kernels for compute-heavy paths.

