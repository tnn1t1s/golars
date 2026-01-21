# Engine Gap Analysis (Polars/DataFusion patterns)

This maps observed Polars Parquet/scan behavior and common DataFusion patterns to
current golars architecture, highlighting gaps and incremental adoption steps.

## Polars patterns (observed in ../polars)

References:
- `../polars/crates/polars-io/src/parquet/read/reader.rs`
- `../polars/crates/polars-io/src/parquet/read/read_impl.rs`
- `../polars/crates/polars-io/src/parquet/read/mmap.rs`
- `../polars/crates/polars-parquet/src/parquet/read/*`
- `../polars/crates/polars-stream/src/nodes/io_sources/parquet/*`

Observed patterns:
- **Mmap + column chunk slicing**: column byte ranges are sliced from a
  memory-mapped file and decoded directly (`ColumnStore::Local`, `mmap_columns`).
- **Page-level decode + prefetch**: custom page readers and decompression with
  prefetch hints before decode.
- **Parallel strategy selection**: switches between row-group and column
  parallelism based on row-group count, projection width, and thread count.
- **Pushdown in scan**: row-group slicing and predicate filters happen at scan
  time (not just planner time).
- **Streaming/async pipeline**: metadata fetch + row-group prefetch + decode are
  pipelined in `polars-stream` for large files and object-store sources.

## DataFusion patterns (general)

Typical DataFusion design patterns (from public docs/architecture):
- **Record batch streaming**: execution engine processes `RecordBatch` streams
  through physical operators without materializing full tables.
- **Physical plan operators**: structured operator tree (scan, filter, aggregate,
  join, sort) with metrics and partition-aware execution.
- **Predicate/projection pushdown**: applied early in scans, often via metadata
  (row-group stats) before decoding.
- **Memory pools + batch sizing**: explicit control of batch size and memory
  allocation behavior.

## Golars gaps vs patterns

Current state summary is in `docs/design/engine/current_engine.md`. Key gaps:

- **Scan decode path**: golars depends on Arrow Go’s Parquet reader rather than a
  custom column-chunk/page decode pipeline. This limits control over prefetch,
  buffering, and direct column decoding strategies.
- **Streaming execution**: lazy plans compile into eager DataFrame operations.
  There is no record-batch pipeline between operators; each stage materializes
  full columns.
- **Pushdown integration**: the lazy optimizer exists, but scan-level predicate
  pushdown (row-group stats + early filters) is not wired into IO.
- **Parallelism**: Parquet read uses Arrow’s default parallel column read; other
  operators are mostly single-threaded (groupby, window, compute kernels).
- **Vectorized kernels**: compute kernels are scalar loops (no SIMD/bitmap
  kernels, no Arrow compute usage outside IO).
- **Memory reuse**: no global pool or allocator reuse across operators; builders
  allocate frequently (see pprof results in `baseline_profiles.md`).

## Incremental adoption (proposed)

1) **Scan-level pushdown wiring**
   - Wire lazy filter/projection into Parquet reader options (columns + row
     groups), add row-group statistics checks when available.

2) **Batch-level execution path**
   - Introduce a minimal `RecordBatch` pipeline for IO -> filter -> projection
     before full DataFrame materialization. Keep API stable, add internal
     execution path toggles.

3) **Row-group / column parallel scheduling**
   - Add a small scheduler that chooses row-group vs column parallelism based on
     row-group count and projection width (mirroring Polars strategy).

4) **Column-chunk decode strategy**
   - Evaluate a custom Parquet decode path (or direct Arrow column chunk usage)
     for targeted datasets where Arrow Go is allocation-heavy.

5) **Compute kernels**
   - Replace per-element loops with vectorized kernels using Arrow compute where
     feasible; prioritize hot paths from profiling.

6) **Memory pool + buffer reuse**
   - Introduce a shared allocator and buffer pools for builders to reduce
     allocations in string/binary paths.

## Success metrics

- Parquet read: reduce alloc_space in Arrow allocator by 2-3x; drop wall time by
  30-50% on medium/large datasets.
- Eager ops: reduce per-row loops in groupby and aggregation; improve 2-4x on
  `benchmarks/agg` and `benchmarks/groupby`.
- Lazy plan: show >1.5x improvement on end-to-end pipelines with pushdown.

