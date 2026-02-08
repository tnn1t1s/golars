# internal/group -- GroupBy Engine

## Purpose

Partitions a DataFrame's rows by key columns, then applies aggregation
functions to each group. This is the engine behind `DataFrame.GroupBy().Agg()`.

## Key Design Decisions

**Hash-based grouping.** Rows are assigned to groups by hashing their key
column values with FNV. The `GroupBy` struct maintains:
- `groups map[uint64][]int` -- hash to row indices
- `groupKeys map[uint64][]interface{}` -- hash to key values
- `groupOrder []uint64` -- preserves first-occurrence order

**DataFrameInterface.** The group package does not import `frame` directly.
Instead, it depends on a `DataFrameInterface` with `Column(name)` and
`Height()` methods. This breaks the import cycle (frame -> group -> frame).

**Arrow-accelerated path.** When key columns are Arrow-backed (checked via
`series.ArrowChunked`), the grouping uses Arrow's dictionary-encoding approach
through `arrow_groupby_multi.go`. This is significantly faster than the
hash-based fallback for supported types.

**Aggregation dispatch.** `Agg()` receives a `map[string]expr.Expr` where each
expression is an aggregate (e.g., `Col("x").Sum()`). It first attempts the
Arrow path (`tryAggArrow`); if the expression or type is unsupported, it falls
back to the Go-native path with typed aggregation helpers in
`aggregation_typed.go`.

**Typed aggregation helpers.** Functions like `typedSum[T]`, `typedMean[T]` in
`aggregation_typed.go` operate on concrete Go slices extracted from series,
avoiding interface{} overhead in the inner loop. They use generics constrained
to numeric types.

**Concurrency.** A `sync.RWMutex` on `GroupBy` protects the group maps. Group
building itself is single-threaded (one pass over the data), but aggregation
across groups can be parallelized.
