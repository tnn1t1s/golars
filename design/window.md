# internal/window -- Window Functions

## Purpose

Implements SQL-style window functions: ROW_NUMBER, RANK, DENSE_RANK,
PERCENT_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, and aggregate
windows (SUM, AVG, MIN, MAX, COUNT over a window frame).

## Key Design Decisions

**Three function categories.** The `Function` interface has three refinements:
- `RankingFunction` -- assigns ranks based on ordering (ROW_NUMBER, RANK, etc.)
- `ValueFunction` -- accesses specific rows by offset (LAG, LEAD, FIRST_VALUE)
- `AggregateFunction` -- computes running aggregates over frame bounds

Each category has different requirements for ORDER BY and frame specifications.

**Partition-then-compute.** Window evaluation is two-phase:
1. `partition.go` groups rows by PARTITION BY columns, then sorts each
   partition by ORDER BY columns
2. Each window function's `Compute(partition)` method receives a fully
   partitioned and ordered view of the data

**Partition interface.** `Partition` provides: `Series()` (all columns),
`Column(name)`, `Indices()` (original row positions), `OrderIndices()` (sorted
order), `Size()`, and `FrameBounds(row, frame)` for computing window frame
boundaries per row.

**Frame bounds.** `FrameBounds(row, frameSpec)` calculates the start/end of the
window frame for a given row. Supports ROWS BETWEEN (offset-based) and RANGE
BETWEEN (value-based) frame types. The default frame is RANGE BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW.

**Spec and Expr (preserved, not stubbed).** `spec.go` defines `Spec` (the
window specification: partition by, order by, frame). `function.go` defines the
interfaces. `api.go` provides the `WindowFunc.Over(spec)` method that creates a
window `Expr`. These files are NOT stubbed; they are the specification.

**functions.go has the implementations.** Each concrete function struct
(rowNumberFunc, rankFunc, lagFunc, etc.) lives in `functions.go`. They follow a
consistent pattern: implement `Function` interface methods (Compute, DataType,
Name, Validate, SetSpec).

**aggregates.go** contains the aggregate window functions (SUM, AVG, etc.) that
compute running values within frame bounds. These iterate over the frame for
each row and accumulate results.
