# series -- Column Abstraction

## Purpose

A `Series` is a named, typed column. The `Series` interface is the public API;
`TypedSeries[T]` is the concrete generic implementation that wraps a
`chunked.ChunkedArray[T]`.

## Key Design Decisions

**Interface + generic struct.** The `Series` interface is type-erased (returns
`interface{}` from Get, ToSlice, Min, Max). `TypedSeries[T]` implements it by
delegating to the underlying `ChunkedArray[T]` and converting results. This
lets DataFrames hold heterogeneous columns in a `[]Series` slice.

**Constructor pattern.** Each numeric type has a convenience constructor
(`NewInt64Series`, `NewFloat64Series`, etc.) that calls the generic
`NewSeries[T]` with the correct `datatypes.DataType`. The generic constructor
creates a `ChunkedArray[T]`, appends the values, and wraps it in a
`TypedSeries[T]`.

**InterfaceSeries.** A fallback implementation that stores `[]interface{}`
directly, used when the concrete type is not known at compile time (e.g., after
certain casts or when constructing from reflection). It implements the same
`Series` interface but without Arrow backing.

**Arrow bridge.** `SeriesFromArrowArray` and `SeriesFromArrowChunked` construct
a Series from Arrow data. `ArrowChunked` extracts the underlying
`*arrow.Chunked` from a Series, used by the group-by and filter engines for
zero-copy Arrow compute.

**Aggregations.** Sum, Mean, Min, Max, Std, Var, Median operate on the
materialized Go slice from `ToSlice()`. They use type-switches over the
concrete value types and return float64 or interface{}.

**Sorting.** `Sort(ascending)` materializes values, sorts with `sort.Slice`,
and builds a new Series. `ArgSort` returns sorted indices without
materializing. `Take(indices)` builds a new Series by gathering values at the
given positions.

**Casting.** `Cast(dt)` converts between types using numeric conversions
(int->float, float->int, etc.) and string parsing. The cast logic lives in
`cast.go` with a type-switch dispatch.

**Fast paths.** `fastpath.go` contains optimized versions of common operations
that avoid interface boxing, using direct typed access to the ChunkedArray.
