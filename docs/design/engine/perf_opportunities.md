# Performance Opportunities (GroupBy / Join / Sort)

Scope: identify order-of-magnitude improvements in Golars groupby, join, and sort by
removing interface-heavy paths and aligning with Polars-style columnar kernels.

## Current Golars Bottlenecks (Observed)

GroupBy:
- Multi-key grouping hashes per-row via `series.Series.Get` and `interface{}` values.
  - `internal/group/groupby.go` builds `map[uint64][]int` with per-row hash and allocs.
- Aggregation materializes per-group `[]interface{}` then computes aggregates in Go loops.
  - `internal/group/aggregation.go` calls `col.Get` for each row and allocates per-group slices.

Join:
- Hash join uses Go maps and string keys, with per-row key materialization.
  - `internal/compute/hash_string.go`, `internal/compute/hash_fast.go`.
- Multi-key joins use pair keys and rely on `StringValuesWithValidity` but still map-heavy.
- Result materialization uses `TakeFast` but still builds index arrays and per-column gathers.

Sort:
- Sorting is `sort.Slice` on indices with per-comparison `series.Get` interface dispatch.
  - `frame/sort.go` compares values row-by-row in Go.
- No sorted flags or typed argsort fast paths.

## Polars Patterns Worth Adopting

GroupBy:
- Row encoding for multi-key groupby: `encode_rows_unordered` with typed buffers.
  - `polars-core/src/frame/group_by/mod.rs`.
- Partitioned hash tables per thread; hash partitioning by key hash.
  - `polars-core/src/frame/group_by/hashing.rs`.
- Grouping uses typed `group_tuples` over chunked arrays (no interface boxing).

Sort:
- `arg_sort` uses typed kernels with fast paths for already-sorted data.
  - `polars-core/src/frame/column/mod.rs`.
- Sorted flags on columns to skip work and preserve order.

Join:
- Build side selection (shorter side) and chunked gather materialization.
  - `polars-ops/src/frame/join/dispatch_left_right.rs`.

## Highest-Impact Opportunities (Order-of-Magnitude)

1) Replace interface-based groupby with typed, streaming hash aggregation
   - Build hash table: key -> group id (u32/u64), store group keys in typed arrays.
   - Update per-group aggregates in-place (sum/mean/min/max/count) without index lists.
   - Keep null tracking and allow per-agg null semantics.
   - Impact: 10x-100x on groupby-heavy workloads by removing per-group allocations.

2) Row-encode multi-key groupby/join/sort into fixed-width key bytes
   - Encode keys into a byte buffer (or dictionary-encoded ids for strings).
   - Hash/compare encoded keys, not `interface{}` values.
   - Impact: 5x-20x on multi-key operations; enables SIMD-friendly hashing.

3) Typed argsort and multi-column sort via precomputed keys
   - Implement per-type argsort (radix for ints, partial quicksort for floats, lex sort for strings).
   - For multi-column sort: row-encode and sort by key bytes once.
   - Track sorted flags to avoid work; implement stable/unstable paths.
   - Impact: 10x-50x on sort-heavy workloads.

4) Dictionary-encode strings for join/groupby/sort
   - Build per-column dictionary with id arrays; operate on u32/u64 keys.
   - Join/groupby/sort on ids; only materialize strings for output.
   - Impact: 5x-20x on string-heavy operations.

5) Partitioned hash join for large datasets
   - Radix-partition keys; build/probe per partition in parallel.
   - Use build-side selection (shorter side) and vectorized probe.
   - Impact: 3x-10x on large joins; reduces cache misses and memory spikes.

## Secondary Opportunities (Likely 2x-5x)

- Columnar aggregators for median/std/var using incremental algorithms or
  partial aggregations to reduce per-group slice builds.
- Reuse scratch buffers / arenas for groupby/join/sort index arrays.
- Use u32 indices when row count < 2^32 to halve memory bandwidth.
- Track sortedness in Series/Column to skip work and enable fast paths.

## Suggested Implementation Sequence

1) GroupBy v2:
   - Typed key extraction + hash map to group id.
   - In-place aggregators for sum/mean/min/max/count.
   - Group key buffers stored as typed arrays (no `[]interface{}`).

2) Row encoding:
   - Shared encoder for multi-key groupby/join/sort.
   - Dictionary-encode strings for faster hashing/comparison.

3) Sort v2:
   - Typed argsort kernels + row-encoded multi-column sort.
   - Sorted flags to skip work and avoid stable sort unless requested.

4) Join v2:
   - Partitioned hash join + typed probing + gather using `TakeFast`.

## File References (Golars)

- GroupBy construction: `internal/group/groupby.go`
- GroupBy aggregations: `internal/group/aggregation.go`
- Join hashing: `internal/compute/hash_string.go`, `internal/compute/hash_fast.go`
- Sort: `frame/sort.go`
