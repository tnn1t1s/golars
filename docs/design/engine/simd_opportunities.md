# SIMD Opportunities and Touchpoints

## Landed Changes

- Hash-table build now pre-hashes int64 keys with SIMD when available.
- Join key extraction now prefers typed slices with validity to reduce per-row interface calls.
- Group-by for single numeric columns now uses typed values with validity (no per-row interface).

## Near-Term Candidates

- Group-by on a single int64/int32 column:
  - Extract typed values + validity in one pass.
  - Pre-hash int64 keys with SIMD; use scalar hash for int32.
  - Keep nulls in a dedicated bucket to avoid collisions with zero values.
- Comparison and filter kernels (int32/int64/float):
  - Batch compare to build boolean masks with SIMD.
  - Use masks in `Filter` and `Where` to reduce per-row branching.
- Arithmetic kernels:
  - SIMD add/sub/mul/div for contiguous chunks, falling back to scalar for remainder.

## Longer-Term Candidates

- String equality/contains: SIMD-accelerated byte scanning for small patterns.
- Hash joins: SIMD-friendly probing on dense candidate arrays (requires bucket compaction).
- Window functions: SIMD prefix sums for numeric rolling aggregates.
