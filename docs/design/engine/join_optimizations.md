# Join Optimizations Research

This note summarizes join techniques worth adopting in golars and highlights
what Polars implements today (based on the local `../polars` checkout).

## Current Golars Join Shape

- `frame/join.go` uses a per-row probe loop and a generic hash table.
- Fast paths exist only for single-column `int64`/`int32` joins.
- Probe is sequential; output materialization appends into `[]int` without a
  precomputed size, which amplifies allocations for large joins.

## Observations From Polars (local code review)

Source paths:
- `../polars/crates/polars-ops/src/frame/join/hash_join/single_keys.rs`
- `../polars/crates/polars-ops/src/frame/join/hash_join/single_keys_inner.rs`
- `../polars/crates/polars-ops/src/frame/join/hash_join/sort_merge.rs`

Highlights:
- Partitioned hash join.
  - `build_tables` hashes each key, counts per-partition sizes, computes offsets,
    scatters keys/indices into contiguous buffers, and builds per-partition hash
    maps. This avoids global locks and improves cache locality.
  - Hash maps are pre-sized conservatively and then reserved once if they grow,
    avoiding repeated resizes.
- Parallel probing + output materialization.
  - Probe is done per-partition in parallel; each partition produces local join
    tuples.
  - Output vectors are allocated once via a two-phase "count + flatten" flow,
    then filled in parallel with raw pointer writes.
- Sort-merge join as a fast alternative when keys are sorted.
  - For numeric keys with no nulls, a parallel sorted-merge join is used.
  - If only one side is sorted and the size ratio is favorable, Polars sorts the
    other side and still uses merge join (heuristic via `POLARS_JOIN_SORT_FACTOR`).
- Join validation and null behavior.
  - Build side size checks and `nulls_equal` handling are part of the join path.

## Modern Join Optimizations Worth Adopting

Algorithm selection:
- Hash join for general equality joins; sort-merge when both sides are sorted or
  one side is small enough to sort cheaply.
- Grace/partitioned hash join when data is large or parallelism is needed.
- Nested-loop only for very small inputs or inequality joins.

Hash join mechanics:
- Radix/partitioned build + probe to isolate partitions and avoid shared maps.
- Precompute hashes for fixed-width types to avoid recomputing in probe loops.
- Use two-pass output materialization (count matches, allocate once, fill).
- Skew handling: detect heavy-hitter keys and handle them separately to reduce
  worst-case bucket blowups.

Data layout + typing:
- Prefer typed fast paths for numeric and dictionary-encoded string keys.
- For strings, consider dictionary encoding or cached hash arrays.
- Avoid per-row map lookups in multi-key joins by hashing a key tuple or
  struct-of-arrays plus combined hash.

Parallelism:
- Partition build/probe by hash range; each worker builds its local table and
  probes matching partitions.
- Use `parallel.For` for build and probe stages with chunking sized to cache.

## Suggested Golars Roadmap

Phase 1: Partitioned hash join for single-key joins
- Implement per-partition build with precomputed hashes.
- Two-pass probe to allocate output once.
- Parallelize build + probe using `internal/parallel`.

Phase 2: Extend to multi-key joins
- Combine key hashes (e.g., xxhash on tuple) into a single partition hash.
- Use struct-of-arrays hashing rather than per-row interface lookups.

Phase 3: Sort-merge join path
- Add a sorted-merge implementation for numeric keys with no nulls.
- Track or infer sortedness on Series/DataFrame; use a size-ratio heuristic.

Phase 4: String + dictionary optimizations
- Add dictionary encoding or cached hash arrays for string keys.
- Reuse encoded dictionaries during repeated joins (if cached at DataFrame level).

## Experiments to Validate

- Compare sequential hash join vs partitioned hash join on:
  - Uniform keys, skewed keys, and high-duplicate keys.
  - Small vs large inputs (10k, 1M, 10M rows).
- Compare sort-merge vs hash join when keys are already sorted.
- Memory profile: output allocations before/after two-pass materialization.

