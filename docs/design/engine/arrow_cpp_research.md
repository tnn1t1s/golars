# Arrow C++ Reading Map
- Acero overview and streaming model: `arrow/docs/source/cpp/acero/overview.rst` (Acero is a streaming execution engine, ExecBatch, and stream-based operation). https://github.com/apache/arrow/blob/main/docs/source/cpp/acero/overview.rst#L36
- ExecPlan / ExecNode API: `arrow/cpp/src/arrow/acero/exec_plan.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/exec_plan.h#L54
- Compute API and function registry: `arrow/docs/source/cpp/compute.rst`. https://github.com/apache/arrow/blob/main/docs/source/cpp/compute.rst#L37
- Hash join interfaces: `arrow/cpp/src/arrow/acero/hash_join.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/hash_join.h#L60
- Hash join dictionary unification: `arrow/cpp/src/arrow/acero/hash_join_dict.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/hash_join_dict.h#L47
- Swiss join + partitioned build: `arrow/cpp/src/arrow/acero/swiss_join_internal.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/swiss_join_internal.h#L529
- Partition utilities: `arrow/cpp/src/arrow/acero/partition_util.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/partition_util.h#L35
- SwissTable (vectorized hash table): `arrow/cpp/src/arrow/compute/key_map_internal.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/key_map_internal.h#L33
- Row-oriented key encoding: `arrow/cpp/src/arrow/compute/row/encode_internal.h` + `row_encoder_internal.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/row/encode_internal.h#L43
- Grouper and GrouperFastImpl: `arrow/cpp/src/arrow/compute/row/grouper.h` + `grouper.cc`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/row/grouper.cc#L548
- Hash aggregate kernel interface: `arrow/cpp/src/arrow/compute/kernels/hash_aggregate_internal.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/kernels/hash_aggregate_internal.h#L39
- Sort implementation: `arrow/cpp/src/arrow/compute/kernels/vector_sort.cc`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/kernels/vector_sort.cc#L388
- Filter output sizing: `arrow/cpp/src/arrow/compute/kernels/vector_selection_filter_internal.cc`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/kernels/vector_selection_filter_internal.cc#L68
- Task scheduling primitives: `arrow/cpp/src/arrow/acero/task_util.h`, `arrow/cpp/src/arrow/util/thread_pool.h`, `arrow/cpp/src/arrow/util/task_group.h`, `arrow/cpp/src/arrow/util/parallel.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/task_util.h#L58
- Memory pool alignment: `arrow/cpp/src/arrow/memory_pool.h`. https://github.com/apache/arrow/blob/main/cpp/src/arrow/memory_pool.h#L108

# Concept Map Table
| Arrow C++ concept | Where in code | Why it matters | Golars mapping | MVP approach |
| --- | --- | --- | --- | --- |
| Streaming exec plan and ExecBatch | `overview.rst`, `exec_plan.h` | Streamed batches avoid table-level churn and enable pipelining | Model Golars engine on ExecBatch flows | Build a minimal ExecBatch pipeline for Q1/Q6; no full ExecPlan DAG required, just the ExecBatch contract and operator-local streaming |
| Task scheduling with task groups | `task_util.h`, `task_group.h`, `parallel.h` | Coarse task groups avoid overscheduling; explicit limits on in-flight tasks | Replace pool fanout with partition tasks | W workers, one task per partition, no nested parallel |
| Partitioned hash join build | `swiss_join_internal.h` | 3-phase build (partition, process, merge) improves locality and parallelism | Implement join build in partitions | Power-of-two partitions, P >= W |
| Partition utilities | `partition_util.h`, `swiss_join.cc` | O(n) bucket sort + partition locks reduces contention | Use partition sort for row ids | Bucket sort on high hash bits |
| SwissTable vectorized map | `key_map_internal.h` | Batch early_filter/find/map_new_keys reduces branchiness | Replace map-based joins/groupby | Port only early_filter/find/map_new_keys; no deletion and growth only via doubling |
| Row-based key encoding | `encode_internal.h`, `row_encoder_internal.h` | Row encoding co-locates key bytes and nulls | Encode join/groupby keys into row buffers | Fixed-width + binary + null byte |
| Dictionary unification for joins | `hash_join_dict.h` | Unified int32 ids align build/probe and missing sentinel | Normalize join keys to int32 ids | Pre-encode once per join |
| GrouperFastImpl minibatches | `grouper.cc` | Minibatch hashing + SwissTable reduces per-row overhead | Groupby uses minibatch hashing | Fixed minibatch size; bitvectors |
| Hash aggregate kernel interface | `hash_aggregate_internal.h` | Resize/Consume/Merge/Finalize enables multi-agg fusion | Unified aggregator interface | GroupedAggregator-like API |
| Sort strategy (radix + merge) | `vector_sort.cc` | Radix per batch + merge across batches; null partitioning | Sort indices per batch then merge | Radix when keys <= 8 |
| Filter output sizing | `vector_selection_filter_internal.cc` | Precompute output size with bit counters | Preallocate filter outputs | BitBlockCounter + CountSetBits |
| Memory pool alignment | `memory_pool.h` | 64-byte aligned allocations reduce cache noise | Use Arrow pool for hot-path buffers | Pool-backed buffers for joins/groupby |

# Recommendations
Key insight:
- Row encoding collapses multi-key + null semantics into a single contiguous byte span, enabling branch-light hashing and vectorized equality checks. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/row/encode_internal.h#L43

1. Port RowTableEncoder + KeyEncoder null-byte scheme to Go for join/groupby keys. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/row/encode_internal.h#L43
2. Implement SwissTable-like vectorized hashing (early_filter/find/map_new_keys) to replace map-based hashing; no deletion and growth only via doubling. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/key_map_internal.h#L33
3. Build partitioned hash join (partition -> process -> merge) like SwissTableForJoinBuild. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/swiss_join_internal.h#L529
4. Use dictionary unification into int32 ids for join keys and missing sentinel semantics. https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/hash_join_dict.h#L47
5. Rework groupby to use GrouperFastImpl minibatch hashing + SwissTable and GroupedAggregator for aggregation fusion. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/row/grouper.cc#L548
6. Update sort to radix-per-batch with merge across batches for tables, retaining null partitioning strategy; avoid comparator-based index sorting where possible by sorting values (or packed keys) and applying the permutation once. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/kernels/vector_sort.cc#L388
7. Use filter output sizing via bit counters before allocation. https://github.com/apache/arrow/blob/main/cpp/src/arrow/compute/kernels/vector_selection_filter_internal.cc#L68
8. Align scheduling to Acero TaskScheduler/task group style (coarse tasks, bounded concurrency). https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/task_util.h#L58

# Milestone Plan
1. **Row encoding + SwissTable core**
   - Change: implement row encoding + SwissTable map in Go and replace map-based key hashing.
   - Expected impact: reduces join/groupby hash overhead and allocs/op.
   - Receipts: microbench for hashing/map, allocs/op, CPU pprof.
   - Risks: null semantics, variable-length encoding correctness.
2. **Partitioned hash join**
   - Change: implement partitioned build/probe with power-of-two partitions and merge step.
   - Expected impact: H2OAI medium_safe InnerJoin <= 4x, MultiKeyJoin <= 3.5x.
   - Receipts: join benchmark suite table + CPU profiles.
   - Risks: partition skew, memory spikes.
3. **Groupby fast path**
   - Change: use GrouperFastImpl-style minibatch hashing + GroupedAggregator merge.
   - Expected impact: Q1 250k rows <= 6x.
   - Receipts: Q1 benchmark table, allocs/op, groupby pprof.
   - Risks: aggregator merge correctness, dictionary handling.
4. **Sort and filter upgrades**
   - Change: radix-per-batch sort with merge; filter output sizing.
   - Expected impact: sort ratios drop materially; filter scaling stabilizes.
   - Receipts: sort/filter benchmark tables and CPU profiles.
   - Risks: sort stability, null ordering behavior.

# Benchmark Targets
- H2OAI medium_safe InnerJoin: ~6.3x → ≤ 4x
- H2OAI medium_safe MultiKeyJoin: ~5.6x → ≤ 3.5x
- TPC-H Q1 (250k rows): ~13x → ≤ 6x

If you cannot cite an upstream Arrow C++ source code location for a claim about join/groupby/parallelism, label the claim as speculation and do not base recommendations on it.
