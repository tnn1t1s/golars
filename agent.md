# Agent Run Guidance

## Join Benchmarks (Consistency Rule)

Always run and report the same join benchmark suite in the same order:

1) Golars H2OAI join benchmarks (small + medium-safe):
   - inner, left, multikey, multikey indices.
2) Polars H2OAI join parity (same parquet files, same joins).
3) Golars micro joins.
4) Polars micro joins.

Report results in a single stable table:
- Operation
- Dataset
- Golars time
- Polars time
- Ratio (Golars/Polars)

Do not report partial suites. If any part fails, rerun the full suite.

## Profiling: Common Traps and What to Do Instead

1) Don't over-interpret runtime-only tops
- If `pprof -top` is dominated by runtime frames (e.g. `pthread_cond_signal`, `chanrecv1`, `usleep`, `semaacquire`), assume scheduling/coordination overhead, not real work.
- Action: capture block + mutex profiles immediately, then pivot to profiling your concurrency primitives (pool submit/worker loops, `parallel.For`, `Take*Fast`, `*Parallel*` helpers), not runtime symbols.

2) `runtime.chanrecv1` focus often lies in benchmarks
- Focusing block profiles on `runtime.chanrecv1` often collapses into `testing.(*B)` or harness goroutines and won't reveal user-level channels.
- Action: don't chase `chanrecv1`. Focus on user-level functions already in the blocked cum-path (e.g. `series.takeParallelFor`, `series.Take*Fast`, `internal/parallel.For`, `internal/parallel.(*stdPool).Submit`, `internal/compute.*Parallel*`).
- Use: `pprof -call_tree -focus '<user symbol>'` on the block profile.

3) Always isolate coordination vs compute with a kill-switch
- Before rewriting concurrency, add a temporary internal toggle to disable parallelism for the suspected stage.
- Examples: `GOLARS_TAKE_PARALLEL=0` (forces sequential `Take*Fast`), `GOLARS_JOIN_PARALLEL=0` (forces sequential join probe), or a compile-time const for profiling.
- Rule: if disabling parallelism makes it faster, you have a coordination-granularity bug.

4) Avoid nested parallelism in hot paths
- Hot loops must not call helpers that internally use the pool (e.g. Join -> `TakeFast` -> `parallel.For`).
- Action: in hot paths, use either sequential code or static range partitioning with W goroutines and preallocated output slices. Do not use shared pool/task fanout inside hot loops.

5) Evidence required for "scheduler overhead fixed"
- A change only counts if:
  - runtime scheduling frames drop out of top 20, and
  - block profile is no longer dominated by `chanrecv1`/`WaitGroup.Wait`, and
  - wall time improves on the target benchmark.
- Attach artifacts: pprof top 20 before/after, block+mutex top 20 before/after, benchmark table diff.

Stop condition: after one attempt to focus on `runtime.*` symbols yields no user-level callers, abort that line of inquiry and pivot to user-level concurrency symbols (`Take*Fast`, `parallel.For`, pool worker loop).

## Arrow C++ Reference

Always consult `docs/design/engine/arrow_cpp_research.md` before proposing engine changes (join/groupby/sort/parallelism). Use its cited sources when making claims and avoid uncited speculation.
