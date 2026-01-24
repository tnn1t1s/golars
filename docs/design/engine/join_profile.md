# Join CPU Profile (InnerJoin + LeftJoin MediumSafe)

Profile files:
- `profiles/join_inner_mediumsafe.pprof`
- `profiles/join_left_mediumsafe.pprof`

Command (GOCACHE workaround for local permissions):
```
env GOCACHE=/tmp/go-build-cache go test ./benchmarks/join -run '^$' -bench BenchmarkInnerJoin_MediumSafe -count 1 -benchtime=5s -cpuprofile profiles/join_inner_mediumsafe.pprof
env GOCACHE=/tmp/go-build-cache go test ./benchmarks/join -run '^$' -bench BenchmarkLeftJoin_MediumSafe -count 1 -benchtime=5s -cpuprofile profiles/join_left_mediumsafe.pprof
```

Top CPU consumers (flat time, from `pprof -top`):

Inner join (medium-safe):
- `runtime.pthread_cond_signal` ~51.8%
- `runtime.pthread_cond_wait` ~11.7%
- `runtime.usleep` ~5.1%
- `internal/chunked.(*ChunkedArray[int32]).copyChunkToSlice` ~2.8%
- `internal/chunked.(*ChunkedArray[string]).copyChunkToSlice` ~2.6%
- `runtime.mapaccess2_faststr` ~1.9%
- `series.TakeStringFast.func1` ~1.5%
- `internal/compute.probeManyUint32` ~0.7% flat (~3.3% cum)
- `internal/compute.dictEncodeJoinStrings` ~0.1% flat (~3.4% cum)

Left join (medium-safe):
- `runtime.pthread_cond_signal` ~38.1%
- `runtime.pthread_cond_wait` ~12.2%
- `runtime.mapaccess2_faststr` ~6.9% (9.3% cum)
- `internal/chunked.(*ChunkedArray[string]).copyChunkToSlice` ~6.4% (9.0% cum)
- `internal/compute.dictEncodeJoinStrings` ~1.7% (16.2% cum)
- `internal/compute.leftJoinParallelUint32.func1` ~0.7% (7.1% cum)

Notes:
- The profile is dominated by runtime thread synchronization (`pthread_cond_*`, `usleep`), which suggests significant time waiting on the parallel worker pool.
- String join probe and chunked copy into slices show up, indicating result materialization costs are still visible.
- `dictEncodeJoinStrings` now appears in the top stack for left join; encoding cost may be worth amortizing or caching per column.
