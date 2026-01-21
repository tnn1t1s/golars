# Baseline Profiles (Step 2)

Goal: capture CPU + allocation profiles for IO, compute, and expression hot paths
using existing benchmarks. Profiles are stored under `/tmp` for now.

## Environment

- GOCACHE: `/Users/palaitis/Development/golars/.gocache`
- IO shared data: `GOLARS_IO_BENCH_DATA_DIR=/tmp/golars-io-data`

Note: `go tool pprof` is not available in the current Go toolchain, and
network access is blocked, so profiles were captured but not analyzed.
Standalone `pprof` is available at `$(go env GOPATH)/bin/pprof` and was used
to extract the summaries below.

## IO (Parquet read)

Command:

```
GOCACHE=/Users/palaitis/Development/golars/.gocache \
GOLARS_IO_BENCH_DATA_DIR=/tmp/golars-io-data \
go test ./benchmarks/io/size -run ^$ -bench BenchmarkParquetRead_Medium -count=1 \
  -cpuprofile /tmp/golars_parquetread_cpu.pprof \
  -memprofile /tmp/golars_parquetread_mem.pprof
```

Result:
- `BenchmarkParquetRead_Medium`: 4,623,443 ns/op (100,000 rows)

Profiles:
- `/tmp/golars_parquetread_cpu.pprof`
- `/tmp/golars_parquetread_mem.pprof`

pprof (CPU top):
- runtime scheduling/condvar wait dominates (thread wake/sleep).
- Arrow parquet decode shows up via `pqarrow.(*ColumnReader).NextBatch`,
  `parquet/file.(*recordReader).ReadRecords`, and dictionary decoders.

pprof (alloc_space top):
- 92%+ of allocations in `arrow/memory.(*GoAllocator).Allocate`.
- Heavy reserve/resize in `arrow/array.(*BinaryBuilder)` and parquet record readers.

## Compute (aggregation)

Command:

```
GOCACHE=/Users/palaitis/Development/golars/.gocache \
go test ./benchmarks/agg -run ^$ -bench BenchmarkMean_Medium -count=1 \
  -cpuprofile /tmp/golars_mean_cpu.pprof \
  -memprofile /tmp/golars_mean_mem.pprof
```

Result:
- `BenchmarkMean_Medium`: 3,714,789 ns/op

Profiles:
- `/tmp/golars_mean_cpu.pprof`
- `/tmp/golars_mean_mem.pprof`

## Expressions (WithColumns)

Command:

```
GOCACHE=/Users/palaitis/Development/golars/.gocache \
go test ./benchmarks/with_columns -run ^$ -bench BenchmarkWithColumnsSmall -count=1 \
  -cpuprofile /tmp/golars_withcolumns_cpu.pprof \
  -memprofile /tmp/golars_withcolumns_mem.pprof
```

Result:
- `BenchmarkWithColumnsSmall`: 335,908 ns/op

Profiles:
- `/tmp/golars_withcolumns_cpu.pprof`
- `/tmp/golars_withcolumns_mem.pprof`

## Next

- Re-run `pprof -top` once a local `pprof` binary is available to extract hot paths.
- Add a small helper script/Make target for consistent profile capture and analysis.
