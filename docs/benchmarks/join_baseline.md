# Join Benchmarks Baseline

Baseline results for `benchmarks/join` on darwin/arm64.

Command:
```
go test ./benchmarks/join -run '^$' -bench 'Benchmark(InnerJoin|LeftJoin|MultiKeyJoin)_(Small|MediumSafe)$|BenchmarkMultiKeyJoinIndices_MediumSafe' -benchmem -count 3
```

Datasets:
- `h2oai_small.parquet` (H2OAI small)
- `h2oai_medium-safe.parquet` (H2OAI medium-safe)

Median of 3 runs (ns/op converted to ms/us for readability).

| Benchmark | Dataset | Time | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| InnerJoin | small | 157.4 us | 965,704 | 184 |
| LeftJoin | small | 158.7 us | 965,704 | 184 |
| MultiKeyJoin (id1,id2) | small | 577.0 us | 3,537,624 | 6,629 |
| InnerJoin | medium-safe | 18.24 ms | 88,394,575 | 871 |
| LeftJoin | medium-safe | 3.47 ms | 19,277,707 | 577 |
| MultiKeyJoin (id1,id2) | medium-safe | 30.14 ms | 136,872,804 | 145,604 |
| MultiKeyJoinIndices (id1,id2) | medium-safe | 14.35 ms | 59,242,092 | 145,255 |
