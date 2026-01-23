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
| InnerJoin | small | 161.7 us | 965,704 | 184 |
| LeftJoin | small | 161.7 us | 965,704 | 184 |
| MultiKeyJoin (id1,id2) | small | 1.43 ms | 4,316,064 | 55,652 |
| InnerJoin | medium-safe | 18.65 ms | 88,394,226 | 871 |
| LeftJoin | medium-safe | 3.50 ms | 19,277,667 | 577 |
| MultiKeyJoin (id1,id2) | medium-safe | 49.39 ms | 165,639,194 | 1,248,212 |
| MultiKeyJoinIndices (id1,id2) | medium-safe | 33.75 ms | 91,629,703 | 1,247,877 |
