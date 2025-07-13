# Group-By Benchmarks

This directory contains golars implementations of the H2O.ai db-benchmark group-by queries, matching those in Polars' benchmark suite.

## Query Mapping

| Query | golars Function | Polars Function | Description |
|-------|----------------|-----------------|-------------|
| Q1 | `BenchmarkGroupByQ1` | `test_groupby_h2oai_q1` | Group by single column (id1), sum aggregation on v1 |
| Q2 | `BenchmarkGroupByQ2` | `test_groupby_h2oai_q2` | Group by two columns (id1, id2), sum aggregation on v1 |
| Q3 | `BenchmarkGroupByQ3` | `test_groupby_h2oai_q3` | Group by id3, sum v1 and mean v3 |
| Q4 | `BenchmarkGroupByQ4` | `test_groupby_h2oai_q4` | Group by id4, mean of v1, v2, v3 |
| Q5 | `BenchmarkGroupByQ5` | `test_groupby_h2oai_q5` | Group by id6, sum of v1, v2, v3 |
| Q6 | `BenchmarkGroupByQ6` | `test_groupby_h2oai_q6` | Group by id4+id5, median and std of v3 |
| Q7 | `BenchmarkGroupByQ7` | `test_groupby_h2oai_q7` | Group by id3, range calculation (max(v1) - min(v2)) |
| Q8 | `BenchmarkGroupByQ8` | `test_groupby_h2oai_q8` | Group by id6, top 2 values of v3 |
| Q9 | `BenchmarkGroupByQ9` | `test_groupby_h2oai_q9` | Group by id2+id4, correlation squared |
| Q10 | `BenchmarkGroupByQ10` | `test_groupby_h2oai_q10` | Group by all ID columns, sum v3 and count v1 |

## Running Benchmarks

### Run all group-by benchmarks
```bash
go test -bench=. -benchmem
```

### Run specific query
```bash
go test -bench=BenchmarkGroupByQ1 -benchmem
```

### Run specific size
```bash
go test -bench=Q1_Medium -benchmem
```

### Generate CPU profile
```bash
go test -bench=BenchmarkGroupByQ1_Medium -cpuprofile=cpu.prof
go tool pprof cpu.prof
```

## Data Sizes

Each query is tested with multiple data sizes:
- **Small**: 10K rows, 100 groups
- **Medium**: 1M rows, 1K groups  
- **Large**: 10M rows, 10K groups (disabled by default)

## Implementation Notes

### Current Limitations

Some golars features may not be fully implemented yet:
- **Q6**: Median and standard deviation functions
- **Q8**: Top-k selection and null handling
- **Q9**: Correlation function

These benchmarks use placeholder implementations where necessary and should be updated as golars adds these features.

### Memory Considerations

The test data is loaded once during initialization to avoid repeated generation overhead. For large datasets, this can consume significant memory. The large dataset is commented out by default.

## Comparing with Polars

To run the equivalent Polars benchmarks:

```bash
cd /path/to/polars/py-polars
pytest tests/benchmark/test_group_by.py -v --benchmark-only
```

To run a specific Polars query:
```bash
pytest tests/benchmark/test_group_by.py::test_groupby_h2oai_q1 -v --benchmark-only
```