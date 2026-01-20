# golars Benchmarks

This directory contains benchmarks that are **exact translations** of Polars' benchmark suite, enabling direct performance comparison.

## Polars Source Mapping

All benchmarks are direct translations from the Polars repository:
- **Source**: https://github.com/pola-rs/polars/tree/main/py-polars/tests/benchmark
- **Data Config**: `generate_group_by_data(10_000, 100, null_ratio=0.05)` from `conftest.py`

### Benchmark Comparability Matrix

| golars Benchmark | Polars Test | Status | Notes |
|------------------|-------------|--------|-------|
| `BenchmarkGroupByH2OAI_Q1` | `test_groupby_h2oai_q1` | Comparable | group_by("id1").agg(sum("v1")) |
| `BenchmarkGroupByH2OAI_Q2` | `test_groupby_h2oai_q2` | Comparable | group_by("id1", "id2").agg(sum("v1")) |
| `BenchmarkGroupByH2OAI_Q3` | `test_groupby_h2oai_q3` | Comparable | group_by("id3").agg(sum("v1"), mean("v3")) |
| `BenchmarkGroupByH2OAI_Q4` | `test_groupby_h2oai_q4` | Comparable | group_by("id4").agg(mean("v1"), mean("v2"), mean("v3")) |
| `BenchmarkGroupByH2OAI_Q5` | `test_groupby_h2oai_q5` | Comparable | group_by("id6").agg(sum("v1"), sum("v2"), sum("v3")) |
| `BenchmarkGroupByH2OAI_Q6` | `test_groupby_h2oai_q6` | Comparable | group_by("id4", "id5").agg(median("v3"), std("v3")) |
| `BenchmarkGroupByH2OAI_Q7` | `test_groupby_h2oai_q7` | **SKIPPED** | Requires expression arithmetic (max-min) |
| `BenchmarkGroupByH2OAI_Q8` | `test_groupby_h2oai_q8` | **SKIPPED** | Requires top_k, drop_nulls, explode |
| `BenchmarkGroupByH2OAI_Q9` | `test_groupby_h2oai_q9` | **SKIPPED** | Requires correlation in aggregations |
| `BenchmarkGroupByH2OAI_Q10` | `test_groupby_h2oai_q10` | Comparable | group_by(all_ids).agg(sum("v3"), count("v1")) |
| `BenchmarkFilter1` | `test_filter1` | Comparable | filter(id1 == "id046") with aggregation |
| `BenchmarkFilter2` | `test_filter2` | Comparable | filter(id1 != "id046") with aggregation |
| `BenchmarkWriteReadFilterCSV` | `test_write_read_scan_large_csv` | Partial | No lazy scan_csv equivalent |

### Feature Gaps

Benchmarks Q7, Q8, Q9 are skipped because golars does not yet support:

- **Q7**: Expression arithmetic in aggregations (`pl.max("v1") - pl.min("v2")`)
- **Q8**: `top_k()` aggregation and `explode()` operation
- **Q9**: `corr()` function in aggregation context

These skips are intentional to show feature parity gaps clearly.

## Directory Structure

```
benchmarks/
├── data/          # H2O.ai data generation (matches Polars datagen_groupby.py)
├── groupby/       # Polars test_group_by.py translations
├── filter/        # Polars test_filter.py translations
├── io/            # Polars test_io.py translations
├── join/          # Join operation benchmarks
├── sort/          # Sort benchmarks
└── agg/           # Direct aggregation benchmarks
```

## Running Benchmarks

### Quick Start

```bash
# Run all comparable benchmarks
go test -bench=. ./benchmarks/... -benchmem

# Run specific suite
go test -bench=. ./benchmarks/groupby -benchmem
go test -bench=. ./benchmarks/filter -benchmem
go test -bench=. ./benchmarks/io -benchmem
```

### With Multiple Iterations

```bash
# Statistical accuracy (5 runs, 10 iterations each)
go test -bench=. ./benchmarks/groupby -benchmem -count=5 -benchtime=10x
```

### Show Skipped Benchmarks

```bash
# See which benchmarks are skipped and why
go test -bench=. ./benchmarks/groupby -v 2>&1 | grep -E "(SKIP|FEATURE GAP)"
```

## Data Configuration

All benchmarks use the same data configuration as Polars:

```go
// From benchmarks/data/h2oai.go
H2OAISmall = H2OAIConfig{
    NRows:     10_000,   // Matches Polars conftest.py
    NGroups:   100,      // Matches Polars conftest.py
    NullRatio: 0.05,     // 5% nulls
    Seed:      0,
}
```

This matches Polars' `conftest.py`:
```python
@pytest.fixture(scope="session")
def groupby_data() -> pl.DataFrame:
    return generate_group_by_data(10_000, 100, null_ratio=0.05)
```

## Running Polars Benchmarks

To compare with Polars, run their benchmark suite:

```bash
cd /path/to/polars/py-polars
pytest tests/benchmark/test_group_by.py --benchmark-only -v
pytest tests/benchmark/test_filter.py --benchmark-only -v
pytest tests/benchmark/test_io.py --benchmark-only -v
```

## Understanding Results

### Go Benchmark Output

```
BenchmarkGroupByH2OAI_Q1-10    306    3492280 ns/op    890327 B/op    41158 allocs/op
```

- `Q1-10`: Benchmark name, 10 CPUs
- `306`: Iterations run
- `3492280 ns/op`: ~3.5ms per operation
- `890327 B/op`: ~870KB allocated per operation
- `41158 allocs/op`: Number of heap allocations

### Comparing with Polars

Polars pytest-benchmark outputs JSON with `mean`, `median`, `stddev` statistics. Convert golars nanoseconds to seconds for direct comparison:

- golars `3492280 ns/op` = `0.00349s` = `3.49ms`
- Polars `mean: 0.00312` = `3.12ms`

## Known Differences

1. **Lazy vs Eager**: Polars uses lazy evaluation (`.lazy().collect()`); golars is eager
2. **Null Handling**: Polars `eq_missing()` treats null=null as true; golars `Eq()` does not
3. **Memory Model**: Go GC vs Rust ownership affects allocation patterns

These differences are documented in each benchmark file with comments.
