# golars vs Polars Benchmarks

This directory contains comprehensive benchmarks comparing golars (Go implementation) with Polars (Rust/Python) using industry-standard tests.

## Overview

The benchmarks are based on:
- **H2O.ai db-benchmark**: Group-by aggregation queries (Q1-Q10)
- **TPC-H inspired**: Join and complex query patterns
- **I/O operations**: CSV and Parquet read/write performance
- **Core operations**: Filtering, sorting, and data manipulation

## Directory Structure

```
benchmarks/
├── data/          # Data generation utilities
├── groupby/       # H2O.ai group-by benchmarks
├── filter/        # Filter operation benchmarks
├── join/          # Join operation benchmarks
├── io/            # I/O benchmarks (CSV, Parquet)
├── compare/       # Comparison scripts and analysis tools
└── results/       # Benchmark results (gitignored)
```

## Running Benchmarks

### Quick Start

```bash
# Run all benchmarks with medium dataset (1M rows)
make benchmark-all

# Run specific benchmark suite
make benchmark-groupby
make benchmark-filter
make benchmark-join
make benchmark-io

# Run with different data sizes
BENCH_SIZE=small make benchmark-all   # 10K rows
BENCH_SIZE=medium make benchmark-all  # 1M rows (default)
BENCH_SIZE=large make benchmark-all   # 10M rows
```

### Detailed Usage

#### 1. Generate Test Data

```bash
# Generate H2O.ai dataset
go run data/generate.go -size medium -output data/h2oai_medium.parquet

# Available sizes:
# - small: 10K rows, 100 groups
# - medium: 1M rows, 1K groups
# - large: 10M rows, 10K groups
# - xlarge: 100M rows, 100K groups
```

#### 2. Run golars Benchmarks

```bash
# Run all group-by benchmarks
go test -bench=. ./groupby -benchmem -benchtime=10x

# Run specific query
go test -bench=BenchmarkGroupByQ1 ./groupby -benchmem

# With CPU profiling
go test -bench=BenchmarkGroupByQ1 ./groupby -cpuprofile=cpu.prof
```

#### 3. Run Polars Benchmarks

```bash
# Run equivalent Polars benchmarks
cd compare
python run_polars.py --size medium --suite groupby

# Run specific query
python run_polars.py --size medium --query q1
```

#### 4. Compare Results

```bash
# Generate comparison report
python compare/analyze.py --golars results/golars_medium.json --polars results/polars_medium.json

# Generate visualization
python compare/visualize.py --output results/comparison_charts.html
```

## Benchmark Details

### H2O.ai Group-By Queries

| Query | Description | Operations |
|-------|-------------|------------|
| Q1 | Simple group-by | Group by 1 column, sum aggregation |
| Q2 | Two-column group-by | Group by 2 columns, sum aggregation |
| Q3 | Mixed aggregations | Group by 1 column, sum and mean |
| Q4 | Multiple means | Group by 1 column, 3 mean aggregations |
| Q5 | Multiple sums | Group by 1 column, 3 sum aggregations |
| Q6 | Statistical | Group by 2 columns, median and std dev |
| Q7 | Range calculation | Group by 1 column, max-min |
| Q8 | Top-k | Group by 1 column, top 2 values |
| Q9 | Correlation | Group by 2 columns, correlation squared |
| Q10 | All columns | Group by all ID columns, sum and count |

### Data Schema

```
H2O.ai Dataset:
- id1, id2, id3: String grouping columns
- id4, id5, id6: Int32 grouping columns
- v1, v2: Int32 value columns
- v3: Float64 value column
- 5% null values in all columns
```

## Performance Metrics

Each benchmark collects:
- **Execution Time**: Wall-clock time for operation
- **Memory Usage**: Peak RSS and allocations
- **CPU Usage**: User and system time
- **Throughput**: Rows/second processed
- **Result Hash**: For correctness validation

## Results Format

Results are stored in JSON format:

```json
{
  "suite": "groupby",
  "query": "q1",
  "size": "medium",
  "rows": 1000000,
  "implementation": "golars",
  "runs": [
    {
      "duration_ms": 45.2,
      "memory_mb": 125.4,
      "cpu_percent": 98.5,
      "result_hash": "abc123..."
    }
  ],
  "summary": {
    "median_ms": 45.8,
    "mean_ms": 46.1,
    "std_ms": 1.2,
    "min_ms": 44.1,
    "max_ms": 48.9
  }
}
```

## Contributing

When adding new benchmarks:
1. Ensure equivalent implementations in both golars and Polars
2. Use the same data generation seeds
3. Validate results match between implementations
4. Document any limitations or differences
5. Add to the appropriate test suite

## Known Differences

Some operations may differ between golars and Polars:
- Null handling in certain aggregations
- Floating-point precision in statistical functions
- Memory allocation patterns due to Go GC vs Rust ownership

These are documented in each benchmark file.