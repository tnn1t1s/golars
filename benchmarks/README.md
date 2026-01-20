# golars vs Polars Benchmarks

This directory contains benchmarks comparing golars (Go implementation) with Polars (Rust/Python) using industry-standard tests.

## Polars Comparability

**IMPORTANT**: Not all benchmarks are directly comparable with Polars. See table below.

| Suite | Benchmark | Polars Comparable | Notes |
|-------|-----------|-------------------|-------|
| groupby | Q1-Q6 | Yes | Matches Polars test_group_by.py |
| groupby | Q7 | **No** | Requires expression arithmetic (max-min) |
| groupby | Q8 | **No** | Requires top_k, drop_nulls, explode |
| groupby | Q9 | **No** | Requires correlation function |
| groupby | Q10 | Yes | Matches Polars test_group_by.py |
| filter | Filter1, Filter2 | Yes | Matches Polars test_filter.py |
| filter | Others | No | golars-specific tests |
| join | All | Partial | Different join types than Polars test_join_where.py |
| io | All | Partial | Polars tests include lazy scan |
| agg, sort | All | No | golars-specific, not in Polars test suite |

Polars benchmark source: https://github.com/pola-rs/polars/tree/main/py-polars/tests/benchmark

## Overview

The benchmarks are based on:
- **H2O.ai db-benchmark**: Group-by aggregation queries (Q1-Q6, Q10)
- **Filter operations**: String equality filters
- **I/O operations**: CSV and Parquet read/write performance
- **Core operations**: Filtering, sorting, and data manipulation

## Directory Structure

```
benchmarks/
├── data/          # Data generation utilities
├── agg/           # Aggregation benchmarks (sum, mean, min, max, std, var, median)
├── filter/        # Filter operation benchmarks
├── groupby/       # H2O.ai group-by benchmarks (Q1-Q10)
├── join/          # Join operation benchmarks
├── sort/          # Sort benchmarks (single/multi column, asc/desc)
├── io/            # I/O benchmarks (CSV, Parquet)
├── compare/       # Comparison scripts and analysis tools
└── results/       # Benchmark results (gitignored)
```

## Running Benchmarks

### Quick Start

```bash
# Run all benchmarks with medium dataset
make benchmark-all

# Run specific benchmark suite
make benchmark-agg       # Aggregation operations (sum, mean, min, max, std, var, median)
make benchmark-filter    # Filter operations (simple, compound, string, OR)
make benchmark-sort      # Sort operations (single/multi column, asc/desc, int/string/float)
make benchmark-groupby   # H2O.ai group-by queries (Q1-Q10)
make benchmark-join      # Join operations (inner, left, multi-key)
make benchmark-io        # I/O operations (CSV, Parquet read/write)

# Run with different data sizes
make benchmark-all SIZE=small    # 10K rows, fast feedback
make benchmark-all SIZE=medium   # 250K rows (default, memory-safe)
make benchmark-all SIZE=large    # 10M rows, production-like

# Run with more iterations for statistical accuracy
make benchmark-all COUNT=5
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

### Aggregation Benchmarks

| Benchmark | Description |
|-----------|-------------|
| Sum | Sum of integer column |
| Mean | Mean of float column |
| Min | Minimum value |
| Max | Maximum value |
| Std | Standard deviation |
| Var | Variance |
| Median | Median value |
| Count | Row count |

### Filter Benchmarks

| Benchmark | Description |
|-----------|-------------|
| FilterSimple | Single column comparison (v1 > 5) |
| FilterCompound | AND condition (v1 > 5 AND v2 < 10) |
| FilterString | String equality (id1 == "id010") |
| FilterOr | OR condition (v1 > 4 OR v2 < 5) |

### Sort Benchmarks

| Benchmark | Description |
|-----------|-------------|
| SortSingleInt | Sort by single integer column |
| SortSingleString | Sort by single string column |
| SortMultiColumn | Sort by multiple columns |
| SortDescending | Sort in descending order |
| SortFloat | Sort by float column |

### Join Benchmarks

| Benchmark | Description |
|-----------|-------------|
| InnerJoin | Inner join on single column |
| LeftJoin | Left join on single column |
| MultiKeyJoin | Join on multiple columns |

### I/O Benchmarks

| Benchmark | Description |
|-----------|-------------|
| WriteCSV | Write DataFrame to CSV |
| ReadCSV | Read DataFrame from CSV |
| WriteParquet | Write DataFrame to Parquet |
| ReadParquet | Read DataFrame from Parquet |

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

## How Benchmark Results are Collected

The golars benchmark suite provides comprehensive tools for collecting, storing, and analyzing performance metrics. This section details the complete process from running benchmarks to analyzing results.

### Collection Process Overview

1. **Benchmark Execution**: Uses Go's built-in testing framework
2. **Result Capture**: Output is saved in standardized formats
3. **Data Parsing**: Scripts extract metrics from raw output
4. **Analysis**: Tools compare golars vs Polars performance
5. **Visualization**: Results are formatted for easy interpretation

### Running Benchmarks

#### Using Go Test Directly

The most basic way to run benchmarks:

```bash
# Run all benchmarks in a package
go test -bench=. ./groupby -benchmem

# Run specific benchmark
go test -bench=BenchmarkGroupByQ1_Small ./groupby -benchmem

# Run with multiple iterations for statistical accuracy
go test -bench=. ./groupby -benchmem -count=5 -benchtime=10x
```

**Command flags explained:**
- `-bench=.`: Run all benchmarks (or use regex pattern)
- `-benchmem`: Include memory allocation statistics
- `-count=5`: Run each benchmark 5 times
- `-benchtime=10x`: Run each benchmark for 10 iterations (or use duration like `10s`)
- `-cpuprofile=cpu.prof`: Generate CPU profile
- `-memprofile=mem.prof`: Generate memory profile
- `-run=^$`: Skip regular tests, only run benchmarks

#### Using Make Commands

```bash
# Run group-by benchmarks and save results
make benchmark-groupby SIZE=medium

# Run all benchmark suites
make benchmark-all SIZE=large

# Compare with Polars
make compare SIZE=medium
```

#### Using Just Commands (Recommended)

The justfile provides the most comprehensive benchmark automation:

```bash
# Light benchmarks (Q1-Q6, small dataset)
just run-light-golars
just run-light-polars
just compare-light-benchmarks

# Full benchmarks (all queries, medium dataset)
just run-full-golars
just run-full-polars
just compare-full-benchmarks

# Heavy benchmarks (all queries, large dataset)
just run-heavy-golars
just run-heavy-polars
just compare-heavy-benchmarks
```

### Understanding Benchmark Output

#### Go Benchmark Format

Go benchmarks produce output in this format:

```
BenchmarkGroupByQ1_Small-8   	     306	   3492280 ns/op	  890327 B/op	   41158 allocs/op
```

**Breaking down each column:**
1. **Benchmark name**: `BenchmarkGroupByQ1_Small-8`
   - Function name: `BenchmarkGroupByQ1_Small`
   - `-8`: Number of GOMAXPROCS (CPU cores used)

2. **Iterations**: `306`
   - Number of times the benchmark was run
   - Go automatically adjusts this for reliable timing

3. **Time per operation**: `3492280 ns/op`
   - Average nanoseconds per benchmark iteration
   - Convert to other units: 3.49ms in this example

4. **Memory per operation**: `890327 B/op`
   - Bytes allocated per iteration
   - Includes all heap allocations during the operation

5. **Allocations per operation**: `41158 allocs/op`
   - Number of heap allocations
   - High allocation counts can indicate GC pressure

#### Statistical Output (with -count flag)

When using `-count=5`, you might see:

```
BenchmarkGroupByQ1_Small-8   	     300	   3492280 ns/op	  890327 B/op	   41158 allocs/op
BenchmarkGroupByQ1_Small-8   	     298	   3510234 ns/op	  890327 B/op	   41158 allocs/op
BenchmarkGroupByQ1_Small-8   	     302	   3488901 ns/op	  890327 B/op	   41158 allocs/op
BenchmarkGroupByQ1_Small-8   	     299	   3502111 ns/op	  890327 B/op	   41158 allocs/op
BenchmarkGroupByQ1_Small-8   	     301	   3495672 ns/op	  890327 B/op	   41158 allocs/op
```

This provides data for calculating statistics like median, standard deviation, etc.

### Result Storage

#### Directory Structure

```
benchmarks/
└── results/              # All results stored here (gitignored)
    ├── golars_light.txt      # Light benchmark results
    ├── golars_full.txt       # Full benchmark results
    ├── golars_heavy.txt      # Heavy benchmark results
    ├── polars_light.txt      # Polars equivalent results
    ├── polars_full.json      # Polars JSON format results
    └── comparison_report.md  # Generated comparison report
```

#### File Formats

**Golars results** (`.txt` files):
- Raw output from `go test -bench`
- Plain text format, easy to parse with regex
- Includes all benchmark runs if using `-count`

**Polars results** (`.json` files):
- JSON format from pytest-benchmark
- Contains detailed statistics
- Includes environment information

### Data Extraction and Parsing

#### Parsing Golars Results

The `analyze.py` script extracts metrics using regex:

```python
def parse_golars_results(txt_path: Path) -> Dict[str, Tuple[float, int]]:
    """Parse Go benchmark output."""
    results = {}
    
    with open(txt_path) as f:
        for line in f:
            # Parse lines like: BenchmarkGroupByQ1_Medium-8    1234    987654 ns/op    12345 B/op    67 allocs/op
            match = re.search(r'BenchmarkGroupBy(Q\d+)_\w+.*?\s+\d+\s+(\d+)\s+ns/op\s+(\d+)\s+B/op', line)
            if match:
                query = match.group(1).lower()
                time_ns = int(match.group(2))
                memory_bytes = int(match.group(3))
                # Convert nanoseconds to seconds
                results[query] = (time_ns / 1e9, memory_bytes)
    
    return results
```

#### Parsing Polars Results

Polars/pytest-benchmark produces JSON with this structure:

```json
{
  "benchmarks": [
    {
      "name": "test_groupby_h2oai_q1",
      "stats": {
        "min": 0.0421,
        "max": 0.0456,
        "mean": 0.0435,
        "median": 0.0433,
        "stddev": 0.0012,
        "rounds": 5,
        "iterations": 1
      }
    }
  ]
}
```

### Performance Metrics Calculated

#### Primary Metrics

1. **Execution Time**
   - Wall-clock time for the operation
   - Measured in nanoseconds, converted to milliseconds/seconds
   - Lower is better

2. **Memory Usage**
   - Total heap memory allocated
   - Measured in bytes, converted to MB/GB
   - Lower is better

3. **Allocation Count**
   - Number of heap allocations
   - Indicates GC pressure
   - Lower is better

#### Derived Metrics

1. **Throughput**
   - Rows processed per second
   - Calculated as: `rows / execution_time`
   - Higher is better

2. **Memory Efficiency**
   - Bytes per row: `memory_usage / rows`
   - Indicates memory scalability
   - Lower is better

3. **Speedup Ratio**
   - Comparison between golars and Polars
   - Calculated as: `polars_time / golars_time`
   - Values > 1.0 mean golars is faster

### Analysis Tools

#### Basic Analysis

```bash
# Analyze light benchmark results
just analyze-light-results

# Custom analysis with Python script
python compare/analyze.py \
  --golars results/golars_full.txt \
  --polars results/polars_full.json
```

#### Analysis Output Example

```
Benchmark Comparison Report
==========================

Dataset: Medium (1,000,000 rows)

Query Performance:
┌─────────┬──────────────┬──────────────┬──────────┬──────────────┐
│ Query   │ Golars (ms)  │ Polars (ms)  │ Speedup  │ Memory (MB)  │
├─────────┼──────────────┼──────────────┼──────────┼──────────────┤
│ Q1      │ 45.2 ± 1.2   │ 38.5 ± 0.8   │ 0.85x    │ 125.4        │
│ Q2      │ 67.8 ± 2.1   │ 55.3 ± 1.5   │ 0.82x    │ 187.6        │
│ Q3      │ 89.3 ± 3.2   │ 71.2 ± 2.1   │ 0.80x    │ 215.8        │
└─────────┴──────────────┴──────────────┴──────────┴──────────────┘

Throughput (rows/sec):
- Golars Q1: 22,123,893
- Polars Q1: 25,974,026
```

### Advanced Features

#### CPU Profiling

```bash
# Generate CPU profile
go test -bench=BenchmarkGroupByQ1 ./groupby -cpuprofile=cpu.prof

# Analyze profile
go tool pprof cpu.prof
```

#### Memory Profiling

```bash
# Generate memory profile
go test -bench=BenchmarkGroupByQ1 ./groupby -memprofile=mem.prof

# Analyze allocations
go tool pprof -alloc_space mem.prof
```

#### Continuous Benchmarking

For CI/CD integration:

```bash
# Save results with timestamp
DATE=$(date +%Y%m%d_%H%M%S)
go test -bench=. ./groupby -benchmem | tee results/golars_${DATE}.txt

# Compare with previous run
python compare/analyze_trends.py \
  --current results/golars_${DATE}.txt \
  --previous results/golars_previous.txt
```

### Best Practices

1. **Warm-up Runs**: Go benchmarks automatically handle warm-up
2. **Multiple Iterations**: Use `-count=5` or more for statistical validity
3. **Consistent Environment**: 
   - Close other applications
   - Disable CPU frequency scaling
   - Use consistent GOMAXPROCS
4. **Data Size Selection**:
   - Small: Quick development feedback
   - Medium: Standard comparisons
   - Large: Production-like performance
5. **Result Validation**: Always verify operations produce correct results

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