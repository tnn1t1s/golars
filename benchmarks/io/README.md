# IO Benchmarks

Comprehensive IO benchmark suite for comparing golars and Polars performance.

## Directory Structure

```
benchmarks/io/
├── data/           # Data generation helpers
│   └── generator.go
├── size/           # Size ladder benchmarks (10K → 10M rows)
│   └── size_test.go
├── width/          # Column width benchmarks (3 → 200 cols)
│   └── width_test.go
├── projection/     # Column projection benchmarks
│   └── projection_test.go
├── format/         # Parquet vs CSV comparison
│   └── format_test.go
├── polars_comparison.py  # Matching Polars benchmarks
└── README.md
```

## Running Benchmarks

### Golars

```bash
# All IO benchmarks (may take several minutes)
go test -bench=. ./benchmarks/io/... -benchmem

# Size ladder only
go test -bench=. ./benchmarks/io/size

# Width ladder only
go test -bench=. ./benchmarks/io/width

# Skip huge (10M row) benchmarks
go test -bench=. ./benchmarks/io/size -short

# With custom iteration count
go test -bench=. ./benchmarks/io/size -benchtime=10x
```

### Shared External Files

To ensure golars and Polars read identical files, set `GOLARS_IO_BENCH_DATA_DIR`.
When set, golars benchmarks will read/write data under that directory and keep
read inputs in place for reuse by the Polars comparison script.

Example:

```bash
export GOLARS_IO_BENCH_DATA_DIR=/tmp/golars-io-data
go test -bench=. ./benchmarks/io/... -run ^$ -benchmem
../polars/.venv/bin/python benchmarks/io/polars_comparison.py
```

### Polars

```bash
# Requires Polars installed in a Python environment
python benchmarks/io/polars_comparison.py
```

## Benchmark Categories

### Size Ladder

Tests scaling behavior across data sizes:

| Size   | Rows       | Groups  |
|--------|------------|---------|
| Small  | 10,000     | 100     |
| Medium | 100,000    | 1,000   |
| Large  | 1,000,000  | 10,000  |
| Huge   | 10,000,000 | 100,000 |

Uses H2O.ai benchmark schema (9 columns: 3 string, 6 numeric).

### Width Ladder

Tests scaling with column count (fixed 100K rows):

| Width     | Columns | String | Int | Float |
|-----------|---------|--------|-----|-------|
| Narrow    | 3       | 1      | 1   | 1     |
| Medium    | 9       | 3      | 4   | 2     |
| Wide      | 50      | 10     | 20  | 20    |
| VeryWide  | 200     | 50     | 100 | 50    |

### Projection

Tests column projection efficiency (reading subset of columns):

- 1 of 50 columns
- 5 of 50 columns
- 10 of 50 columns
- 25 of 50 columns
- All 50 columns (baseline)

### Format Comparison

Direct Parquet vs CSV comparison:

- Read/write speed
- File size
- Compression options (Parquet only)

## Sample Results

Run on Apple M4, 100K rows, 9 columns:

| Operation     | Golars (µs) | Polars (µs) | Ratio |
|---------------|-------------|-------------|-------|
| ParquetRead   | 4,659       | 54,227      | 11.6x golars |
| ParquetWrite  | 57,003      | 61,391      | 1.1x golars |
| CSVRead       | 49,604      | 61,547      | 1.2x golars |
| CSVWrite      | 60,268      | 34,256      | 1.8x polars |

Note: Results vary by hardware, data characteristics, and library versions.

## Adding New Benchmarks

1. Add data generation to `data/generator.go` if needed
2. Create new test file in appropriate subdirectory
3. Add matching Python benchmark to `polars_comparison.py`
4. Update this README
