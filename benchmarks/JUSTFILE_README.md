# Golars Benchmarks with Justfile

This directory now uses `just` for running benchmarks with a consistent naming convention.

## Naming Convention

All commands follow the pattern: `{verb}-{adjective}-{noun}`

- **Verbs**: `generate`, `run`, `compare`, `analyze`, `clean`, `list`, `show`
- **Adjectives**: `light`, `full`, `heavy`, `all`
- **Nouns**: `data`, `golars`, `polars`, `benchmarks`, `results`

## Quick Start

```bash
# Show all available commands
just

# Show benchmark implementation status
just show-benchmark-status

# Run the working benchmarks (Q1-Q6)
just compare-light-benchmarks

# Run everything
just test-quick-benchmarks
```

## Benchmark Levels

### Light (Recommended for Development)
- **Queries**: Q1-Q6 (fully implemented)
- **Data**: Small dataset (10K rows)
- **Purpose**: Quick iteration during development

```bash
just run-light-golars
just run-light-polars
just compare-light-benchmarks
```

### Full (Standard Benchmarks)
- **Queries**: All (Q1-Q10)
- **Data**: Medium dataset (100K rows)
- **Purpose**: Performance testing
- **Note**: Q7-Q10 have limited functionality

```bash
just run-full-golars
just run-full-polars
just compare-full-benchmarks
```

### Heavy (Stress Testing)
- **Queries**: All (Q1-Q10)
- **Data**: Large dataset (1M rows)
- **Purpose**: Scalability testing

```bash
just run-heavy-golars
just run-heavy-polars
just compare-heavy-benchmarks
```

## Common Workflows

### Quick benchmark test
```bash
just test-quick-benchmarks
```

### Generate data only
```bash
just generate-light-data    # 10K rows
just generate-medium-data   # 100K rows
just generate-heavy-data    # 1M rows
```

### Clean up
```bash
just clean-all-data        # Remove generated data
just clean-all-results     # Remove benchmark results
just clean-all-everything  # Remove everything
```

### Analysis
```bash
just analyze-light-results   # After running light benchmarks
just analyze-full-results    # After running full benchmarks
```

## Implementation Status

Use `just show-benchmark-status` to see which benchmarks are working:

- ✅ **Q1-Q6**: Fully implemented (sum, mean, count, median, std)
- ⚠️ **Q7-Q8**: Partially working (need expression support, topk)
- ❌ **Q9-Q10**: Not implemented (need correlation)

## Tips

1. Start with `just compare-light-benchmarks` for quick results
2. Data is automatically generated if missing
3. Results are saved in `compare/results/`
4. The justfile handles all the complex go test commands
5. Use `just list-all-benchmarks` for detailed command descriptions