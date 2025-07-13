# Minimal Golars Benchmarks

## Currently Working Benchmarks

The following H2O.ai group-by benchmarks are fully implemented and working:

### Q1-Q6: Basic and Advanced Aggregations

1. **Q1** - Simple sum aggregation (group by 1 column)
   - `df.group_by("id1").agg(pl.sum("v1"))`

2. **Q2** - Sum with 2 group-by columns
   - `df.group_by("id1", "id2").agg(pl.sum("v1"))`

3. **Q3** - Sum and mean aggregations
   - `df.group_by("id3").agg([pl.sum("v1"), pl.mean("v3")])`

4. **Q4** - Multiple mean aggregations
   - `df.group_by("id4").agg([pl.mean("v1"), pl.mean("v2"), pl.mean("v3")])`

5. **Q5** - Multiple sum aggregations
   - `df.group_by("id6").agg([pl.sum("v1"), pl.sum("v2"), pl.sum("v3")])`

6. **Q6** - Median and standard deviation (newly implemented!)
   - `df.group_by("id4", "id5").agg([pl.median("v3"), pl.std("v3")])`

## Running the Minimal Benchmarks

### Quick Test (golars only)
```bash
cd benchmarks/groupby
go test -bench="BenchmarkGroupBy(Q[1-6])_Small" -benchtime=1x -run=^$
```

### Full Comparison with Polars
```bash
cd benchmarks/compare
./run_minimal_comparison.sh small  # or medium/large
```

## Implemented Aggregation Functions

✅ **Fully Implemented:**
- Sum
- Mean
- Min
- Max
- Count
- Median (new!)
- Standard Deviation (new!)
- Variance (new!)
- First (new!)
- Last (new!)

❌ **Not Yet Implemented:**
- Correlation (needed for Q9)
- TopK (needed for Q8)
- Complex expressions in aggregations (needed for Q7: max - min)

## Performance Notes

- Q1-Q5 use basic aggregations that have been optimized
- Q6 uses the newly implemented median and std functions
- Golars is written in Go, while Polars is written in Rust
- Performance comparisons should consider the language differences