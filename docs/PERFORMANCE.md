# Golars Performance Guide

This guide covers performance characteristics, optimization techniques, and best practices for using Golars efficiently.

## Performance Overview

Golars is designed for high performance with minimal memory allocations:

| Operation | Performance | Memory | Notes |
|-----------|------------|--------|-------|
| ChunkedArray.Get | 25ns/op | 0 B/op | Zero allocations |
| Series.Get | 35ns/op | 7 B/op | Minimal allocations |
| DataFrame.Create | 697ns/op | 504 B/op | 10 columns |
| DataFrame.Select | 437ns/op | 264 B/op | 3 from 20 columns |
| Filter 100k rows | 6ms | ~800KB | Simple condition |
| Arithmetic 100k | 1ms | 2.19 MB | Vectorized |
| Aggregation 100k | 185μs | 8 B/op | Sum operation |
| GroupBy 10k rows | ~1ms | Variable | Hash-based |
| Sort 10k rows | ~3ms | Variable | Stable sort |
| Join 1k x 1k | ~2ms | Variable | Hash join |

## Query Optimization

### Lazy Evaluation

Lazy evaluation provides significant performance improvements by:
- Avoiding intermediate DataFrame materialization
- Enabling query optimization before execution
- Reducing memory usage

```go
// Eager (creates intermediate DataFrames)
df1 := df.Filter(expr1)        // Intermediate DataFrame
df2 := df1.Filter(expr2)        // Another intermediate
df3 := df2.Select("a", "b")     // Another intermediate
result := df3.GroupBy("a").Sum("b")

// Lazy (optimized execution)
result := golars.LazyFromDataFrame(df).
    Filter(expr1).
    Filter(expr2).              // Filters will be combined
    Select("a", "b").           // Projection pushed down
    GroupBy("a").Sum("b").
    Collect()
```

### Predicate Pushdown

The optimizer automatically pushes filters closer to the data source:

```go
// Before optimization:
// Limit -> Filter -> Sort -> Filter -> Scan

// After optimization:
// Limit -> Sort -> Scan with combined filters

lf := golars.LazyFromDataFrame(df).
    Filter(expr.ColBuilder("year").Eq(expr.Lit(2023)).Build()).
    Sort("amount", true).
    Filter(expr.ColBuilder("amount").Gt(1000).Build()).
    Limit(10)

// Both filters are pushed to scan level
// Filters are combined: (year == 2023) AND (amount > 1000)
```

Performance impact:
- Reduces data movement through the pipeline
- Enables early filtering at scan level
- Optimization overhead: ~286ns/op

### Projection Pushdown

The optimizer reads only required columns:

```go
// Only reads columns: store, product, quantity
result := golars.LazyFromDataFrame(df).
    Filter(expr.ColBuilder("store").Eq(expr.Lit("NY")).Build()).
    GroupBy("product").
    Sum("quantity").
    SelectColumns("product", "quantity_sum").
    Collect()
```

Performance impact:
- Reduces memory usage
- Faster scans with fewer columns
- Optimization overhead: ~2.7μs/op

## Best Practices

### 1. Use Lazy Evaluation for Complex Queries

```go
// Good: Single execution with optimization
result := golars.LazyFromDataFrame(df).
    Filter(condition1).
    Filter(condition2).
    GroupBy("category").
    Agg(aggregations).
    Sort("total", true).
    Limit(100).
    Collect()

// Less efficient: Multiple intermediate DataFrames
result := df.
    Filter(condition1).
    Filter(condition2).
    GroupBy("category").
    Agg(aggregations).
    Sort("total", true).
    Limit(100)
```

### 2. Filter Early

Place filters as early as possible in your pipeline:

```go
// Good: Filter before expensive operations
df.Filter(expr.ColBuilder("active").Eq(expr.Lit(true)).Build()).
   GroupBy("category").
   Sum("amount")

// Less efficient: Filter after grouping
df.GroupBy("category").
   Sum("amount").
   Filter(expr.ColBuilder("amount_sum").Gt(1000).Build())
```

### 3. Select Only Required Columns

```go
// Good: Select only needed columns
df.Select("id", "name", "amount").
   Filter(expr.ColBuilder("amount").Gt(100).Build())

// Less efficient: Process all columns
df.Filter(expr.ColBuilder("amount").Gt(100).Build())
// All columns remain in memory
```

### 4. Batch Operations

```go
// Good: Single pass with multiple aggregations
df.GroupBy("category").Agg(map[string]expr.Expr{
    "total": expr.ColBuilder("amount").Sum().Build(),
    "avg": expr.ColBuilder("amount").Mean().Build(),
    "count": expr.ColBuilder("").Count().Build(),
})

// Less efficient: Multiple groupby operations
total := df.GroupBy("category").Sum("amount")
avg := df.GroupBy("category").Mean("amount")
count := df.GroupBy("category").Count()
```

### 5. Use Type-Specific Series When Possible

```go
// Good: Direct typed access
series := df.Column("amount").(*golars.TypedSeries[float64])
for i := 0; i < series.Len(); i++ {
    value := series.Get(i) // No type assertion needed
}

// Less efficient: Interface access
series := df.Column("amount")
for i := 0; i < series.Len(); i++ {
    value := series.Get(i).(float64) // Type assertion overhead
}
```

## Memory Management

### ChunkedArray Structure

ChunkedArrays store data in chunks for efficient appends:
- Each chunk is an Arrow array
- Appends may create new chunks
- No reallocation of existing data

### Zero-Copy Operations

These operations share memory without copying:
- Series.Slice()
- DataFrame.Slice()
- DataFrame.Head()
- DataFrame.Tail()

### Operations That Copy Data

These operations create new arrays:
- Filter (creates new filtered arrays)
- Sort (creates new sorted arrays)
- GroupBy aggregations (creates result arrays)
- Joins (creates combined arrays)

## Profiling and Benchmarking

### Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./...

# Run specific benchmarks
go test -bench=BenchmarkDataFrameFilter ./frame

# With memory allocation stats
go test -bench=. -benchmem ./...

# Multiple runs for stability
go test -bench=. -count=5 ./...
```

### CPU Profiling

```bash
# Generate CPU profile
go test -cpuprofile=cpu.prof -bench=BenchmarkName ./package

# Analyze profile
go tool pprof cpu.prof
# In pprof: top10, list functionName, web
```

### Memory Profiling

```bash
# Generate memory profile
go test -memprofile=mem.prof -bench=BenchmarkName ./package

# Analyze allocations
go tool pprof -alloc_objects mem.prof

# Analyze memory usage
go tool pprof -inuse_objects mem.prof
```

### Example Profiling Session

```bash
# 1. Profile a specific operation
go test -cpuprofile=cpu.prof -bench=BenchmarkLazyEvaluation ./lazy

# 2. Find bottlenecks
go tool pprof cpu.prof
(pprof) top10
(pprof) list optimizer.Optimize

# 3. Profile memory
go test -memprofile=mem.prof -benchmem -bench=BenchmarkLazyEvaluation ./lazy

# 4. Find allocation hotspots
go tool pprof -alloc_objects mem.prof
(pprof) top10
```

## Optimization Techniques

### 1. Minimize Allocations

```go
// Reuse builders
builder := expr.NewBuilder(expr.Col("amount"))
for _, threshold := range thresholds {
    filter := builder.Gt(threshold).Build()
    // Process with filter
    builder = expr.NewBuilder(expr.Col("amount")) // Reset
}
```

### 2. Use Appropriate Data Types

```go
// Use smaller types when possible
// If values fit in int32, don't use int64
golars.NewSeries("count", []int32{1, 2, 3})

// Use categorical for repeated strings
// TODO: When implemented
```

### 3. Parallel Processing

Golars automatically parallelizes some operations:
- Large sort operations
- Some aggregations
- Future: parallel groupby execution

### 4. Cache Computed Results

```go
// If using the same filter multiple times
filtered := df.Filter(complexExpression)

// Reuse filtered DataFrame
result1 := filtered.GroupBy("a").Sum("b")
result2 := filtered.GroupBy("c").Mean("d")
```

## Common Performance Pitfalls

### 1. Repeated Operations

```go
// Avoid: Multiple passes over data
for _, col := range columns {
    mean := df.Column(col).Mean()
    // ...
}

// Better: Single pass with multiple aggregations
stats := df.Agg(map[string]expr.Expr{
    "col1_mean": expr.ColBuilder("col1").Mean().Build(),
    "col2_mean": expr.ColBuilder("col2").Mean().Build(),
    // ...
})
```

### 2. String Operations

String comparisons are slower than numeric:
```go
// If possible, use numeric codes instead of strings
// Or use categorical types when implemented
```

### 3. Unnecessary Type Conversions

```go
// Avoid: Converting between Series types
series := golars.NewSeries("values", []int64{1, 2, 3})
// Don't convert to float64 unless necessary

// Use consistent types across operations
```

## Future Optimizations

Planned performance improvements:
1. **Parallel GroupBy**: Process groups in parallel
2. **SIMD Operations**: Use CPU vector instructions
3. **Memory Pool**: Reuse allocations
4. **Query Planning**: Cost-based optimization
5. **Columnar Compression**: Reduce memory usage
6. **Streaming Execution**: Process data in chunks

## Conclusion

Golars provides excellent performance out of the box. For best results:
- Use lazy evaluation for complex queries
- Let the optimizer push down predicates and projections
- Filter early and select only needed columns
- Profile your specific use case when needed

Remember: premature optimization is the root of all evil. Profile first, optimize second!