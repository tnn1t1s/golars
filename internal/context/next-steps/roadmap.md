# Implementation Roadmap

## Completed Features ✅

### Core Infrastructure
- ✅ ChunkedArray with Arrow integration
- ✅ Series with type erasure and null support
- ✅ DataFrame with schema validation
- ✅ Expression system with fluent API
- ✅ Compute kernels (arithmetic, comparison, aggregation)

### Major Features
- ✅ **GroupBy Operations** - Single/multi-column with all aggregations
- ✅ **Sorting** - Single/multi-column with null handling
- ✅ **Join Operations** - All join types with hash implementation
- ✅ **I/O Operations** - CSV read/write with type inference
- ✅ **Lazy Evaluation** - Complete framework with query planning
- ✅ **Query Optimization** - Predicate and projection pushdown
- ✅ **Parquet I/O** - Read/write support with compression
- ✅ **Window Functions** - Complete implementation with ranking, aggregations, and offset functions

## Next Priority Features

Based on user needs and ecosystem requirements:

### 1. Additional I/O Formats (HIGH PRIORITY)
- ✅ **Parquet Support** - Native Arrow format with compression
- **JSON/NDJSON** - Common data interchange format
- **Database Connectors** - PostgreSQL, MySQL, SQLite
- **Excel Support** - For business users

### 2. Window Functions (COMPLETED ✅)
- ✅ **Ranking Functions** - ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, NTILE
- ✅ **Rolling Aggregations** - SUM, AVG, MIN, MAX, COUNT with custom frames
- ✅ **Offset Functions** - LAG, LEAD, FIRST_VALUE, LAST_VALUE
- ✅ **Frame Specifications** - ROWS BETWEEN with various boundaries
- ✅ **Partitioning** - PARTITION BY support for all functions
- ✅ **Comprehensive Testing** - Unit tests for all functions, partitioning, and frames

### 3. String Operations (MEDIUM PRIORITY)
- **String Manipulation** - split, concat, replace, trim
- **Pattern Matching** - contains, startswith, endswith
- **Regular Expressions** - extract, match, replace
- **Case Operations** - upper, lower, title

### 4. DateTime Support (MEDIUM PRIORITY)
- **Parsing** - From various string formats
- **Formatting** - To string representations
- **Arithmetic** - Add/subtract durations
- **Components** - Extract year, month, day, etc.
- **Timezone** - Conversion and handling

### 5. Advanced Query Optimization (LOW PRIORITY)
- **Common Subexpression Elimination** - Detect and reuse computations
- **Join Reordering** - Optimize join order by size
- **Cost-Based Optimization** - Use statistics for planning
- **Constant Folding** - Evaluate constants at compile time

### 6. Performance Enhancements (LOW PRIORITY)
- **SIMD Operations** - Use CPU vector instructions
- **Parallel Execution** - Multi-threaded operations
- **Memory Pool** - Reuse allocations
- **Columnar Compression** - Reduce memory usage

## Implementation Guidelines

### For Each New Feature:
1. **Design First** - Write design doc in context/
2. **Test Driven** - Write tests before implementation
3. **Benchmark** - Add performance benchmarks
4. **Document** - Update API docs and examples
5. **Integrate** - Ensure works with lazy evaluation

### Code Organization:
```go
feature/
├── implementation.go    // Core logic
├── implementation_test.go
├── benchmark_test.go
└── example/            // Usage examples
```

## Testing Strategy

For each new feature:
1. Unit tests for core functionality
2. Integration tests with existing features
3. Performance benchmarks
4. Example usage in cmd/example/

## Performance Considerations

### Memory
- Reuse allocations where possible
- Use Arrow builders efficiently
- Consider memory pooling for large operations

### CPU
- Vectorize operations
- Use parallel processing for independent groups
- Cache hash computations

### Profiling
```bash
# CPU profiling
go test -cpuprofile=cpu.prof -bench=BenchmarkGroupBy
go tool pprof cpu.prof

# Memory profiling
go test -memprofile=mem.prof -bench=BenchmarkGroupBy
go tool pprof mem.prof
```

## Integration Points

### With Existing Code
- GroupBy uses existing aggregation kernels
- Sorting can reuse comparison kernels
- Joins need new hash computation kernels

### Thread Safety
- Maintain RWMutex pattern
- Consider concurrent group processing
- Lock at appropriate granularity

## Error Handling

Consistent with existing patterns:
```go
result, err := operation()
if err != nil {
    return nil, fmt.Errorf("operation failed: %w", err)
}
```

## API Design Principles

1. **Fluent Interface**: Support method chaining
2. **Immutability**: Operations return new objects
3. **Type Safety**: Use generics where appropriate
4. **Null Handling**: Consistent null propagation
5. **Performance**: Optimize common cases

## Documentation Requirements

For each feature:
1. API documentation with examples
2. Performance characteristics
3. Null handling behavior
4. Thread safety guarantees
5. Integration examples