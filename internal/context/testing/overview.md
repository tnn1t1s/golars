# Testing Overview

## Test Coverage

All implemented components have comprehensive test coverage:

| Package | Test Files | Coverage Areas |
|---------|-----------|----------------|
| datatypes | datatype_test.go | Type equality, string representation, properties |
| chunked | chunked_array_test.go | Append, get, slice, null handling |
| series | series_test.go | All series operations, type variants |
| frame | dataframe_test.go, filter_test.go | DataFrame ops, filtering |
| expr | expr_test.go | Expression building, evaluation |
| compute | kernels_test.go | Arithmetic, comparison, aggregation |

## Test Patterns

### 1. Table-Driven Tests

```go
tests := []struct {
    name     string
    input    interface{}
    expected interface{}
}{
    {"case1", input1, expected1},
    {"case2", input2, expected2},
}

for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        result := Operation(tt.input)
        assert.Equal(t, tt.expected, result)
    })
}
```

### 2. Subtest Organization

```go
func TestDataFrameFilter(t *testing.T) {
    t.Run("SimpleComparison", func(t *testing.T) { ... })
    t.Run("CompoundFilter", func(t *testing.T) { ... })
    t.Run("NullHandling", func(t *testing.T) { ... })
}
```

### 3. Benchmark Pattern

```go
func BenchmarkOperation(b *testing.B) {
    // Setup
    data := createLargeDataset()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = Operation(data)
    }
}
```

## Key Test Cases

### Type System
- Type equality comparisons
- String representations
- Type properties (numeric, nested, etc.)
- Schema operations

### ChunkedArray
- Single and multiple chunks
- Null value handling
- Boundary conditions
- Different data types

### Series
- Creation with/without nulls
- Slicing operations
- Head/tail operations
- Type-specific operations

### DataFrame
- Creation validation
- Column operations
- Row operations
- Schema consistency

### Filtering
- Simple comparisons
- Complex boolean logic
- Null handling
- Performance with large data

### Expressions
- Builder pattern
- Expression composition
- Type propagation
- Edge cases

### Compute Kernels
- Type-specific operations
- Null propagation
- Division by zero
- Aggregation edge cases

## Performance Benchmarks

Key benchmarks to monitor:

```
BenchmarkChunkedArrayGet      - Basic array access
BenchmarkSeriesGet            - Series access overhead
BenchmarkDataFrameCreation    - DataFrame construction
BenchmarkDataFrameSelect      - Column selection
BenchmarkDataFrameFilter      - Filter operations
BenchmarkArithmeticKernel     - Compute operations
BenchmarkAggregateKernel      - Aggregation performance
```

## Testing Commands

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package tests
go test ./frame -v

# Run benchmarks
go test -bench=. -benchmem ./...

# Run specific test
go test -run TestDataFrameFilter ./frame
```

## Test Data Patterns

### Small Test Data
```go
df, _ := golars.NewDataFrameFromMap(map[string]interface{}{
    "id":   []int32{1, 2, 3},
    "name": []string{"A", "B", "C"},
})
```

### Large Test Data
```go
size := 100000
ids := make([]int64, size)
values := make([]float64, size)
for i := 0; i < size; i++ {
    ids[i] = int64(i)
    values[i] = rand.Float64() * 100
}
```

### Null Test Data
```go
values := []float64{1.0, 2.0, 3.0, 4.0}
validity := []bool{true, false, true, false}
series := golars.NewSeriesWithValidity("data", values, validity, golars.Float64)
```