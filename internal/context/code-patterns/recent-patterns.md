# Recent Code Patterns and Solutions

## Pattern 1: Interface Pattern for Circular Dependencies

### Problem
Frame package imports Group package, but Group needs DataFrame type.

### Solution
```go
// Define minimal interface in the dependent package
type DataFrameInterface interface {
    Column(name string) (series.Series, error)
    Height() int
}

// Use interface instead of concrete type
func NewGroupBy(df DataFrameInterface, columns []string) (*GroupBy, error)
```

### Applied In
- `group/groupby.go` - DataFrameInterface
- `frame/groupby.go` - GroupByWrapper pattern

## Pattern 2: Type Erasure Boundary Management

### Problem
Generic types can't be stored in interface{} slices directly.

### Solution
```go
// Helper function to convert interface{} to typed slices
func createSeriesFromInterface(name string, values []interface{}, dtype datatypes.DataType) series.Series {
    if dtype.Equals(datatypes.Int32{}) {
        data := make([]int32, len(values))
        for i, v := range values {
            if v != nil {
                data[i] = v.(int32)
            }
        }
        return series.NewInt32Series(name, data)
    }
    // ... handle other types
}
```

### Applied In
- `group/aggregation.go` - Building result Series
- `series/sort.go` - Take operation

## Pattern 3: Builder Pattern for Incremental Construction

### Problem
Need to build arrays incrementally with unknown size.

### Solution
```go
type ChunkedBuilder[T datatypes.ArrayValue] struct {
    builders []array.Builder
    dataType datatypes.DataType
}

func (cb *ChunkedBuilder[T]) Append(value T) { /* ... */ }
func (cb *ChunkedBuilder[T]) AppendNull() { /* ... */ }
func (cb *ChunkedBuilder[T]) Finish() *ChunkedArray[T] { /* ... */ }
```

### Applied In
- `chunked/builder.go` - ChunkedBuilder implementation
- `series/sort.go` - Take operation

## Pattern 4: Configuration Objects

### Problem
Functions with many optional parameters.

### Solution
```go
type SortConfig struct {
    Order      SortOrder
    NullsFirst bool
    Stable     bool
}

// Simple API with defaults
func Sort(ascending bool) Series {
    return SortWithConfig(SortConfig{
        Order:  ifThenElse(ascending, Ascending, Descending),
        NullsFirst: false,
        Stable: true,
    })
}
```

### Applied In
- `series/sort.go` - SortConfig
- `frame/sort.go` - SortOptions

## Pattern 5: Type-Safe Operations with Generics

### Problem
Avoid runtime type assertions in hot paths.

### Solution
```go
// Generic function with compile-time type safety
func compareValues[T datatypes.ArrayValue](a, b T) int {
    switch v1 := any(a).(type) {
    case int32:
        v2 := any(b).(int32)  // Safe - we know the type
        if v1 < v2 { return -1 }
        if v1 > v2 { return 1 }
        return 0
    // ... other types
    }
}
```

### Applied In
- `series/sort.go` - compareValues function
- `chunked/builder.go` - Type-specific append

## Pattern 6: Hash-Based Grouping

### Problem
Efficiently group rows by multiple column values.

### Solution
```go
// Hash multiple values into single key
func hashValues(values []interface{}) uint64 {
    h := fnv.New64a()
    for _, val := range values {
        switch v := val.(type) {
        case int32:
            binary.Write(h, binary.LittleEndian, v)
        case string:
            h.Write([]byte(v))
        case nil:
            h.Write([]byte("__null__"))
        }
    }
    return h.Sum64()
}
```

### Applied In
- `group/groupby.go` - Group key hashing

## Pattern 7: Null-Aware Operations

### Problem
Handle null values consistently across operations.

### Solution
```go
// Aggregation skips nulls
for _, v := range values {
    if v != nil {
        sum += toInt64(v)
        count++
    }
}

// Sorting puts nulls first/last
if iNull && jNull { return false }  // Both null = equal
if iNull { return config.NullsFirst }
if jNull { return !config.NullsFirst }
```

### Applied In
- `group/aggregation.go` - All aggregation functions
- `series/sort.go` - Comparison function

## Pattern 8: Immutable Operations

### Problem
Maintain thread safety and predictable behavior.

### Solution
```go
// Always return new instances
func (df *DataFrame) Sort(columns ...string) (*DataFrame, error) {
    indices := df.multiColumnArgSort(...)
    return df.Take(indices)  // New DataFrame
}

// Never modify existing data
func (s *TypedSeries[T]) Take(indices []int) Series {
    builder := NewChunkedBuilder[T](...)
    // Build new series
    return &TypedSeries[T]{chunkedArray: builder.Finish(), ...}
}
```

### Applied In
- All DataFrame operations
- All Series operations

## Pattern 9: Error Wrapping

### Problem
Provide context for errors without losing original error.

### Solution
```go
col, err := df.Column(targetCol)
if err != nil {
    return fmt.Errorf("column %s not found", targetCol)
}

// Better with wrapping
if err != nil {
    return fmt.Errorf("aggregation failed: %w", err)
}
```

### Applied In
- Throughout error handling

## Pattern 10: Benchmark-Driven Optimization

### Problem
Ensure performance meets requirements.

### Solution
```go
func BenchmarkGroupBy(b *testing.B) {
    // Setup
    df := createLargeDataFrame(10000)
    
    b.ResetTimer()  // Don't measure setup
    
    for i := 0; i < b.N; i++ {
        gb, _ := df.GroupBy("category")
        _, _ = gb.Sum("value")
    }
}
```

### Applied In
- `group/groupby_test.go`
- `series/sort_test.go`
- `frame/sort_test.go`

## Pattern 11: Efficient Join Implementation

### Problem
Need to join large DataFrames efficiently.

### Solution
```go
// Build hash table on smaller side
if left.Height() <= right.Height() {
    ht, err = BuildHashTable(leftKeys)
    probeDF = right
} else {
    ht, err = BuildHashTable(rightKeys)
    probeDF = left
}
```

### Applied In
- `frame/join.go` - All join implementations

## Pattern 12: Null-Safe Operations

### Problem
Handle -1 indices representing nulls in join results.

### Solution
```go
for i, idx := range indices {
    if idx >= 0 {
        values[i] = s.Get(idx)
        validity[i] = s.IsValid(idx)
    } else {
        values[i] = getZeroValue(s.DataType())
        validity[i] = false
    }
}
```

### Applied In
- `frame/join.go` - takeSeriesWithNulls function

## Anti-Patterns Avoided

### 1. Unsafe Type Assertions
```go
// Bad
value := series.Get(0).(int32)  // Can panic

// Good  
if value, ok := series.Get(0).(int32); ok {
    // Use value
}
```

### 2. Modifying Input Parameters
```go
// Bad
func Sort(data []int) {
    sort.Ints(data)  // Modifies input
}

// Good
func Sort(data []int) []int {
    copied := make([]int, len(data))
    copy(copied, data)
    sort.Ints(copied)
    return copied
}
```

### 3. Ignoring Errors
```go
// Bad
df.Column("maybe_exists")  // Might return error

// Good
col, err := df.Column("maybe_exists")
if err != nil {
    return nil, fmt.Errorf("required column not found: %w", err)
}
```