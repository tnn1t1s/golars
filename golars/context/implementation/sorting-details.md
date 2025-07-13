# Sorting Implementation Details

## Architecture Overview

Sorting is implemented at two levels:
1. **Series Level** - Sort individual columns
2. **DataFrame Level** - Sort by one or more columns

## Key Components

### 1. SortConfig Structure
```go
type SortConfig struct {
    Order      SortOrder    // Ascending or Descending
    NullsFirst bool         // Where to place nulls
    Stable     bool         // Preserve order of equal elements
}
```

### 2. Comparison Strategy
Uses Go's `sort.Slice` with custom comparator functions:

```go
func (s *TypedSeries[T]) makeComparator(config SortConfig) func(i, j int) bool {
    return func(i, j int) bool {
        // 1. Handle nulls
        if iNull && jNull { return false }
        if iNull { return config.NullsFirst }
        if jNull { return !config.NullsFirst }
        
        // 2. Compare values
        cmp := compareValues(iVal, jVal)
        
        // 3. Apply order
        if config.Order == Ascending {
            return cmp < 0
        } else {
            return cmp > 0
        }
    }
}
```

### 3. Type-Specific Comparisons
Each type has optimized comparison logic:

```go
func compareValues[T datatypes.ArrayValue](a, b T) int {
    switch v1 := any(a).(type) {
    case float64:
        v2 := any(b).(float64)
        // Handle NaN
        if math.IsNaN(v1) && math.IsNaN(v2) { return 0 }
        if math.IsNaN(v1) { return 1 }  // NaN sorts last
        if math.IsNaN(v2) { return -1 }
        // Normal comparison
        if v1 < v2 { return -1 }
        if v1 > v2 { return 1 }
        return 0
    // ... other types
    }
}
```

## Multi-Column Sorting

DataFrame sorting supports multiple columns with different orders:

```go
func (df *DataFrame) multiColumnArgSort(sortSeries []series.Series, options SortOptions) []int {
    less := func(i, j int) bool {
        for k, s := range sortSeries {
            cmp := compareSeriesValues(s, indices[i], indices[j], 
                                      options.Orders[k], options.NullsFirst)
            if cmp < 0 { return true }
            if cmp > 0 { return false }
            // Equal, continue to next column
        }
        return false
    }
    sort.SliceStable(indices, less)
}
```

## Key Algorithms

### 1. ArgSort Pattern
Returns indices that would sort the data, without moving data:

```go
func (s *TypedSeries[T]) ArgSort(config SortConfig) []int {
    n := s.Len()
    indices := make([]int, n)
    for i := 0; i < n; i++ {
        indices[i] = i  // Initialize with 0,1,2,...
    }
    
    sort.SliceStable(indices, comparator)
    return indices
}
```

### 2. Take Operation
Efficiently reorders data using indices:

```go
func (s *TypedSeries[T]) Take(indices []int) Series {
    builder := chunked.NewChunkedBuilder[T](s.DataType())
    
    for _, idx := range indices {
        if s.IsValid(idx) {
            val, _ := s.chunkedArray.Get(int64(idx))
            builder.Append(val)
        } else {
            builder.AppendNull()
        }
    }
    
    return &TypedSeries[T]{
        chunkedArray: builder.Finish(),
        name:         s.name,
    }
}
```

### 3. ChunkedBuilder
New component for efficient array construction:

```go
type ChunkedBuilder[T datatypes.ArrayValue] struct {
    builders []array.Builder
    dataType datatypes.DataType
}

// Type-safe append
func (cb *ChunkedBuilder[T]) Append(value T) {
    switch b := builder.(type) {
    case *array.Int32Builder:
        b.Append(any(value).(int32))
    // ... other types
    }
}
```

## Performance Characteristics

### Time Complexity
- Single column sort: O(n log n)
- Multi-column sort: O(n log n * k) where k = number of columns
- ArgSort: Same as sort, but no data movement
- Take: O(n) where n = number of indices

### Memory Usage
- ArgSort: O(n) for indices array
- Take: O(n) for new array
- Sort creates new DataFrame/Series (immutable pattern)

### Benchmarks
```
BenchmarkSeriesSort/Int32_Size_100         15μs
BenchmarkSeriesSort/Int32_Size_1000       200μs
BenchmarkSeriesSort/Int32_Size_10000      3ms
BenchmarkDataFrameSort/SingleColumn       5ms (10k rows)
BenchmarkDataFrameSort/MultiColumn        8ms (10k rows)
```

## Special Cases

### 1. NaN Handling
- NaN values sort to the end by default
- Consistent ordering: NaN > any number
- All NaN values compare equal

### 2. Null Handling
- Configurable: nulls first or last
- Default: nulls last
- Nulls never compare equal to values

### 3. Stable Sorting
- Preserves relative order of equal elements
- Important for multi-column sorts
- Small performance overhead (~10%)

## Thread Safety

- All operations are read-only on source data
- No locks needed for ArgSort (pure computation)
- Take creates new series (no mutation)

## API Design Decisions

### 1. Simple Methods for Common Cases
```go
df.Sort("column")      // Ascending, nulls last, stable
df.SortDesc("column")  // Descending, nulls last, stable
```

### 2. Advanced Options When Needed
```go
df.SortBy(SortOptions{
    Columns:    []string{"dept", "salary"},
    Orders:     []SortOrder{Ascending, Descending},
    NullsFirst: true,
    Stable:     false,  // Faster if order doesn't matter
})
```

### 3. Composable Operations
```go
indices := series.ArgSort(config)     // Get sort order
shuffled := series.Take(randomIndices) // Custom reordering
reversed := series.Take(reverseIndices) // Reverse without sorting
```

## Testing Strategy

### Unit Tests
- Each data type (int, float, string, etc.)
- Null handling
- NaN handling for floats
- Stable sort verification

### Property Tests
- Sort(Sort(x)) = Sort(x) (idempotent)
- Sorted data has correct order
- All original values present

### Edge Cases
- Empty series/dataframe
- All nulls
- All same values
- Already sorted
- Reverse sorted

## Integration with Other Features

### 1. After Filtering
```go
df.Filter(expr).Sort("column")  // Sort filtered results
```

### 2. Before GroupBy
```go
df.Sort("date").GroupBy("category")  // Ordered groups
```

### 3. For Display
```go
df.Head(10).Sort("value")  // Show top 10 sorted
```

## Future Enhancements

1. **Partial Sorting**
   - Top-K elements without full sort
   - Useful for large datasets

2. **External Sorting**
   - Sort data larger than memory
   - Merge sort with disk spilling

3. **Parallel Sorting**
   - Use multiple cores for large sorts
   - Partition and merge approach

4. **Custom Comparators**
   - User-defined comparison functions
   - Locale-aware string sorting