# Sorting Implementation Guide

## Overview

Implement efficient sorting for Series and DataFrames:
- Single column sort
- Multi-column sort with different orders
- Custom comparators
- Stable sorting
- Null handling options

## Architecture

### Core Structure

```go
// golars/frame/sort.go
package frame

type SortOrder int

const (
    Ascending SortOrder = iota
    Descending
)

type SortOptions struct {
    Columns    []string
    Orders     []SortOrder
    NullsFirst bool
    Stable     bool
}

// golars/series/sort.go
type SortConfig struct {
    Order      SortOrder
    NullsFirst bool
    Stable     bool
}
```

## Implementation Steps

### 1. Series Sorting

```go
// series/sort.go
func (s *TypedSeries[T]) Sort(ascending bool) series.Series {
    return s.SortWithConfig(SortConfig{
        Order:      ifThenElse(ascending, Ascending, Descending),
        NullsFirst: false,
        Stable:     true,
    })
}

func (s *TypedSeries[T]) SortWithConfig(config SortConfig) series.Series {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    // Get sort indices
    indices := s.ArgSort(config)
    
    // Gather values in sorted order
    return s.Take(indices)
}

func (s *TypedSeries[T]) ArgSort(config SortConfig) []int {
    n := s.Len()
    indices := make([]int, n)
    for i := 0; i < n; i++ {
        indices[i] = i
    }
    
    // Create comparator
    less := s.makeComparator(config)
    
    // Use stable or unstable sort
    if config.Stable {
        sort.SliceStable(indices, func(i, j int) bool {
            return less(indices[i], indices[j])
        })
    } else {
        sort.Slice(indices, func(i, j int) bool {
            return less(indices[i], indices[j])
        })
    }
    
    return indices
}

func (s *TypedSeries[T]) makeComparator(config SortConfig) func(i, j int) bool {
    return func(i, j int) bool {
        // Handle nulls
        iNull := s.IsNull(i)
        jNull := s.IsNull(j)
        
        if iNull && jNull {
            return false // Equal
        }
        if iNull {
            return config.NullsFirst
        }
        if jNull {
            return !config.NullsFirst
        }
        
        // Compare values
        iVal := s.chunkedArray.Get(i)
        jVal := s.chunkedArray.Get(j)
        
        if config.Order == Ascending {
            return compareValues(iVal, jVal) < 0
        } else {
            return compareValues(iVal, jVal) > 0
        }
    }
}
```

### 2. Type-Specific Comparisons

```go
// compute/compare.go
func compareValues[T datatypes.ArrayValue](a, b T) int {
    switch v1 := any(a).(type) {
    case int8:
        v2 := any(b).(int8)
        if v1 < v2 { return -1 }
        if v1 > v2 { return 1 }
        return 0
    case int32:
        v2 := any(b).(int32)
        if v1 < v2 { return -1 }
        if v1 > v2 { return 1 }
        return 0
    case float64:
        v2 := any(b).(float64)
        // Handle NaN
        if math.IsNaN(v1) && math.IsNaN(v2) { return 0 }
        if math.IsNaN(v1) { return 1 }
        if math.IsNaN(v2) { return -1 }
        if v1 < v2 { return -1 }
        if v1 > v2 { return 1 }
        return 0
    case string:
        v2 := any(b).(string)
        return strings.Compare(v1, v2)
    // Add more types
    }
}
```

### 3. DataFrame Sorting

```go
// frame/sort.go
func (df *DataFrame) Sort(columns ...string) (*DataFrame, error) {
    orders := make([]SortOrder, len(columns))
    for i := range orders {
        orders[i] = Ascending
    }
    
    return df.SortBy(SortOptions{
        Columns: columns,
        Orders:  orders,
        Stable:  true,
    })
}

func (df *DataFrame) SortBy(options SortOptions) (*DataFrame, error) {
    df.mu.RLock()
    defer df.mu.RUnlock()
    
    // Validate columns
    sortSeries := make([]series.Series, len(options.Columns))
    for i, col := range options.Columns {
        s, err := df.Column(col)
        if err != nil {
            return nil, fmt.Errorf("column %s not found", col)
        }
        sortSeries[i] = s
    }
    
    // Get sort indices
    indices := df.multiColumnArgSort(sortSeries, options)
    
    // Create new sorted DataFrame
    return df.Take(indices)
}

func (df *DataFrame) multiColumnArgSort(sortSeries []series.Series, 
    options SortOptions) []int {
    
    n := df.Height()
    indices := make([]int, n)
    for i := 0; i < n; i++ {
        indices[i] = i
    }
    
    // Create multi-column comparator
    less := func(i, j int) bool {
        for k, s := range sortSeries {
            order := Ascending
            if k < len(options.Orders) {
                order = options.Orders[k]
            }
            
            cmp := compareSeriesValues(s, indices[i], indices[j], 
                order, options.NullsFirst)
            
            if cmp < 0 {
                return true
            } else if cmp > 0 {
                return false
            }
            // Equal, continue to next column
        }
        return false // All equal
    }
    
    if options.Stable {
        sort.SliceStable(indices, less)
    } else {
        sort.Slice(indices, less)
    }
    
    return indices
}
```

### 4. Take Operation

```go
// frame/dataframe.go
func (df *DataFrame) Take(indices []int) (*DataFrame, error) {
    df.mu.RLock()
    defer df.mu.RUnlock()
    
    // Validate indices
    for _, idx := range indices {
        if idx < 0 || idx >= df.height {
            return nil, fmt.Errorf("index %d out of bounds", idx)
        }
    }
    
    // Create new columns with gathered values
    newColumns := make([]series.Series, len(df.columns))
    for i, col := range df.columns {
        newColumns[i] = col.Take(indices)
    }
    
    return NewDataFrame(newColumns...)
}

// series/series.go
func (s *TypedSeries[T]) Take(indices []int) series.Series {
    values := make([]T, len(indices))
    validity := make([]bool, len(indices))
    
    for i, idx := range indices {
        if s.IsValid(idx) {
            values[i] = s.chunkedArray.Get(idx)
            validity[i] = true
        } else {
            validity[i] = false
        }
    }
    
    return NewSeriesWithValidity(s.name, values, validity, s.DataType())
}
```

### 5. Optimized Sorting Algorithms

```go
// compute/radix_sort.go
// For integer types - O(n) complexity
func RadixSortInt32(values []int32, indices []int) {
    n := len(values)
    if n <= 1 {
        return
    }
    
    // Find min and max
    min, max := values[0], values[0]
    for _, v := range values[1:] {
        if v < min { min = v }
        if v > max { max = v }
    }
    
    // Shift to handle negative numbers
    shift := int32(0)
    if min < 0 {
        shift = -min
    }
    
    // Radix sort implementation
    const radix = 256
    maxVal := uint32(max + shift)
    
    for exp := uint32(1); maxVal/exp > 0; exp *= radix {
        countingSort(values, indices, exp, shift)
    }
}

// compute/quicksort.go
// For better cache locality on small data
func QuickSort[T constraints.Ordered](values []T, indices []int, 
    low, high int) {
    if low < high {
        pi := partition(values, indices, low, high)
        QuickSort(values, indices, low, pi-1)
        QuickSort(values, indices, pi+1, high)
    }
}
```

## Usage Examples

```go
// Sort series
sorted := series.Sort(true)  // Ascending
sorted := series.Sort(false) // Descending

// Sort DataFrame by single column
df.Sort("age")

// Sort by multiple columns
df.SortBy(SortOptions{
    Columns: []string{"department", "salary"},
    Orders:  []SortOrder{Ascending, Descending},
})

// Custom null handling
df.SortBy(SortOptions{
    Columns:    []string{"score"},
    Orders:     []SortOrder{Descending},
    NullsFirst: true,
})

// Get sort indices without sorting
indices := series.ArgSort(SortConfig{Order: Ascending})
```

## Performance Optimizations

### 1. Algorithm Selection

```go
func selectSortAlgorithm(dtype datatypes.DataType, n int) string {
    switch dtype {
    case datatypes.Int8, datatypes.Int16, datatypes.Int32:
        if n > 10000 {
            return "radix"  // O(n) for integers
        }
    case datatypes.Float32, datatypes.Float64:
        if n > 100000 {
            return "parallel_quicksort"
        }
    case datatypes.String:
        if n > 50000 {
            return "parallel_timsort"
        }
    }
    
    if n < 64 {
        return "insertion"  // Best for small data
    }
    
    return "quicksort"  // General purpose
}
```

### 2. Parallel Sorting

```go
func ParallelSort(values []float64, indices []int) {
    n := len(values)
    if n < 10000 {
        // Too small for parallelization
        sort.Slice(indices, func(i, j int) bool {
            return values[indices[i]] < values[indices[j]]
        })
        return
    }
    
    // Parallel quicksort
    ncpu := runtime.NumCPU()
    pSort(values, indices, 0, n-1, ncpu)
}

func pSort(values []float64, indices []int, low, high, threads int) {
    if threads <= 1 || high-low < 1000 {
        quickSort(values, indices, low, high)
        return
    }
    
    pi := partition(values, indices, low, high)
    
    var wg sync.WaitGroup
    wg.Add(2)
    
    go func() {
        defer wg.Done()
        pSort(values, indices, low, pi-1, threads/2)
    }()
    
    go func() {
        defer wg.Done()
        pSort(values, indices, pi+1, high, threads/2)
    }()
    
    wg.Wait()
}
```

### 3. Memory Efficiency

```go
// In-place index sorting to avoid copying large arrays
func (df *DataFrame) SortInPlace(column string) error {
    // Modify existing DataFrame instead of creating new one
    indices := df.getSortIndices(column)
    
    // Reorder all columns in-place
    for _, col := range df.columns {
        reorderInPlace(col, indices)
    }
    
    return nil
}
```

## Testing Strategy

```go
func TestSort(t *testing.T) {
    // Test basic sorting
    s := NewInt32Series("values", []int32{3, 1, 4, 1, 5, 9})
    sorted := s.Sort(true)
    expected := []int32{1, 1, 3, 4, 5, 9}
    
    // Test with nulls
    s = NewSeriesWithValidity("values", 
        []float64{3.0, 1.0, 4.0}, 
        []bool{true, false, true},
        Float64)
    
    // Test stability
    df := NewDataFrameFromMap(map[string]interface{}{
        "key": []string{"a", "b", "a", "b"},
        "val": []int{2, 1, 1, 2},
    })
    sorted := df.Sort("key") // Should preserve order within groups
    
    // Test multi-column sort
    sorted := df.SortBy(SortOptions{
        Columns: []string{"key", "val"},
        Orders:  []SortOrder{Ascending, Descending},
    })
}

func BenchmarkSort(b *testing.B) {
    sizes := []int{100, 1000, 10000, 100000}
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            data := make([]float64, size)
            for i := range data {
                data[i] = rand.Float64()
            }
            s := NewFloat64Series("test", data)
            
            b.ResetTimer()
            for i := 0; i < b.N; i++ {
                _ = s.Sort(true)
            }
        })
    }
}
```

## Edge Cases

1. **All nulls**: Return as-is or sorted based on index
2. **Already sorted**: Detect and return early
3. **Reverse sorted**: Optimize by reversing
4. **Floating point NaN**: Consistent ordering
5. **String collation**: Consider locale
6. **Large identical values**: Maintain stability