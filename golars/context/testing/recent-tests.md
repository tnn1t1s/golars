# Recent Testing Patterns and Results

## GroupBy Testing

### Test Structure
Located in `group/groupby_test.go`

#### Mock Pattern for Unit Testing
```go
// Mock DataFrame to avoid circular dependencies
type mockDataFrame struct {
    columns map[string]series.Series
    height  int
}

func (df *mockDataFrame) Column(name string) (series.Series, error) {
    col, exists := df.columns[name]
    if !exists {
        return nil, fmt.Errorf("column %s not found", name)
    }
    return col, nil
}
```

#### Test Coverage
- ✅ Single column groupby
- ✅ Multiple column groupby
- ✅ All aggregations (Sum, Mean, Min, Max, Count)
- ✅ Null handling
- ✅ Empty DataFrame
- ✅ Error cases (missing columns)

#### Known Test Issues
Some tests fail due to map iteration order:
```go
// Map iteration is non-deterministic in Go
for hash, indices := range gb.groups {
    // Order varies between runs
}
```

**Solution**: Tests verify values exist rather than checking exact positions.

### Example Test Pattern
```go
func TestGroupBySum(t *testing.T) {
    df := newMockDataFrame(
        series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
        series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
    )
    
    gb, err := NewGroupBy(df, []string{"category"})
    assert.NoError(t, err)
    
    result, err := gb.Sum("value")
    assert.NoError(t, err)
    
    // Build map of results to handle non-deterministic order
    sums := make(map[string]int32)
    catCol, sumCol := getColumns(result, "category", "value_sum")
    
    for i := 0; i < catCol.Len(); i++ {
        cat := catCol.Get(i).(string)
        sum := sumCol.Get(i).(int32)
        sums[cat] = sum
    }
    
    assert.Equal(t, int32(9), sums["A"])  // 1 + 3 + 5
    assert.Equal(t, int32(6), sums["B"])  // 2 + 4
}
```

## Sorting Testing

### Test Structure
- `series/sort_test.go` - Series sorting tests
- `frame/sort_test.go` - DataFrame sorting tests

#### Series Sort Tests
```go
func TestSeriesSort(t *testing.T) {
    t.Run("Int32 Ascending", func(t *testing.T) {
        s := NewInt32Series("values", []int32{3, 1, 4, 1, 5, 9})
        sorted := s.Sort(true)
        
        expected := []int32{1, 1, 3, 4, 5, 9}
        for i, exp := range expected {
            assert.Equal(t, exp, sorted.Get(i))
        }
    })
    
    t.Run("Float64 with NaN", func(t *testing.T) {
        nan := math.NaN()
        s := NewFloat64Series("values", []float64{3.0, nan, 1.0, 4.0})
        sorted := s.Sort(true)
        
        // NaN values should be at the end
        assert.Equal(t, 1.0, sorted.Get(0))
        assert.Equal(t, 3.0, sorted.Get(1))
        assert.Equal(t, 4.0, sorted.Get(2))
        assert.True(t, math.IsNaN(sorted.Get(3).(float64)))
    })
}
```

#### DataFrame Sort Tests
```go
func TestDataFrameSort(t *testing.T) {
    t.Run("Multi-column sort", func(t *testing.T) {
        df := createTestDataFrame()
        
        sorted, err := df.SortBy(SortOptions{
            Columns: []string{"age", "score"},
            Orders:  []SortOrder{Ascending, Descending},
        })
        
        // Verify age is ascending
        ageCol, _ := sorted.Column("age")
        for i := 1; i < sorted.Height(); i++ {
            assert.GreaterOrEqual(t, ageCol.Get(i), ageCol.Get(i-1))
        }
        
        // Within same age, score is descending
        // ... additional verification
    })
}
```

#### Special Cases Tested
1. **Null Handling**
   - Nulls first/last configuration
   - All nulls
   - Mixed nulls and values

2. **NaN Handling**
   - Consistent NaN ordering
   - Mixed NaN and regular floats

3. **Stable Sort**
   - Preserves order of equal elements
   - Important for multi-column sorts

4. **Edge Cases**
   - Empty series/dataframe
   - Single element
   - Already sorted
   - Reverse sorted

## Benchmark Results

### GroupBy Benchmarks
```
BenchmarkGroupBy-8                          1000      1050000 ns/op
BenchmarkGroupByMultipleColumns-8            500      2310000 ns/op
```
- 10,000 rows, 5 groups: ~1ms
- Multi-column grouping adds ~2x overhead

### Sort Benchmarks
```
BenchmarkSeriesSort/Int32_Size_100-8       80000         15000 ns/op
BenchmarkSeriesSort/Int32_Size_1000-8       6000        200000 ns/op
BenchmarkSeriesSort/Int32_Size_10000-8       400       3000000 ns/op

BenchmarkDataFrameSort/SingleColumn-8        200       5000000 ns/op
BenchmarkDataFrameSort/MultiColumn-8         100       8000000 ns/op
```
- Linear-logarithmic scaling as expected
- Multi-column adds ~60% overhead

## Test Data Patterns

### Small Test Data
```go
// Explicit, readable test data
df := newMockDataFrame(
    series.NewStringSeries("category", []string{"A", "B", "A"}),
    series.NewInt32Series("value", []int32{1, 2, 3}),
)
```

### Large Test Data
```go
// Programmatically generated
size := 10000
categories := []string{"A", "B", "C", "D", "E"}
data := make([]string, size)
for i := 0; i < size; i++ {
    data[i] = categories[i%len(categories)]
}
```

### Null Test Data
```go
values := []float64{1.0, 2.0, 3.0, 4.0}
validity := []bool{true, false, true, false}  // false = null
series := NewSeriesWithValidity("data", values, validity, datatypes.Float64{})
```

## Test Helpers

### Column Extraction Helper
```go
func getColumns(result *AggResult, names ...string) []series.Series {
    cols := make([]series.Series, len(names))
    for i, name := range names {
        for _, col := range result.Columns {
            if col.Name() == name {
                cols[i] = col
                break
            }
        }
    }
    return cols
}
```

### Map Builder for Non-Deterministic Results
```go
func buildResultMap(keyCol, valueCol series.Series) map[string]interface{} {
    result := make(map[string]interface{})
    for i := 0; i < keyCol.Len(); i++ {
        key := keyCol.Get(i).(string)
        value := valueCol.Get(i)
        result[key] = value
    }
    return result
}
```

## Test Execution Commands

```bash
# Run all tests
go test ./...

# Run specific package tests with verbose output
go test ./group -v
go test ./series -run TestSort -v

# Run with race detector
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem ./group
go test -bench=Sort ./series

# Coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Join Testing

### Test Structure
Located in `frame/join_test.go` and `compute/hash_test.go`

#### Join Type Tests
```go
func TestInnerJoin(t *testing.T) {
    left, _ := NewDataFrame(
        series.NewInt32Series("id", []int32{1, 2, 3, 4}),
        series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
    )
    
    right, _ := NewDataFrame(
        series.NewInt32Series("id", []int32{2, 3, 4, 5}),
        series.NewStringSeries("city", []string{"NYC", "LA", "Chicago", "Houston"}),
    )
    
    result, err := left.Join(right, "id", InnerJoin)
    // Build map to handle row order variations
}
```

#### Hash Table Tests
```go
func TestBuildHashTable(t *testing.T) {
    s := series.NewInt32Series("id", []int32{1, 2, 3, 2, 1})
    ht, err := BuildHashTable([]series.Series{s})
    
    assert.Equal(t, 3, ht.Size())      // Unique keys
    assert.Equal(t, 5, ht.TotalRows()) // Total rows
}
```

#### Special Cases Tested
1. **Join Types**
   - All 7 join types tested
   - Multi-column joins
   - Different column names
   - Column name conflicts

2. **Edge Cases**
   - Empty DataFrames
   - No matches
   - All matches
   - Duplicate keys

3. **Type Safety**
   - Type mismatch errors
   - Missing column errors

### Join Benchmarks
```
BenchmarkInnerJoin/10k_rows-8         200      5.2 ms/op
BenchmarkMultiColumnJoin/10k_rows-8   100      8.1 ms/op
```

## Test Maintenance Notes

1. **Map Iteration Tests**: Accept non-deterministic order
2. **Float Comparisons**: Use `assert.InDelta` for floating point
3. **Null Handling**: Always test with explicit null cases
4. **Benchmarks**: Reset timer after setup
5. **Mock Interfaces**: Keep minimal, only required methods
6. **Join Results**: Use maps to verify results (order may vary)