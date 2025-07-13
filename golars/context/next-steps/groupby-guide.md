# GroupBy Implementation Guide

## Overview

GroupBy is a split-apply-combine operation:
1. **Split**: Group rows by key columns
2. **Apply**: Apply aggregations to each group
3. **Combine**: Merge results into new DataFrame

## Architecture

### Core Structure

```go
// golars/group/groupby.go
package group

type GroupBy struct {
    df          *frame.DataFrame
    groupCols   []string
    groups      map[uint64][]int  // group hash -> row indices
    mu          sync.RWMutex
}

type GroupKey struct {
    Values []interface{}
    Hash   uint64
}
```

### Implementation Steps

## 1. Create GroupBy Structure

```go
// In frame/dataframe.go, add method:
func (df *DataFrame) GroupBy(columns ...string) (*group.GroupBy, error) {
    df.mu.RLock()
    defer df.mu.RUnlock()
    
    // Validate columns exist
    for _, col := range columns {
        if _, err := df.Column(col); err != nil {
            return nil, fmt.Errorf("column %s not found", col)
        }
    }
    
    return group.NewGroupBy(df, columns)
}
```

## 2. Implement Grouping Logic

```go
// group/groupby.go
func NewGroupBy(df *frame.DataFrame, columns []string) (*GroupBy, error) {
    gb := &GroupBy{
        df:        df,
        groupCols: columns,
        groups:    make(map[uint64][]int),
    }
    
    // Build groups
    if err := gb.buildGroups(); err != nil {
        return nil, err
    }
    
    return gb, nil
}

func (gb *GroupBy) buildGroups() error {
    // Get group columns
    groupSeries := make([]series.Series, len(gb.groupCols))
    for i, col := range gb.groupCols {
        s, err := gb.df.Column(col)
        if err != nil {
            return err
        }
        groupSeries[i] = s
    }
    
    // Build groups by hashing row values
    for i := 0; i < gb.df.Height(); i++ {
        key := gb.getGroupKey(groupSeries, i)
        gb.groups[key.Hash] = append(gb.groups[key.Hash], i)
    }
    
    return nil
}

func (gb *GroupBy) getGroupKey(groupSeries []series.Series, row int) GroupKey {
    values := make([]interface{}, len(groupSeries))
    h := fnv.New64a()
    
    for i, s := range groupSeries {
        val := s.Get(row)
        values[i] = val
        
        // Hash the value
        switch v := val.(type) {
        case int32:
            binary.Write(h, binary.LittleEndian, v)
        case int64:
            binary.Write(h, binary.LittleEndian, v)
        case float64:
            binary.Write(h, binary.LittleEndian, v)
        case string:
            h.Write([]byte(v))
        case nil:
            h.Write([]byte("__null__"))
        // Add more types as needed
        }
    }
    
    return GroupKey{
        Values: values,
        Hash:   h.Sum64(),
    }
}
```

## 3. Implement Aggregation Methods

```go
// group/aggregation.go
type AggregationResult struct {
    GroupKeys map[uint64][]interface{}
    Results   map[string]series.Series
}

func (gb *GroupBy) Agg(aggregations map[string]expr.Expr) (*frame.DataFrame, error) {
    gb.mu.RLock()
    defer gb.mu.RUnlock()
    
    result := &AggregationResult{
        GroupKeys: make(map[uint64][]interface{}),
        Results:   make(map[string]series.Series),
    }
    
    // For each group
    for hash, indices := range gb.groups {
        // Get group key values
        key := gb.getKeyForHash(hash, indices[0])
        result.GroupKeys[hash] = key.Values
        
        // Apply aggregations
        for colName, aggExpr := range aggregations {
            if err := gb.applyAggregation(result, hash, indices, colName, aggExpr); err != nil {
                return nil, err
            }
        }
    }
    
    // Build result DataFrame
    return gb.buildResultDataFrame(result)
}

// Convenience methods
func (gb *GroupBy) Sum(columns ...string) (*frame.DataFrame, error) {
    aggs := make(map[string]expr.Expr)
    for _, col := range columns {
        aggs[col] = expr.Col(col).Sum()
    }
    return gb.Agg(aggs)
}

func (gb *GroupBy) Mean(columns ...string) (*frame.DataFrame, error) {
    aggs := make(map[string]expr.Expr)
    for _, col := range columns {
        aggs[col] = expr.Col(col).Mean()
    }
    return gb.Agg(aggs)
}

func (gb *GroupBy) Count() (*frame.DataFrame, error) {
    // Special case: just count rows per group
    // Implementation details...
}
```

## 4. Apply Aggregations to Groups

```go
func (gb *GroupBy) applyAggregation(result *AggregationResult, hash uint64, 
    indices []int, colName string, aggExpr expr.Expr) error {
    
    // Get the column to aggregate
    col, err := gb.df.Column(colName)
    if err != nil {
        return err
    }
    
    // Extract values for this group
    groupValues := make([]interface{}, len(indices))
    for i, idx := range indices {
        groupValues[i] = col.Get(idx)
    }
    
    // Apply aggregation based on expression type
    var aggResult interface{}
    switch agg := aggExpr.(type) {
    case *expr.AggregationExpr:
        switch agg.AggType() {
        case expr.AggSum:
            aggResult = computeSum(groupValues, col.DataType())
        case expr.AggMean:
            aggResult = computeMean(groupValues, col.DataType())
        case expr.AggMin:
            aggResult = computeMin(groupValues, col.DataType())
        case expr.AggMax:
            aggResult = computeMax(groupValues, col.DataType())
        case expr.AggCount:
            aggResult = int64(len(indices))
        }
    }
    
    // Store result
    if _, exists := result.Results[colName]; !exists {
        result.Results[colName] = createEmptySeries(colName, col.DataType())
    }
    result.Results[colName].Append(aggResult)
    
    return nil
}
```

## 5. Build Result DataFrame

```go
func (gb *GroupBy) buildResultDataFrame(result *AggregationResult) (*frame.DataFrame, error) {
    columns := make([]series.Series, 0)
    
    // Add group columns
    for i, groupCol := range gb.groupCols {
        values := make([]interface{}, 0, len(result.GroupKeys))
        for _, key := range result.GroupKeys {
            values = append(values, key[i])
        }
        
        // Create series for group column
        s := series.NewSeries(groupCol, values, gb.getColumnType(groupCol))
        columns = append(columns, s)
    }
    
    // Add aggregation result columns
    for colName, s := range result.Results {
        columns = append(columns, s)
    }
    
    return frame.NewDataFrame(columns...)
}
```

## Usage Example

```go
// Basic groupby with single aggregation
df.GroupBy("category").Sum("sales")

// Multiple group columns
df.GroupBy("year", "month").Mean("temperature", "humidity")

// Multiple aggregations
df.GroupBy("department").Agg(map[string]expr.Expr{
    "salary_sum": expr.Col("salary").Sum(),
    "salary_avg": expr.Col("salary").Mean(),
    "count": expr.Col("salary").Count(),
})

// Chained operations
df.Filter(expr.Col("active").Eq(true)).
   GroupBy("department").
   Sum("revenue")
```

## Performance Optimizations

### 1. Parallel Group Processing
```go
// Process groups in parallel for large datasets
var wg sync.WaitGroup
results := make(chan groupResult, len(gb.groups))

for hash, indices := range gb.groups {
    wg.Add(1)
    go func(h uint64, idx []int) {
        defer wg.Done()
        // Process group
        results <- processGroup(h, idx)
    }(hash, indices)
}
```

### 2. Hash Optimization
- Use FNV-1a hash for speed
- Cache hash values if grouping multiple times
- Consider perfect hashing for small key sets

### 3. Memory Efficiency
- Pre-allocate result arrays when group count is known
- Use type-specific aggregation functions
- Avoid interface{} in hot paths

## Testing Strategy

```go
func TestGroupBy(t *testing.T) {
    // Test single column groupby
    df := createTestDataFrame()
    result, err := df.GroupBy("category").Sum("value")
    assert.NoError(t, err)
    assert.Equal(t, expectedCategories, result.Height())
    
    // Test multiple columns
    result, err = df.GroupBy("year", "month").Mean("temperature")
    
    // Test with nulls
    dfWithNulls := createDataFrameWithNulls()
    result, err = dfWithNulls.GroupBy("group").Sum("nullable_col")
    
    // Test empty groups
    // Test single group (all same values)
    // Test no groups (each row unique)
}

func BenchmarkGroupBy(b *testing.B) {
    df := createLargeDataFrame(1000000) // 1M rows
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, _ = df.GroupBy("category").Sum("value")
    }
}
```

## Edge Cases to Handle

1. **Null values in group columns**: Create special null group
2. **Empty DataFrame**: Return empty result
3. **All null aggregation column**: Return null result
4. **Mixed types in aggregation**: Type promotion rules
5. **Large number of groups**: Memory management
6. **Single group**: Optimize to avoid hashing