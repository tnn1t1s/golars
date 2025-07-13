# Join Operations Implementation Guide

## Overview

Implement various join types:
- Inner Join
- Left Join  
- Right Join
- Full Outer Join
- Cross Join
- Anti Join
- Semi Join

## Architecture

### Core Structure

```go
// golars/frame/join.go
package frame

type JoinType string

const (
    InnerJoin JoinType = "inner"
    LeftJoin  JoinType = "left"
    RightJoin JoinType = "right"
    OuterJoin JoinType = "outer"
    CrossJoin JoinType = "cross"
    AntiJoin  JoinType = "anti"
    SemiJoin  JoinType = "semi"
)

type JoinConfig struct {
    How      JoinType
    LeftOn   []string
    RightOn  []string
    Suffix   string  // Default: "_right"
}
```

## Implementation Steps

### 1. Main Join Method

```go
func (df *DataFrame) Join(other *DataFrame, on string, how JoinType) (*DataFrame, error) {
    return df.JoinOn(other, []string{on}, []string{on}, how)
}

func (df *DataFrame) JoinOn(other *DataFrame, leftOn []string, rightOn []string, how JoinType) (*DataFrame, error) {
    config := JoinConfig{
        How:     how,
        LeftOn:  leftOn,
        RightOn: rightOn,
        Suffix:  "_right",
    }
    return df.JoinWithConfig(other, config)
}

func (df *DataFrame) JoinWithConfig(other *DataFrame, config JoinConfig) (*DataFrame, error) {
    df.mu.RLock()
    other.mu.RLock()
    defer df.mu.RUnlock()
    defer other.mu.RUnlock()
    
    // Validate join columns
    if err := validateJoinColumns(df, other, config); err != nil {
        return nil, err
    }
    
    // Dispatch to specific join implementation
    switch config.How {
    case InnerJoin:
        return innerJoin(df, other, config)
    case LeftJoin:
        return leftJoin(df, other, config)
    case RightJoin:
        return rightJoin(other, df, config) // Swap order
    case OuterJoin:
        return outerJoin(df, other, config)
    case CrossJoin:
        return crossJoin(df, other, config)
    case AntiJoin:
        return antiJoin(df, other, config)
    case SemiJoin:
        return semiJoin(df, other, config)
    default:
        return nil, fmt.Errorf("unknown join type: %s", config.How)
    }
}
```

### 2. Hash Join Implementation

```go
// compute/hash.go
package compute

type HashTable struct {
    indices map[uint64][]int
    keys    [][]interface{}
}

func BuildHashTable(series []series.Series) (*HashTable, error) {
    ht := &HashTable{
        indices: make(map[uint64][]int),
        keys:    make([][]interface{}, 0),
    }
    
    nRows := series[0].Len()
    for i := 0; i < nRows; i++ {
        key := make([]interface{}, len(series))
        h := fnv.New64a()
        
        for j, s := range series {
            val := s.Get(i)
            key[j] = val
            hashValue(h, val)
        }
        
        hash := h.Sum64()
        ht.indices[hash] = append(ht.indices[hash], i)
        ht.keys = append(ht.keys, key)
    }
    
    return ht, nil
}

func (ht *HashTable) Probe(series []series.Series, row int) []int {
    key := make([]interface{}, len(series))
    h := fnv.New64a()
    
    for j, s := range series {
        val := s.Get(row)
        key[j] = val
        hashValue(h, val)
    }
    
    hash := h.Sum64()
    candidates := ht.indices[hash]
    
    // Verify actual equality (handle hash collisions)
    matches := make([]int, 0)
    for _, idx := range candidates {
        if keysEqual(key, ht.keys[idx]) {
            matches = append(matches, idx)
        }
    }
    
    return matches
}
```

### 3. Inner Join Implementation

```go
func innerJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
    // Get join columns
    leftKeys, err := getJoinColumns(left, config.LeftOn)
    if err != nil {
        return nil, err
    }
    
    rightKeys, err := getJoinColumns(right, config.RightOn)
    if err != nil {
        return nil, err
    }
    
    // Build hash table on smaller side
    var ht *compute.HashTable
    var probeSide *DataFrame
    var buildSide *DataFrame
    
    if left.Height() <= right.Height() {
        ht, err = compute.BuildHashTable(leftKeys)
        buildSide = left
        probeSide = right
    } else {
        ht, err = compute.BuildHashTable(rightKeys)
        buildSide = right
        probeSide = left
    }
    
    // Probe and collect matches
    leftIndices := make([]int, 0)
    rightIndices := make([]int, 0)
    
    for i := 0; i < probeSide.Height(); i++ {
        matches := ht.Probe(probeKeys, i)
        for _, match := range matches {
            if buildSide == left {
                leftIndices = append(leftIndices, match)
                rightIndices = append(rightIndices, i)
            } else {
                leftIndices = append(leftIndices, i)
                rightIndices = append(rightIndices, match)
            }
        }
    }
    
    // Build result DataFrame
    return buildJoinResult(left, right, leftIndices, rightIndices, config)
}
```

### 4. Left Join Implementation

```go
func leftJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
    // Similar to inner join but include unmatched left rows
    leftKeys, _ := getJoinColumns(left, config.LeftOn)
    rightKeys, _ := getJoinColumns(right, config.RightOn)
    
    // Build hash table on right side
    ht, err := compute.BuildHashTable(rightKeys)
    if err != nil {
        return nil, err
    }
    
    leftIndices := make([]int, 0)
    rightIndices := make([]int, 0)
    
    // Probe left side
    for i := 0; i < left.Height(); i++ {
        matches := ht.Probe(leftKeys, i)
        
        if len(matches) == 0 {
            // No match - include with null right side
            leftIndices = append(leftIndices, i)
            rightIndices = append(rightIndices, -1) // Sentinel for null
        } else {
            // Include all matches
            for _, match := range matches {
                leftIndices = append(leftIndices, i)
                rightIndices = append(rightIndices, match)
            }
        }
    }
    
    return buildJoinResult(left, right, leftIndices, rightIndices, config)
}
```

### 5. Build Join Result

```go
func buildJoinResult(left, right *DataFrame, leftIndices, rightIndices []int, 
    config JoinConfig) (*DataFrame, error) {
    
    resultColumns := make([]series.Series, 0)
    
    // Add left columns
    for _, col := range left.columns {
        values := make([]interface{}, len(leftIndices))
        validity := make([]bool, len(leftIndices))
        
        for i, idx := range leftIndices {
            if idx >= 0 {
                values[i] = col.Get(idx)
                validity[i] = col.IsValid(idx)
            } else {
                values[i] = nil
                validity[i] = false
            }
        }
        
        newSeries := series.NewSeriesWithValidity(col.Name(), values, validity, col.DataType())
        resultColumns = append(resultColumns, newSeries)
    }
    
    // Add right columns (handle name conflicts)
    rightJoinCols := make(map[string]bool)
    for _, col := range config.RightOn {
        rightJoinCols[col] = true
    }
    
    for _, col := range right.columns {
        // Skip join columns from right (already in left)
        if rightJoinCols[col.Name()] {
            continue
        }
        
        values := make([]interface{}, len(rightIndices))
        validity := make([]bool, len(rightIndices))
        
        for i, idx := range rightIndices {
            if idx >= 0 {
                values[i] = col.Get(idx)
                validity[i] = col.IsValid(idx)
            } else {
                values[i] = nil
                validity[i] = false
            }
        }
        
        // Handle column name conflicts
        colName := col.Name()
        if left.HasColumn(colName) {
            colName = colName + config.Suffix
        }
        
        newSeries := series.NewSeriesWithValidity(colName, values, validity, col.DataType())
        resultColumns = append(resultColumns, newSeries)
    }
    
    return NewDataFrame(resultColumns...)
}
```

## Usage Examples

```go
// Simple join on single column
result, err := df1.Join(df2, "id", InnerJoin)

// Join on multiple columns
result, err := df1.JoinOn(df2, 
    []string{"year", "month"}, 
    []string{"year", "month"}, 
    LeftJoin)

// Join with different column names
result, err := orders.JoinOn(customers,
    []string{"customer_id"},
    []string{"id"},
    InnerJoin)

// Custom suffix for conflicts
config := JoinConfig{
    How:     LeftJoin,
    LeftOn:  []string{"id"},
    RightOn: []string{"id"},
    Suffix:  "_from_right",
}
result, err := df1.JoinWithConfig(df2, config)
```

## Performance Optimizations

### 1. Choose Build Side Wisely
```go
// Always build hash table on smaller side
if left.Height() * len(config.LeftOn) <= right.Height() * len(config.RightOn) {
    // Build on left
} else {
    // Build on right
}
```

### 2. Parallel Hash Table Building
```go
func BuildHashTableParallel(series []series.Series) (*HashTable, error) {
    nRows := series[0].Len()
    nWorkers := runtime.NumCPU()
    chunkSize := (nRows + nWorkers - 1) / nWorkers
    
    // Process chunks in parallel
    var wg sync.WaitGroup
    results := make([]*HashTable, nWorkers)
    
    for i := 0; i < nWorkers; i++ {
        start := i * chunkSize
        end := min((i+1)*chunkSize, nRows)
        
        wg.Add(1)
        go func(workerID, s, e int) {
            defer wg.Done()
            results[workerID] = buildChunk(series, s, e)
        }(i, start, end)
    }
    
    wg.Wait()
    
    // Merge results
    return mergeHashTables(results), nil
}
```

### 3. Memory-Efficient String Joins
```go
// Intern strings to reduce memory usage
type StringInterner struct {
    strings map[string]string
}

func (si *StringInterner) Intern(s string) string {
    if interned, exists := si.strings[s]; exists {
        return interned
    }
    si.strings[s] = s
    return s
}
```

## Testing Strategy

```go
func TestJoins(t *testing.T) {
    // Test inner join
    left := createLeftDataFrame()
    right := createRightDataFrame()
    
    result, err := left.Join(right, "id", InnerJoin)
    assert.NoError(t, err)
    assert.Equal(t, expectedRows, result.Height())
    
    // Test left join with nulls
    result, err = left.Join(right, "id", LeftJoin)
    // Verify null handling
    
    // Test multi-column join
    result, err = left.JoinOn(right, 
        []string{"year", "month"}, 
        []string{"year", "month"}, 
        InnerJoin)
    
    // Test cartesian product (cross join)
    result, err = small1.Join(small2, "", CrossJoin)
    assert.Equal(t, small1.Height()*small2.Height(), result.Height())
}

func BenchmarkHashJoin(b *testing.B) {
    left := createLargeDataFrame(100000)
    right := createLargeDataFrame(50000)
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = left.Join(right, "id", InnerJoin)
    }
}
```

## Edge Cases

1. **Null join keys**: Nulls don't match (even null != null)
2. **Duplicate join keys**: Cartesian product of duplicates
3. **Empty DataFrames**: Return empty result
4. **Type mismatches**: Error on incompatible key types
5. **Column name conflicts**: Apply suffix
6. **Self-joins**: Handle same DataFrame on both sides