# Join Implementation Details

## Architecture Overview

The join implementation consists of two main components:
1. **Hash Table** (`compute/hash.go`) - Efficient lookup structure
2. **Join Logic** (`frame/join.go`) - All join type implementations

## Key Components

### 1. Hash Table Structure
```go
type HashTable struct {
    indices map[uint64][]int      // hash -> row indices
    keys    [][]interface{}       // original key values for collision detection
}
```

### 2. Hash Function
- Uses FNV-1a (Fowler-Noll-Vo) hash algorithm
- Fast, non-cryptographic hash
- Good distribution for typical data

### 3. Join Types Implemented

#### Inner Join
- Returns only rows with matches in both DataFrames
- Most common join type
- Uses hash join algorithm

#### Left Join
- Returns all rows from left DataFrame
- Matched rows from right DataFrame
- Nulls for non-matching right rows

#### Right Join
- Implemented as swapped Left Join
- Returns all rows from right DataFrame

#### Outer Join
- Combination of Left Join + unmatched right rows
- Returns all rows from both DataFrames
- Nulls for non-matching sides

#### Cross Join
- Cartesian product of both DataFrames
- No join keys required
- Result size = left.Height() × right.Height()

#### Anti Join
- Returns left rows WITHOUT matches in right
- Useful for finding missing data
- Only returns left columns

#### Semi Join
- Returns left rows WITH matches in right
- Like inner join but only left columns
- Deduplicates results

## Implementation Strategy

### Hash Join Algorithm
1. **Build Phase**
   - Choose smaller DataFrame (minimize memory)
   - Build hash table on join keys
   - Store row indices for each hash

2. **Probe Phase**
   - Iterate through larger DataFrame
   - Look up matching rows in hash table
   - Handle hash collisions with key comparison

3. **Result Building**
   - Take matched rows from both sides
   - Handle nulls for outer joins
   - Resolve column name conflicts

### Performance Optimizations

1. **Smaller Side Hashing**
```go
if left.Height() <= right.Height() {
    // Build hash table on left
} else {
    // Build hash table on right
}
```

2. **Efficient Type Handling**
- Type-specific hash functions
- Binary encoding for numeric types
- Direct byte hashing for strings

3. **Memory Management**
- Pre-allocation where possible
- Reuse of indices arrays
- Minimal copying of data

## Special Cases Handled

### 1. Null Join Keys
- Nulls never match (even null != null)
- Consistent with SQL semantics

### 2. Duplicate Keys
- Cartesian product of duplicates
- Preserves all matches

### 3. Column Name Conflicts
```go
// Add suffix to conflicting right columns
if left.HasColumn(colName) {
    colName = colName + config.Suffix
}
```

### 4. Empty DataFrames
- Returns empty result for inner/cross/semi
- Returns left/right for left/right joins

## Type Safety

### Hash Value Handling
```go
func hashValue(h hash.Hash64, val interface{}) {
    switch v := val.(type) {
    case int32:
        binary.Write(h, binary.LittleEndian, v)
    case string:
        h.Write([]byte(v))
    case nil:
        h.Write([]byte("__null__"))
    // ... other types
    }
}
```

### Value Equality
- Type-safe comparisons
- No cross-type matches
- Explicit nil handling

## Null Handling in Results

### Take with Nulls
```go
func takeSeriesWithNulls(s series.Series, indices []int) (series.Series, error) {
    // -1 index means null
    for i, idx := range indices {
        if idx >= 0 {
            values[i] = s.Get(idx)
            validity[i] = s.IsValid(idx)
        } else {
            values[i] = getZeroValue(s.DataType())
            validity[i] = false
        }
    }
}
```

### Null Series Creation
- Type-specific null series
- Proper validity masks
- Zero values for null positions

## Thread Safety

- All operations are read-only on source DataFrames
- Mutex protection on DataFrame access
- No shared mutable state

## Testing Approach

### Unit Tests
- Each join type tested separately
- Multi-column joins
- Different column names
- Name conflict resolution

### Edge Cases
- Empty DataFrames
- All nulls
- No matches
- Duplicate keys

### Benchmarks
```
BenchmarkInnerJoin-8         200      5.2 ms/op
BenchmarkMultiColumnJoin-8   100      8.1 ms/op
```

## Memory Complexity

- **Hash Table**: O(n) for smaller DataFrame
- **Result Arrays**: O(m) where m = number of matches
- **Working Memory**: Minimal, mostly indices

## Time Complexity

- **Build Phase**: O(n) for hash table construction
- **Probe Phase**: O(m) with good hash distribution
- **Result Building**: O(m) for matched rows
- **Overall**: O(n + m) expected case

## API Design Philosophy

### 1. Simple Common Cases
```go
df.Join(other, "id", InnerJoin)
```

### 2. Flexible Advanced Cases
```go
df.JoinOn(other, leftCols, rightCols, joinType)
```

### 3. Full Control When Needed
```go
df.JoinWithConfig(other, JoinConfig{...})
```

## Integration Points

### With GroupBy
```go
// Group then join
grouped := df.GroupBy("category").Sum("value")
result := grouped.Join(categories, "category", LeftJoin)
```

### With Sorting
```go
// Join then sort
joined := df1.Join(df2, "id", InnerJoin)
sorted := joined.Sort("timestamp")
```

### With Filtering
```go
// Filter then join (more efficient)
filtered := df.Filter(expr.Col("active").Eq(true))
result := filtered.Join(other, "id", InnerJoin)
```

## Known Limitations

1. **Memory Usage**
   - Full materialization of join results
   - No streaming joins yet

2. **Join Key Types**
   - Must have identical types
   - No automatic type coercion

3. **Result Ordering**
   - Not guaranteed (hash table iteration)
   - Use Sort() if order matters

## Future Enhancements

1. **Sort-Merge Join**
   - For pre-sorted data
   - Lower memory usage

2. **Parallel Hash Join**
   - Partition data for parallel processing
   - Better performance on large datasets

3. **Broadcast Join**
   - For small dimension tables
   - Optimize memory usage

4. **Streaming Joins**
   - Process data in chunks
   - Handle larger-than-memory datasets