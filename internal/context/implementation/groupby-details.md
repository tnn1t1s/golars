# GroupBy Implementation Details

## Architecture Overview

The GroupBy implementation uses a three-step process:
1. **Split** - Hash rows by group keys
2. **Apply** - Apply aggregations to each group
3. **Combine** - Build result DataFrame

## Key Design Decisions

### 1. Avoiding Circular Imports
**Problem**: `frame` package needs `group`, but `group` needs `frame.DataFrame`

**Solution**: Interface pattern
```go
// group/groupby.go
type DataFrameInterface interface {
    Column(name string) (series.Series, error)
    Height() int
}

// frame/groupby.go
type GroupByWrapper struct {
    gb *group.GroupBy
}
```

### 2. Hash-Based Grouping
**Choice**: FNV-1a hash function
- Fast for small keys
- Good distribution
- Non-cryptographic (perfect for our use)

**Implementation**:
```go
func (gb *GroupBy) hashValue(h hash.Hash64, val interface{}) {
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

### 3. Type-Safe Aggregations
**Problem**: Need to handle different types in aggregations

**Solution**: Type switches and conversion functions
```go
func computeSum(values []interface{}, dtype datatypes.DataType) interface{} {
    if dtype.IsInteger() && dtype.IsSigned() {
        var sum int64
        for _, v := range values {
            if v != nil {
                sum += toInt64(v)
            }
        }
        return convertToType(sum, dtype)
    }
    // ... other type branches
}
```

### 4. Result Building
**Challenge**: Converting interface{} slices to typed Series

**Solution**: `createSeriesFromInterface` helper
```go
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
    // ... other types
}
```

## Memory Layout

```
GroupBy Structure:
┌─────────────────┐
│   DataFrame     │
│  (interface)    │
└────────┬────────┘
         │
┌────────▼────────┐
│  Group Keys     │
│ []string{"A","B"}│
└────────┬────────┘
         │
┌────────▼────────┐
│  Hash Table     │
│ map[uint64][]int│
│                 │
│ 0x123 → [0,2,4] │ (rows with key "A")
│ 0x456 → [1,3,5] │ (rows with key "B")
└─────────────────┘
```

## Performance Optimizations

### 1. Pre-allocation
```go
// Known group count allows pre-allocation
result.Results[colName] = make([]interface{}, 0, len(gb.groups))
```

### 2. Single-Pass Grouping
- Build all groups in one pass through data
- Hash computation is inline

### 3. Null Handling
- Special hash for null values
- Nulls excluded from aggregations (SQL semantics)

## Example Execution Flow

```go
df.GroupBy("category").Sum("value")
```

1. **Create GroupBy**
   - Extract "category" column
   - Hash each row's category value
   - Build map: hash → [row indices]

2. **Apply Sum**
   - For each group (hash → indices):
     - Extract "value" at those indices
     - Compute sum (skip nulls)
     - Store result

3. **Build Result**
   - Create "category" Series from unique keys
   - Create "value_sum" Series from sums
   - Return new DataFrame

## Thread Safety

All operations are protected by RWMutex:
- GroupBy creation: Read lock on DataFrame
- Aggregation: Read lock on GroupBy
- No mutations of original data

## Error Handling Patterns

1. **Column Validation**
```go
for _, col := range columns {
    if _, err := df.Column(col); err != nil {
        return nil, fmt.Errorf("column %s not found", col)
    }
}
```

2. **Type Mismatches**
- Handled gracefully in type switches
- Unsupported types return nil

3. **Empty Groups**
- Return empty DataFrame
- No special handling needed

## Testing Approach

### Unit Tests
- Mock DataFrame implementation for isolation
- Test each aggregation function
- Verify null handling

### Integration Tests  
- Real DataFrame → GroupBy → Aggregations
- Multi-column grouping
- Edge cases (empty, all nulls, single group)

### Benchmarks
```go
BenchmarkGroupBy-8              1000    1.05 ms/op
BenchmarkGroupByMultipleColumns-8  500    2.31 ms/op
```

## Known Limitations

1. **Memory Usage**
   - Full materialization of groups
   - No streaming/chunked processing

2. **Hash Collisions**
   - Theoretical possibility
   - Not handled (assumes good hash distribution)

3. **Result Order**
   - Non-deterministic (Go map iteration)
   - Could add sorting if needed

## Future Enhancements

1. **Parallel Aggregation**
   - Process groups concurrently
   - Useful for expensive aggregations

2. **Custom Aggregations**
   - User-defined functions
   - Plugin architecture

3. **Memory Optimization**
   - Streaming aggregations
   - Spill to disk for large groups