# Common Errors and Fixes

## Query Optimization Errors

### Column Not Found After Optimization
**Error**: `failed to evaluate filter expression: column 'X' not found`

**Cause**: Projection pushdown removed a column needed by a filter.

**Example**:
```go
// This used to fail with "column 'store' not found"
df.Filter(expr.ColBuilder("store").Eq(expr.Lit("B")).Build()).
   GroupBy("product").
   Sum("quantity")
```

**Fix**: This bug was fixed in the latest version. Update to the latest Golars.

**Workaround** (if using older version):
```go
lf := golars.LazyFromDataFrame(df).WithOptimizers(
    lazy.NewPredicatePushdown(), // Keep predicate pushdown only
)
```

### Explain() Shows Unoptimized Plan
**Issue**: `ExplainOptimized()` looks the same as `Explain()`

**Cause**: No optimizers registered or optimization had no effect.

**Fix**: Ensure optimizers are enabled (they are by default in latest version):
```go
lf := golars.LazyFromDataFrame(df) // Has optimizers by default
// Or explicitly:
lf = lf.WithOptimizers(
    lazy.NewPredicatePushdown(),
    lazy.NewProjectionPushdown(),
)
```

## Import and Package Errors

### 1. Arrow Allocator Import Error
**Error**: `undefined: arrow.Allocator`

**Fix**: 
```go
// Wrong
import "github.com/apache/arrow/go/v14/arrow"
allocator := arrow.Allocator  // Does not exist

// Correct
import "github.com/apache/arrow/go/v14/arrow/memory"
allocator := memory.DefaultAllocator
```

### 2. Missing Package Imports
**Error**: `undefined: datatypes`

**Fix**: Add the import
```go
import "github.com/davidpalaitis/golars/datatypes"
```

## Type Constraint Errors

### 3. Overlapping Type Constraints
**Error**: `overlapping terms ~[]uint8 and []byte`

**Fix**: Remove redundant constraint since `[]byte` is an alias for `[]uint8`
```go
// Wrong
type ArrayValue interface {
    ~[]uint8 | []byte | // ... other types
}

// Correct
type ArrayValue interface {
    []byte | // ... other types ([]byte covers []uint8)
}
```

## Expression Building Errors

### 4. String Literals in Expressions
**Issue**: Strings default to column names, not literal values

**Wrong**:
```go
// This looks for a column named "NYC", not the string value "NYC"
df.Filter(golars.ColBuilder("city").Eq("NYC").Build())
```

**Correct**:
```go
// Use Lit() for string literals
df.Filter(golars.ColBuilder("city").Eq(golars.Lit("NYC")).Build())
```

### 5. Missing Build() Call
**Error**: Type mismatch - `ExprBuilder` vs `Expr`

**Wrong**:
```go
expr := golars.ColBuilder("age").Gt(25)  // This is ExprBuilder, not Expr
df.Filter(expr)  // Error: expects Expr
```

**Correct**:
```go
expr := golars.ColBuilder("age").Gt(25).Build()  // Now it's Expr
df.Filter(expr)
```

## Data Type Errors

### 6. Mismatched Series Length
**Error**: `series must have same length`

**Prevention**:
```go
// Check lengths before creating DataFrame
if len(series1) != len(series2) {
    return nil, fmt.Errorf("series lengths don't match: %d vs %d", 
        len(series1), len(series2))
}
```

### 7. Type Assertion Failures
**Issue**: Runtime panic when asserting wrong type

**Safe Pattern**:
```go
// Wrong - can panic
value := series.Get(0).(int32)

// Correct - safe with check
if value, ok := series.Get(0).(int32); ok {
    // Use value
} else {
    // Handle wrong type
}
```

## Null Handling Errors

### 8. Null Comparison Confusion
**Issue**: Nulls in comparisons always return false (not null)

```go
// This will NOT include rows where value is null
df.Filter(golars.ColBuilder("value").Gt(0).Build())

// To include nulls, use explicit check
df.Filter(
    golars.ColBuilder("value").Gt(0).Or(
        golars.ColBuilder("value").IsNull(),
    ).Build(),
)
```

### 9. Null Propagation in Arithmetic
**Issue**: Any operation with null returns null

```go
// If any value in "price" is null, result will be null
golars.ColBuilder("price").Mul(1.1).Build()

// Filter nulls first if needed
df.Filter(golars.ColBuilder("price").IsNotNull().Build()).
   // Then do arithmetic
```

## Memory and Performance Issues

### 10. Memory Leaks with Arrow Arrays
**Issue**: Not releasing Arrow memory

**Fix**:
```go
// Always release Arrow arrays when done
defer array.Release()

// For ChunkedArrays
for _, chunk := range chunks {
    defer chunk.Release()
}
```

### 11. Inefficient Column Access
**Wrong**: Accessing column multiple times
```go
for i := 0; i < df.Height(); i++ {
    col, _ := df.Column("value")  // Gets column each time
    val := col.Get(i)
}
```

**Correct**: Get column once
```go
col, _ := df.Column("value")
for i := 0; i < col.Len(); i++ {
    val := col.Get(i)
}
```

## Common Runtime Errors

### 12. Index Out of Bounds
**Error**: `index out of range`

**Prevention**:
```go
// Always check bounds
if idx < 0 || idx >= series.Len() {
    return nil, fmt.Errorf("index %d out of bounds [0, %d)", idx, series.Len())
}
```

### 13. Column Not Found
**Error**: `column not found`

**Handle gracefully**:
```go
col, err := df.Column("maybe_exists")
if err != nil {
    // Column doesn't exist - handle appropriately
    // Don't assume it exists
}
```

### 14. Empty DataFrame Operations
**Issue**: Some operations fail on empty DataFrames

**Check first**:
```go
if df.Height() == 0 {
    return nil, errors.New("cannot operate on empty DataFrame")
}
```

## Build and Test Errors

### 15. Test Data Type Mismatches
**Issue**: Test expects specific type but gets interface{}

**Fix**:
```go
// Be explicit about types in tests
expected := []int32{1, 2, 3}  // Not []int{1, 2, 3}
series := golars.NewInt32Series("test", expected)
```

### 16. Concurrent Access Issues
**Issue**: Race conditions when accessing DataFrame concurrently

**Fix**: Always use mutex protection
```go
df.mu.RLock()
defer df.mu.RUnlock()
// Read operations

df.mu.Lock()
defer df.mu.Unlock()
// Write operations
```

## Debugging Tips

### Print DataFrame Structure
```go
fmt.Println("Shape:", df.Height(), "x", df.Width())
fmt.Println("Columns:", df.ColumnNames())
fmt.Println("Schema:", df.Schema())
```

### Check Expression Type
```go
fmt.Printf("Expression type: %T\n", expr)
fmt.Printf("Expression string: %s\n", expr.String())
```

### Validate Data Before Operations
```go
// Check for nulls
nullCount := series.NullCount()
if nullCount > 0 {
    log.Printf("Warning: series contains %d nulls", nullCount)
}

// Check data type
if series.DataType() != golars.Float64 {
    return errors.New("operation requires Float64 series")
}
```