# Golars API Patterns and Best Practices

## 1. Builder Pattern for Expressions

Always use `.Build()` to finalize expressions:

```go
// ✅ Correct
expr := golars.ColBuilder("age").Gt(25).Build()

// ❌ Wrong - missing Build()
expr := golars.ColBuilder("age").Gt(25)  // This is ExprBuilder, not Expr
```

## 2. String Literal Handling

Be explicit with string literals in comparisons:

```go
// ✅ Correct - explicit literal
df.Filter(golars.ColBuilder("city").Eq(golars.Lit("NYC")).Build())

// ⚠️  Wrong - "NYC" treated as column name
df.Filter(golars.ColBuilder("city").Eq("NYC").Build())
```

## 3. Null Handling

Always consider nulls in your logic:

```go
// Check for nulls explicitly
nonNull := df.Filter(golars.ColBuilder("value").IsNotNull().Build())

// Nulls in arithmetic return null
// null + 5 = null
// null > 5 = false (in filters)
```

## 4. Memory Management

Series and DataFrames manage memory automatically, but be aware:

```go
// Operations create new instances (immutable pattern)
filtered := df.Filter(expr)  // df is unchanged

// ChunkedArrays use reference counting
// Memory is freed when all references are dropped
```

## 5. Error Handling Pattern

Check errors immediately:

```go
df, err := golars.NewDataFrame(series...)
if err != nil {
    return fmt.Errorf("failed to create dataframe: %w", err)
}
```

## 6. Type-Safe Series Creation

Use the specific constructors for type safety:

```go
// ✅ Type-safe
s := golars.NewInt32Series("age", []int32{25, 30, 35})

// ❌ Less safe (requires type assertion later)
s := golars.NewSeries("age", []interface{}{25, 30, 35}, golars.Int32)
```

## 7. Chaining Pattern

Expression builders support chaining:

```go
expr := golars.ColBuilder("price").
    Mul(1.1).        // Add 10% tax
    Add(5).          // Add shipping
    Gt(100).         // Check if over 100
    Build()
```

## 8. Complex Filters

Build complex filters step by step:

```go
// Readable complex filter
isAdult := golars.ColBuilder("age").Ge(18)
isActive := golars.ColBuilder("status").Eq(golars.Lit("active"))
hasEmail := golars.ColBuilder("email").IsNotNull()

filter := isAdult.And(isActive).And(hasEmail).Build()
```

## 9. Column Name Constants

Define column names as constants:

```go
const (
    ColID     = "id"
    ColName   = "name"
    ColAge    = "age"
    ColActive = "active"
)

df.Select(ColID, ColName, ColAge)
```

## 10. Defensive Programming

Validate before operations:

```go
// Check column exists
if _, err := df.Column("maybe_exists"); err != nil {
    // Handle missing column
}

// Check DataFrame is not empty
if df.Height() == 0 {
    return errors.New("cannot operate on empty dataframe")
}
```

## 11. Performance Tips

```go
// Filter early to reduce data size
df.Filter(expr).Select("needed_columns")

// Use specific types (avoid interface{} where possible)
golars.NewInt32Series() // not NewSeries() with interface{}

// Reuse expressions
filter := golars.ColBuilder("active").Eq(true).Build()
df1.Filter(filter)
df2.Filter(filter)  // Reuse same expression
```

## 12. Testing Pattern

```go
// Create test data easily
df, _ := golars.NewDataFrameFromMap(map[string]interface{}{
    "id":   []int32{1, 2, 3},
    "name": []string{"A", "B", "C"},
})

// Verify results
assert.Equal(t, 3, df.Height())
col, _ := df.Column("id")
assert.Equal(t, int32(1), col.Get(0))
```