# Key Code Examples and Patterns

## DataFrame Creation Patterns

### From Slices
```go
// Basic creation
df, err := golars.NewDataFrame(
    golars.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
    golars.NewStringSeries("name", []string{"Alice", "Bob", "Carol", "Dave", "Eve"}),
    golars.NewFloat64Series("score", []float64{95.5, 87.0, 92.3, 78.5, 88.9}),
)

// With null values
values := []float64{1.1, 2.2, 3.3, 4.4}
validity := []bool{true, false, true, false}  // false = null
series := golars.NewSeriesWithValidity("data", values, validity, golars.Float64)

// From map
df, err := golars.NewDataFrameFromMap(map[string]interface{}{
    "id":     []int32{1, 2, 3},
    "name":   []string{"A", "B", "C"},
    "active": []bool{true, false, true},
})
```

## Expression Building Patterns

### Basic Expressions
```go
// Column reference
age := golars.Col("age")

// Literal value - IMPORTANT: use Lit() for string values!
threshold := golars.Lit(25)
city := golars.Lit("NYC")  // Don't use just "NYC" - it's treated as column name

// Arithmetic
doubled := golars.ColBuilder("value").Mul(2).Build()
taxed := golars.ColBuilder("price").Mul(1.1).Build()
```

### Complex Filters
```go
// Simple comparison
adults := df.Filter(
    golars.ColBuilder("age").Ge(18).Build(),
)

// Compound conditions
filtered := df.Filter(
    golars.ColBuilder("age").Gt(25).And(
        golars.ColBuilder("city").Eq(golars.Lit("NYC")),
    ).Build(),
)

// Multiple OR conditions
statuses := df.Filter(
    golars.ColBuilder("status").Eq(golars.Lit("active")).Or(
        golars.ColBuilder("status").Eq(golars.Lit("pending")),
    ).Or(
        golars.ColBuilder("status").Eq(golars.Lit("approved")),
    ).Build(),
)

// Null handling
nonNull := df.Filter(
    golars.ColBuilder("email").IsNotNull().Build(),
)
```

### Builder Pattern Chain
```go
// Complex expression building
expr := golars.ColBuilder("revenue").
    Sub(golars.Col("costs")).           // revenue - costs
    Div(golars.Col("revenue")).         // (revenue - costs) / revenue
    Mul(100).                           // * 100
    Alias("profit_margin_pct").         // rename
    Build()
```

## Type-Safe Operations

### Working with Specific Types
```go
// Type-specific series creation
intSeries := golars.NewInt32Series("counts", []int32{1, 2, 3, 4, 5})
floatSeries := golars.NewFloat64Series("prices", []float64{9.99, 19.99, 29.99})
stringSeries := golars.NewStringSeries("names", []string{"foo", "bar", "baz"})
boolSeries := golars.NewBooleanSeries("flags", []bool{true, false, true})

// Access with type assertion if needed
if val, ok := series.Get(0).(int32); ok {
    fmt.Printf("Value: %d\n", val)
}
```

## Common Operations

### DataFrame Transformations
```go
// Select columns
subset := df.Select("name", "age", "score")

// Drop columns
reduced := df.Drop("temporary_column", "debug_info")

// Add calculated column
withBonus := df.AddColumn(
    golars.NewFloat64Series("bonus", bonusValues),
)

// Rename column
renamed := df.RenameColumn("old_name", "new_name")

// Slice rows
top10 := df.Head(10)
bottom10 := df.Tail(10)
middle := df.Slice(100, 200)  // rows 100-199
```

### Aggregations (when implemented)
```go
// Sum
total := golars.ColBuilder("sales").Sum().Build()

// Mean
average := golars.ColBuilder("temperature").Mean().Build()

// Multiple aggregations
stats := df.Select(
    golars.ColBuilder("price").Min().Alias("min_price"),
    golars.ColBuilder("price").Max().Alias("max_price"),
    golars.ColBuilder("price").Mean().Alias("avg_price"),
    golars.ColBuilder("price").Std().Alias("std_price"),
)
```

## Error Handling Patterns

### Defensive Programming
```go
// Check column exists before use
col, err := df.Column("maybe_exists")
if err != nil {
    // Handle missing column
    return fmt.Errorf("required column not found: %w", err)
}

// Validate DataFrame not empty
if df.Height() == 0 {
    return errors.New("cannot process empty dataframe")
}

// Check series compatibility
if series1.Len() != series2.Len() {
    return errors.New("series must have same length")
}
```

### Chain Error Handling
```go
// Chain operations with error checking
result, err := df.Filter(expr)
if err != nil {
    return nil, fmt.Errorf("filter failed: %w", err)
}

result, err = result.Select("col1", "col2")
if err != nil {
    return nil, fmt.Errorf("select failed: %w", err)
}

result, err = result.Sort("col1")
if err != nil {
    return nil, fmt.Errorf("sort failed: %w", err)
}
```

## Performance Patterns

### Efficient Filtering
```go
// Filter early to reduce data size
processed := df.
    Filter(golars.ColBuilder("active").Eq(true).Build()).    // First: reduce rows
    Select("id", "name", "value").                            // Then: reduce columns
    Sort("value")                                             // Finally: expensive operations
```

### Batch Operations
```go
// Process in chunks for large data
const chunkSize = 10000
for i := 0; i < df.Height(); i += chunkSize {
    end := min(i+chunkSize, df.Height())
    chunk := df.Slice(i, end)
    // Process chunk
}
```

### Reuse Expressions
```go
// Define once, use multiple times
isActive := golars.ColBuilder("status").Eq(golars.Lit("active")).Build()

df1Filtered := df1.Filter(isActive)
df2Filtered := df2.Filter(isActive)
```

## Testing Patterns

### Table-Driven Tests
```go
tests := []struct {
    name     string
    input    []int32
    filter   expr.Expr
    expected []int32
}{
    {
        name:     "greater than 5",
        input:    []int32{1, 5, 10, 15},
        filter:   golars.ColBuilder("value").Gt(5).Build(),
        expected: []int32{10, 15},
    },
    {
        name:     "equals 5",
        input:    []int32{1, 5, 10, 15},
        filter:   golars.ColBuilder("value").Eq(5).Build(),
        expected: []int32{5},
    },
}

for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        df, _ := golars.NewDataFrame(
            golars.NewInt32Series("value", tt.input),
        )
        result, _ := df.Filter(tt.filter)
        // Assert expected
    })
}
```

### Benchmark Pattern
```go
func BenchmarkFilter(b *testing.B) {
    // Setup
    size := 100000
    values := make([]float64, size)
    for i := range values {
        values[i] = rand.Float64() * 100
    }
    
    df, _ := golars.NewDataFrame(
        golars.NewFloat64Series("value", values),
    )
    
    expr := golars.ColBuilder("value").Gt(50).Build()
    
    // Benchmark
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = df.Filter(expr)
    }
}
```

## Advanced Patterns

### Custom Column Operations
```go
// Apply custom function to column (when implemented)
transformed := df.Apply("temperature", func(val interface{}) interface{} {
    if temp, ok := val.(float64); ok {
        return temp * 9/5 + 32  // Celsius to Fahrenheit
    }
    return nil
})
```

### Conditional Operations
```go
// When-Then-Otherwise pattern (when implemented)
category := golars.When(
    golars.ColBuilder("score").Ge(90),
).Then(
    golars.Lit("A"),
).When(
    golars.ColBuilder("score").Ge(80),
).Then(
    golars.Lit("B"),
).Otherwise(
    golars.Lit("C"),
).Build()
```

### Memory-Efficient Operations
```go
// Drop references to allow GC
var df *golars.DataFrame
df = loadLargeDataFrame()

// Process and immediately drop reference
result := df.Filter(expr).Select("needed_columns")
df = nil  // Original can be GC'd

// Use result...
```