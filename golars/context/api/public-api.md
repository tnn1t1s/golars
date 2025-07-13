# Golars Public API Reference

## DataFrame Creation

```go
// From Series
df, err := golars.NewDataFrame(
    golars.NewInt32Series("id", []int32{1, 2, 3}),
    golars.NewStringSeries("name", []string{"A", "B", "C"}),
)

// From map
df, err := golars.NewDataFrameFromMap(map[string]interface{}{
    "id":   []int32{1, 2, 3},
    "name": []string{"A", "B", "C"},
    "age":  []float64{25.5, 30.0, 35.5},
})
```

## Series Creation

```go
// Basic series
s1 := golars.NewInt32Series("numbers", []int32{1, 2, 3, 4, 5})
s2 := golars.NewStringSeries("names", []string{"a", "b", "c"})

// With null values
values := []float64{1.1, 2.2, 3.3}
validity := []bool{true, false, true}  // false = null
s3 := golars.NewSeriesWithValidity("scores", values, validity, golars.Float64)
```

## DataFrame Operations

### Selection and Projection

```go
// Select columns
selected, err := df.Select("name", "age")

// Drop columns
dropped, err := df.Drop("temp_column")

// Get single column
ageSeries, err := df.Column("age")

// Get row
row, err := df.GetRow(0)  // returns map[string]interface{}
```

### Slicing

```go
// First/last N rows
head := df.Head(5)
tail := df.Tail(5)

// Slice range
sliced, err := df.Slice(10, 20)  // rows 10-19
```

### Filtering

```go
// Simple filter
filtered, err := df.Filter(
    golars.ColBuilder("age").Gt(25).Build(),
)

// Complex filter
filtered, err := df.Filter(
    golars.ColBuilder("age").Gt(25).And(
        golars.ColBuilder("city").Eq(golars.Lit("NYC")),
    ).Or(
        golars.ColBuilder("vip").Eq(true),
    ).Build(),
)

// Null checks
withData, err := df.Filter(
    golars.ColBuilder("score").IsNotNull().Build(),
)
```

### Column Operations

```go
// Add column
newDf, err := df.AddColumn(golars.NewFloat64Series("score", scores))

// Rename column
renamed, err := df.RenameColumn("old_name", "new_name")
```

## Expression API

### Basic Expressions

```go
// Column reference
col := golars.Col("age")

// Literal value
lit := golars.Lit(42)

// With builder
expr := golars.ColBuilder("price").Mul(1.1).Add(5).Build()
```

### Comparison Expressions

```go
// All comparison operators
gt := golars.ColBuilder("age").Gt(25)        // >
ge := golars.ColBuilder("age").Ge(25)        // >=
lt := golars.ColBuilder("age").Lt(65)        // <
le := golars.ColBuilder("age").Le(65)        // <=
eq := golars.ColBuilder("status").Eq("active") // ==
ne := golars.ColBuilder("status").Ne("deleted") // !=
```

### Logical Expressions

```go
// AND
expr := golars.ColBuilder("age").Gt(18).And(
    golars.ColBuilder("age").Lt(65),
)

// OR
expr := golars.ColBuilder("status").Eq("active").Or(
    golars.ColBuilder("status").Eq("pending"),
)

// NOT
expr := golars.ColBuilder("blacklisted").Eq(true).Not()
```

### Arithmetic Expressions

```go
// Basic arithmetic
add := golars.ColBuilder("price").Add(10)
sub := golars.ColBuilder("price").Sub(discount)
mul := golars.ColBuilder("quantity").Mul(price)
div := golars.ColBuilder("total").Div(count)
mod := golars.ColBuilder("value").Mod(10)
```

### Aggregation Expressions

```go
// Available aggregations
sum := golars.ColBuilder("sales").Sum()
mean := golars.ColBuilder("scores").Mean()
min := golars.ColBuilder("prices").Min()
max := golars.ColBuilder("prices").Max()
count := golars.ColBuilder("id").Count()
std := golars.ColBuilder("values").Std()
var := golars.ColBuilder("values").Var()
```

### Conditional Expressions

```go
// When-Then-Otherwise
expr := golars.When(
    golars.ColBuilder("score").Ge(90),
).Then(
    golars.Lit("A"),
).Otherwise(
    golars.Lit("B"),
).Build()
```

## Series Operations

```go
// Access
value := series.Get(0)          // returns interface{}
str := series.GetAsString(0)    // returns string representation
isNull := series.IsNull(0)      // check if null
length := series.Len()          // number of elements
nulls := series.NullCount()     // number of nulls

// Slicing
sliced, err := series.Slice(5, 15)
head := series.Head(10)
tail := series.Tail(10)

// Metadata
name := series.Name()
dtype := series.DataType()
renamed := series.Rename("new_name")

// Conversion
slice := series.ToSlice()  // returns underlying Go slice
```

## Data Types

```go
// Available types
golars.Boolean
golars.Int8, golars.Int16, golars.Int32, golars.Int64
golars.UInt8, golars.UInt16, golars.UInt32, golars.UInt64
golars.Float32, golars.Float64
golars.String
golars.Binary
golars.Date
golars.Time
golars.Null
```

## Error Handling

All operations that can fail return an error:

```go
df, err := golars.NewDataFrame(series1, series2)
if err != nil {
    // Handle error - e.g., mismatched lengths
}

filtered, err := df.Filter(expr)
if err != nil {
    // Handle error - e.g., invalid column name
}
```

## Lazy Evaluation

### Creating LazyFrames

```go
// From DataFrame
lf := golars.LazyFromDataFrame(df)

// From CSV (lazy reading)
lf := golars.ScanCSV("data.csv")
```

### Lazy Operations

All DataFrame operations are available in lazy mode:

```go
// Building a query plan
result := lf.
    Filter(expr.ColBuilder("age").Gt(25).Build()).
    SelectColumns("name", "age", "salary").
    Sort("salary", true).
    Limit(10)

// Nothing is executed until Collect() is called
df, err := result.Collect()
```

### Query Planning

```go
// Inspect unoptimized plan
plan := lf.Explain()
fmt.Println(plan)

// Inspect optimized plan
optimized, err := lf.ExplainOptimized()
fmt.Println(optimized)
```

### Lazy-Specific Methods

```go
// Clone a LazyFrame
cloned := lf.Clone()

// Get expected schema without execution
schema, err := lf.Schema()

// Custom optimizers
lf = lf.WithOptimizers(
    lazy.NewPredicatePushdown(),
    lazy.NewProjectionPushdown(),
)
```

## Query Optimization

### Built-in Optimizers

Golars includes several query optimizers that automatically improve performance:

#### Predicate Pushdown
Pushes filters closer to the data source:
```go
// Before optimization:
// Project -> Filter -> Scan

// After optimization:
// Project -> Scan with filter

lf := golars.LazyFromDataFrame(df).
    SelectColumns("name", "age").
    Filter(expr.ColBuilder("age").Gt(25).Build())
```

#### Projection Pushdown
Reads only required columns:
```go
// Only columns "product" and "quantity" are read from source
lf := golars.LazyFromDataFrame(df).
    GroupBy("product").
    Sum("quantity").
    SelectColumns("product", "quantity_sum")
```

#### Expression Optimization
Combines multiple filters:
```go
// Multiple filters are combined with AND
lf := golars.LazyFromDataFrame(df).
    Filter(expr.ColBuilder("age").Gt(25).Build()).
    Filter(expr.ColBuilder("active").Eq(true).Build())
// Optimized to: (age > 25) AND (active == true)
```

### Custom Optimizers

Implement the Optimizer interface to create custom optimizations:

```go
type Optimizer interface {
    Optimize(plan LogicalPlan) (LogicalPlan, error)
}

// Example: Custom optimizer
type MyOptimizer struct{}

func (opt *MyOptimizer) Optimize(plan LogicalPlan) (LogicalPlan, error) {
    // Custom optimization logic
    return plan, nil
}

// Use custom optimizer
lf = lf.WithOptimizers(
    &MyOptimizer{},
    lazy.NewPredicatePushdown(),
)
```

## GroupBy Operations

### Basic GroupBy

```go
// Single column groupby
grouped, err := df.GroupBy("category").Sum("amount")

// Multiple columns
grouped, err := df.GroupBy("year", "month").Mean("temperature")
```

### Aggregation Methods

```go
// Available methods on GroupBy
grouped.Sum("column")      // Sum values
grouped.Mean("column")     // Average values
grouped.Min("column")      // Minimum value
grouped.Max("column")      // Maximum value
grouped.Count()            // Count rows per group
```

### Custom Aggregations

```go
// Multiple aggregations at once
result, err := df.GroupBy("store").Agg(map[string]golars.Expr{
    "total_sales": golars.ColBuilder("sales").Sum().Build(),
    "avg_price": golars.ColBuilder("price").Mean().Build(),
    "item_count": golars.ColBuilder("").Count().Build(),
})
```

## Join Operations

### Basic Joins

```go
// Inner join
joined := df1.Join(df2, "id", golars.InnerJoin)

// Left join
joined := df1.Join(df2, "id", golars.LeftJoin)

// Available join types
golars.InnerJoin
golars.LeftJoin
golars.RightJoin
golars.OuterJoin
golars.CrossJoin
golars.AntiJoin
golars.SemiJoin
```

### Multi-Column Joins

```go
// Join on multiple columns
joined := df1.JoinOn(df2,
    []string{"year", "month"},     // left columns
    []string{"year", "month"},     // right columns
    golars.InnerJoin,
)
```

### Custom Join Configuration

```go
// Join with different column names and suffix
joined := df1.JoinWithConfig(df2, golars.JoinConfig{
    How:     golars.LeftJoin,
    LeftOn:  []string{"customer_id"},
    RightOn: []string{"id"},
    Suffix:  "_right",
})
```

## Sorting

### Basic Sorting

```go
// Single column ascending
sorted := df.Sort("column")

// Single column descending
sorted := df.SortDesc("column")
```

### Multi-Column Sorting

```go
sorted := df.SortBy(golars.SortOptions{
    Columns:    []string{"dept", "salary"},
    Orders:     []golars.SortOrder{golars.Ascending, golars.Descending},
    NullsFirst: false,
    Stable:     true,
})
```

### Series Sorting

```go
// Sort a series
sorted := series.Sort(true)  // true = ascending

// Get sort indices without sorting
indices := series.ArgSort(golars.SortConfig{
    Ascending:  true,
    NullsFirst: false,
})

// Reorder by indices
reordered := series.Take(indices)
```

## I/O Operations

### CSV Reading

```go
// Basic CSV reading
df, err := golars.ReadCSV("data.csv")

// With options
df, err := golars.ReadCSV("data.csv",
    golars.WithDelimiter(';'),
    golars.WithHeader(true),
    golars.WithNullValues([]string{"NA", "null", ""}),
    golars.WithColumns([]string{"name", "age", "salary"}),
    golars.WithSkipRows(5),
    golars.WithComment('#'),
)
```

### CSV Writing

```go
// Basic CSV writing
err := golars.WriteCSV(df, "output.csv")

// With options
err := golars.WriteCSV(df, "output.csv",
    golars.WithWriteDelimiter(','),
    golars.WithWriteHeader(true),
    golars.WithNullValue("N/A"),
    golars.WithFloatFormat("%.2f"),
)
```