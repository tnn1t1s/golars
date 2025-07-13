# Golars Quick Start Guide

Get up and running with Golars in minutes!

## Installation

```bash
go get github.com/davidpalaitis/golars
```

## Basic Concepts

Golars provides three main data structures:

1. **Series**: A typed column of data
2. **DataFrame**: A table with named columns (collection of Series)
3. **LazyFrame**: A lazy-evaluated DataFrame for optimized queries

## Creating Data

### From Slices

```go
package main

import (
    "fmt"
    "log"
    "github.com/davidpalaitis/golars"
)

func main() {
    // Create a DataFrame from slices
    df, err := golars.NewDataFrame(
        golars.NewSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
        golars.NewSeries("age", []int32{25, 30, 35, 28}),
        golars.NewSeries("salary", []float64{50000, 60000, 75000, 55000}),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println(df)
}
```

### From a Map

```go
df, err := golars.NewDataFrameFromMap(map[string]interface{}{
    "product": []string{"Apple", "Banana", "Cherry"},
    "price": []float64{1.20, 0.50, 2.00},
    "quantity": []int32{100, 150, 75},
})
```

### From CSV

```go
df, err := golars.ReadCSV("data.csv",
    golars.WithDelimiter(','),
    golars.WithNullValues([]string{"NA", "null", ""}),
)
if err != nil {
    log.Fatal(err)
}
```

## Basic Operations

### Selecting Columns

```go
// Select specific columns
subset := df.Select("name", "salary")

// Select all except certain columns
subset := df.Drop("age")
```

### Filtering Rows

```go
// Simple filter
filtered := df.Filter(
    golars.ColBuilder("age").Gt(30).Build(),
)

// Complex filter with AND
filtered := df.Filter(
    golars.ColBuilder("age").Gt(25).
        And(golars.ColBuilder("salary").Lt(70000)).
        Build(),
)

// Filter with OR
filtered := df.Filter(
    golars.ColBuilder("department").Eq(golars.Lit("Sales")).
        Or(golars.ColBuilder("department").Eq(golars.Lit("Marketing"))).
        Build(),
)
```

### Sorting

```go
// Sort by single column (ascending)
sorted := df.Sort("salary")

// Sort descending
sorted := df.SortDesc("salary")

// Sort by multiple columns
sorted := df.SortBy(golars.SortOptions{
    Columns: []string{"department", "salary"},
    Orders:  []golars.SortOrder{golars.Ascending, golars.Descending},
})
```

### Aggregations

```go
// Simple aggregation
sumSalary := df.Column("salary").Sum()
avgAge := df.Column("age").Mean()

// Group by aggregations
grouped, err := df.GroupBy("department").Sum("salary")

// Multiple aggregations
grouped, err := df.GroupBy("department").Agg(map[string]golars.Expr{
    "total_salary": golars.ColBuilder("salary").Sum().Build(),
    "avg_age": golars.ColBuilder("age").Mean().Build(),
    "count": golars.ColBuilder("").Count().Build(),
})
```

### Joins

```go
// Create two DataFrames to join
employees, _ := golars.NewDataFrameFromMap(map[string]interface{}{
    "id": []int32{1, 2, 3},
    "name": []string{"Alice", "Bob", "Charlie"},
})

departments, _ := golars.NewDataFrameFromMap(map[string]interface{}{
    "id": []int32{1, 2, 3},
    "dept": []string{"Sales", "IT", "HR"},
})

// Inner join
joined := employees.Join(departments, "id", golars.InnerJoin)

// Left join
joined := employees.Join(departments, "id", golars.LeftJoin)
```

## Lazy Evaluation

For better performance with complex queries, use lazy evaluation:

```go
// Convert to lazy
lf := golars.LazyFromDataFrame(df)

// Build complex query (nothing is executed yet)
result := lf.
    Filter(golars.ColBuilder("age").Gt(25).Build()).
    Filter(golars.ColBuilder("salary").Gt(50000).Build()).
    GroupBy("department").
    Agg(map[string]golars.Expr{
        "avg_salary": golars.ColBuilder("salary").Mean().Build(),
        "total": golars.ColBuilder("salary").Sum().Build(),
        "count": golars.ColBuilder("").Count().Build(),
    }).
    Sort("avg_salary", true).
    Limit(10)

// View the optimized query plan
plan, _ := result.ExplainOptimized()
fmt.Println("Query plan:", plan)

// Execute the query
df, err := result.Collect()
```

## Working with Nulls

```go
// Create Series with null values
s := golars.NewSeriesWithValidity(
    "scores",
    []float64{95.5, 87.0, 92.5, 88.0},
    []bool{true, false, true, true}, // false = null
    golars.Float64,
)

// Check for nulls
hasNulls := s.NullCount() > 0

// Filter out nulls
filtered := df.Filter(
    golars.ColBuilder("score").IsNotNull().Build(),
)

// Fill nulls with a value
// TODO: When implemented
```

## Expressions

Golars uses expressions for complex operations:

```go
// Arithmetic
expr := golars.ColBuilder("price").
    Mul(golars.ColBuilder("quantity")).
    Build()

// Add computed column
df = df.AddColumn("total", expr)

// Conditional expressions
expr := golars.When(
    golars.ColBuilder("age").Gt(65),
    golars.Lit("Senior"),
).Otherwise(
    golars.Lit("Adult"),
).Build()
```

## Performance Tips

1. **Use Lazy Evaluation**: For complex queries with multiple operations
2. **Filter Early**: Reduce data size as soon as possible
3. **Select Only Needed Columns**: Reduces memory usage
4. **Use Type-Specific Series**: When doing many operations on one column

```go
// Good: Lazy evaluation with early filtering
result := golars.LazyFromDataFrame(df).
    Filter(golars.ColBuilder("active").Eq(golars.Lit(true)).Build()).
    SelectColumns("id", "name", "amount").
    GroupBy("name").
    Sum("amount").
    Collect()

// Less efficient: Eager evaluation
filtered := df.Filter(golars.ColBuilder("active").Eq(golars.Lit(true)).Build())
selected := filtered.Select("id", "name", "amount")
result := selected.GroupBy("name").Sum("amount")
```

## Complete Example

Here's a complete example showing common operations:

```go
package main

import (
    "fmt"
    "log"
    "github.com/davidpalaitis/golars"
)

func main() {
    // Create sample data
    df, err := golars.NewDataFrameFromMap(map[string]interface{}{
        "date": []string{"2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"},
        "product": []string{"A", "B", "A", "B"},
        "store": []string{"NY", "NY", "LA", "LA"},
        "sales": []float64{100, 150, 200, 250},
        "cost": []float64{80, 100, 150, 180},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Use lazy evaluation for efficiency
    result := golars.LazyFromDataFrame(df).
        // Add profit column
        AddColumn("profit", 
            golars.ColBuilder("sales").Sub(golars.ColBuilder("cost")).Build()).
        // Filter for profitable sales
        Filter(golars.ColBuilder("profit").Gt(0).Build()).
        // Group by store
        GroupBy("store").
        // Multiple aggregations
        Agg(map[string]golars.Expr{
            "total_sales": golars.ColBuilder("sales").Sum().Build(),
            "total_profit": golars.ColBuilder("profit").Sum().Build(),
            "avg_profit_margin": golars.ColBuilder("profit").
                Div(golars.ColBuilder("sales")).Mean().Build(),
        }).
        // Sort by total profit
        Sort("total_profit", true).
        // Execute
        Collect()

    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Store Performance:")
    fmt.Println(result)
}
```

## Next Steps

- Explore more [examples](cmd/example/)
- Read the [API documentation](context/api/public-api.md)
- Learn about [performance optimization](PERFORMANCE.md)
- Check out [contributing guidelines](CONTRIBUTING.md) to help improve Golars

## Getting Help

- Check the [documentation](context/)
- Look at [example programs](cmd/example/)
- Open an issue on GitHub for bugs or questions
- See [TROUBLESHOOTING.md](context/errors-and-fixes.md) for common issues

Happy data wrangling with Golars!
