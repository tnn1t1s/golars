# Golars - High-Performance DataFrames for Go

Golars is a blazingly fast DataFrame library for Go, inspired by the Polars library for Rust/Python. It provides a modern, performant alternative for data manipulation in Go with a focus on:

- **Columnar data storage** using Apache Arrow format
- **Lazy evaluation** for query optimization
- **Parallel execution** leveraging Go's concurrency primitives
- **Type safety** with Go generics
- **Zero-copy operations** where possible

## Features

### Core Data Structures
- **DataFrame**: Column-oriented table with schema validation
- **Series**: Strongly-typed columns with null support
- **ChunkedArray**: Efficient columnar storage using Apache Arrow

### Operations
- **Filtering**: Expression-based with complex boolean logic
- **Selection**: Column projection and computed columns
- **GroupBy**: Single/multi-column grouping with aggregations
- **Joins**: Inner, Left, Right, Outer, Cross, Anti, Semi joins
- **Sorting**: Single/multi-column with configurable null handling
- **Aggregations**: Sum, Mean, Min, Max, Count, Std, Var

### Lazy Evaluation & Query Optimization
- **LazyFrame**: Build query plans without immediate execution
- **Predicate Pushdown**: Push filters closer to data source
- **Projection Pushdown**: Read only required columns
- **Query Planning**: Inspect and optimize before execution
- **Expression Optimization**: Combine and simplify expressions

### I/O Support
- **CSV**: Read/write with type inference and custom options
- **Parquet**: Read/write with compression support (Snappy, Gzip, Zstd)
  - Lazy evaluation support with ScanParquet
  - Column projection pushdown
  - Predicate pushdown optimization
- **JSON**: Planned

### Expression API
- **Fluent Builder**: Chain operations naturally
- **Type Safe**: Compile-time type checking with generics
- **Rich Operations**: Arithmetic, comparison, logical, aggregations
- **Null Aware**: Proper null handling throughout

## Installation

```bash
go get github.com/davidpalaitis/golars
```

## Quick Start

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
    "github.com/davidpalaitis/golars"
)

func main() {
    // Create a DataFrame
    df, err := golars.NewDataFrame(
        golars.NewSeries("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
        golars.NewSeries("age", []int32{25, 30, 35, 28, 32}),
        golars.NewSeries("city", []string{"NYC", "LA", "Chicago", "NYC", "LA"}),
        golars.NewSeries("salary", []float64{70000, 85000, 95000, 75000, 90000}),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // Filter and select
    result := df.
        Filter(golars.ColBuilder("age").Gt(28).Build()).
        Select("name", "city", "salary")
    
    fmt.Println(result)
}
```

### Lazy Evaluation with Query Optimization

```go
// Use lazy evaluation for better performance
lf := golars.LazyFromDataFrame(df).
    Filter(golars.ColBuilder("age").Gt(25).Build()).
    Filter(golars.ColBuilder("salary").Gt(80000).Build()).
    GroupBy("city").
    Agg(map[string]golars.Expr{
        "avg_salary": golars.ColBuilder("salary").Mean().Build(),
        "count": golars.ColBuilder("").Count().Build(),
    }).
    Sort("avg_salary", true)

// Inspect the optimized query plan
optimizedPlan, _ := lf.ExplainOptimized()
fmt.Println("Optimized Plan:")
fmt.Println(optimizedPlan)

// Execute the query
result, err := lf.Collect()
if err != nil {
    log.Fatal(err)
}
fmt.Println("\nResult:")
fmt.Println(result)
```

### Reading from CSV

```go
// Read CSV with automatic type inference
df, err := golars.ReadCSV("data.csv",
    golars.WithDelimiter(','),
    golars.WithNullValues([]string{"NA", "null", ""}),
)
if err != nil {
    log.Fatal(err)
}

// Process with lazy evaluation
result := golars.LazyFromDataFrame(df).
    Filter(golars.ColBuilder("status").Eq(golars.Lit("active")).Build()).
    SelectColumns("id", "name", "value").
    Collect()
```

### Lazy Parquet Reading

```go
// Create a lazy scan of a Parquet file
lf := golars.ScanParquet("large_dataset.parquet").
    Filter(golars.ColBuilder("year").Eq(golars.Lit(2024)).Build()).
    SelectColumns("id", "name", "amount").  // Only read needed columns
    GroupBy("name").
    Agg(map[string]golars.Expr{
        "total": golars.ColBuilder("amount").Sum().Build(),
        "count": golars.ColBuilder("name").Count().Build(),
    })

// The query is optimized before execution
optimizedPlan, _ := lf.ExplainOptimized()
fmt.Println(optimizedPlan)  // Shows predicate/projection pushdown

// Execute the optimized query
result, err := lf.Collect()
```

## Architecture

Golars is built on top of Apache Arrow for efficient columnar data storage and processing. The architecture consists of:

- **ChunkedArray**: Generic, strongly-typed columnar storage
- **Series**: Type-erased column wrapper with dynamic dispatch
- **DataFrame**: Collection of Series with schema validation
- **Expression Engine**: DSL for building complex queries
- **Lazy Evaluation**: Query optimization before execution
- **Compute Kernels**: Vectorized operations for performance

## Performance

Golars is designed for high performance with minimal allocations:

| Operation | Performance | Notes |
|-----------|------------|-------|
| ChunkedArray Get | 25ns/op | Zero allocations |
| Series Get | 35ns/op | Minimal allocations |
| DataFrame Creation | 697ns/op | 10 columns |
| Filter 100k rows | 6ms | Simple condition |
| Arithmetic 100k | 1ms | Vectorized operations |
| GroupBy 10k rows | ~1ms | Hash-based grouping |
| Sort 10k rows | ~3ms | Parallel sorting |

Query optimization can significantly improve performance:
- **Predicate Pushdown**: Reduces data scanned by filtering early
- **Projection Pushdown**: Reads only required columns
- **Expression Simplification**: Combines multiple filters efficiently

## Documentation

- [API Reference](context/api/public-api.md)
- [Implementation Details](IMPLEMENTATION_SUMMARY.md)
- [Examples](cmd/example/)
- [Performance Guide](PERFORMANCE.md)
- [Contributing](CONTRIBUTING.md)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details