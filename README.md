# Golars

[![Go Reference](https://pkg.go.dev/badge/github.com/tnn1t1s/golars.svg)](https://pkg.go.dev/github.com/tnn1t1s/golars)
[![Go Report Card](https://goreportcard.com/badge/github.com/tnn1t1s/golars)](https://goreportcard.com/report/github.com/tnn1t1s/golars)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance DataFrame library for Go, inspired by Polars. Golars provides fast, memory-efficient data manipulation with a familiar API for data scientists and engineers working in Go.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Examples](#examples)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## Features

- 🚀 **High Performance**: Built on Apache Arrow for columnar memory layout
- 🔧 **Rich API**: Comprehensive data manipulation operations
- 📊 **DataFrames & Series**: Core data structures for tabular and columnar data
- 🔗 **Expression API**: Lazy evaluation for query optimization
- 📁 **Multiple I/O Formats**: CSV, Parquet, JSON support
- 🔄 **Data Operations**: Filtering, grouping, joining, sorting, and aggregations
- 🪟 **Window Functions**: SQL-like analytical functions
- 🔤 **String Operations**: Comprehensive string manipulation
- 📅 **DateTime Support**: Full temporal data handling
- 🧮 **Statistical Functions**: Built-in statistical operations

## Installation

```bash
go get github.com/tnn1t1s/golars
```

### Requirements

- Go 1.21 or higher
- CGO enabled (for Apache Arrow)

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/tnn1t1s/golars"
)

func main() {
    // Create a DataFrame from a map
    df, err := golars.DataFrameFrom(map[string]interface{}{
        "name":   []string{"Alice", "Bob", "Charlie"},
        "age":    []int{25, 30, 35},
        "salary": []float64{50000, 60000, 75000},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // Filter and calculate mean salary
    result, err := df.
        Filter(golars.Col("age").Gt(25)).
        GroupBy("age").
        Agg(map[string]golars.Expr{
            "avg_salary": golars.Col("salary").Mean(),
        })
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println(result)
}
```

## Performance Preview

Early benchmarks show Golars achieving competitive performance with Polars. In light dataset tests (10,000 rows, queries Q1-Q6), Golars completed all operations in 23.17ms compared to Polars' 24.24ms - a 4% performance advantage. While these are preliminary results on small datasets, they demonstrate Golars' efficient foundation built on Apache Arrow. Individual query performance ranged from 0.65x to 1.34x relative to Polars, with most queries performing at or better than parity. See the [Performance](#performance) section for detailed benchmark results.

## Core Concepts

### DataFrame

A DataFrame is a 2-dimensional table with labeled columns, similar to a spreadsheet or SQL table.

```go
// Create from map
df, _ := golars.DataFrameFrom(map[string]interface{}{
    "col1": []int{1, 2, 3},
    "col2": []string{"a", "b", "c"},
})

// Create from series
df, _ := golars.NewDataFrame(
    golars.NewInt32Series("id", []int32{1, 2, 3}),
    golars.NewStringSeries("name", []string{"a", "b", "c"}),
)
```

### Series

A Series is a 1-dimensional array with a name and data type.

```go
// Type-specific constructors
s1 := golars.NewInt64Series("numbers", []int64{1, 2, 3, 4, 5})
s2 := golars.NewStringSeries("names", []string{"Alice", "Bob"})
s3 := golars.NewFloat64Series("scores", []float64{95.5, 87.0, 92.5})

// With null values
s4 := golars.NewSeriesWithValidity("values", 
    []int32{1, 2, 3}, 
    []bool{true, false, true},  // false indicates null
    golars.Int32,
)
```

### Expressions

Expressions enable lazy evaluation and query optimization.

```go
// Column reference
golars.Col("age")

// Literal value
golars.Lit(42)

// Complex expressions
expr := golars.Col("price").Mul(golars.Col("quantity")).Alias("total")

// Conditional expressions
golars.When(golars.Col("age").Gt(18)).
    Then(golars.Lit("adult")).
    Otherwise(golars.Lit("minor"))
```

## Examples

### Data Loading

```go
// Read CSV
df, err := golars.ReadCSV("data.csv",
    golars.WithDelimiter(','),
    golars.WithHeader(true),
)

// Read Parquet
df, err := golars.ReadParquet("data.parquet")

// Read JSON
df, err := golars.ReadJSON("data.json")
```

### Data Manipulation

```go
// Select columns
selected, _ := df.Select("name", "age")

// Filter rows
filtered, _ := df.Filter(golars.Col("age").Gt(25))

// Sort
sorted, _ := df.Sort("age", "name")

// Group by and aggregate
grouped, _ := df.GroupBy("department").Agg(map[string]golars.Expr{
    "avg_salary": golars.Col("salary").Mean(),
    "count":      golars.Col("id").Count(),
})

// Join
joined, _ := df1.Join(df2, "id", golars.InnerJoin)
```

### Window Functions

```go
// Add row numbers partitioned by department
windowed, _ := df.WithColumn("row_num",
    golars.RowNumber().Over(
        golars.Window().PartitionBy("department").OrderBy("salary"),
    ),
)

// Calculate running sum
cumulative, _ := df.WithColumn("running_total",
    golars.Sum("sales").Over(
        golars.Window().OrderBy("date"),
    ),
)
```

## Project Structure

```
golars/
├── README.md                    # This file
├── go.mod                       # Go module definition
├── go.sum                       # Go module checksums
├── golars.go                    # Main package file with public API
├── dataframe_auto.go            # Automatic DataFrame creation utilities
├── series_auto.go               # Automatic Series creation utilities
├── golars_example_test.go       # Package-level examples
│
├── expr/                        # Expression API
│   ├── expr.go                  # Core expression types
│   ├── col.go                   # Column expressions
│   ├── binary_methods.go        # Binary operations (Add, Sub, etc.)
│   ├── string_expr.go           # String-specific expressions
│   ├── when_builder.go          # Conditional expressions
│   ├── special_exprs.go         # Special expressions (IsIn, Between)
│   └── expr_test.go             # Expression tests
│
├── frame/                       # DataFrame implementation
│   ├── dataframe.go             # Core DataFrame type
│   ├── filter.go                # Filtering operations
│   ├── groupby.go               # GroupBy functionality
│   ├── join.go                  # Join operations
│   ├── sort.go                  # Sorting functionality
│   ├── concat.go                # Concatenation operations
│   ├── cumulative.go            # Cumulative operations
│   ├── interpolate.go           # Missing value interpolation
│   ├── melt.go                  # Melt/unpivot operations
│   ├── pivot.go                 # Pivot operations
│   ├── reshape.go               # Reshaping operations
│   ├── stats.go                 # Basic statistics
│   ├── stats_advanced.go        # Advanced statistics
│   └── *_test.go                # Corresponding test files
│
├── series/                      # Series implementation
│   ├── series.go                # Core Series type
│   ├── aggregations.go          # Aggregation functions
│   ├── sort.go                  # Sorting functionality
│   └── *_test.go                # Test files
│
├── io/                          # Input/Output operations
│   ├── io.go                    # Common I/O interfaces
│   ├── csv/                     # CSV support
│   │   ├── reader.go            # CSV reading
│   │   └── writer.go            # CSV writing
│   ├── parquet/                 # Parquet support
│   │   ├── reader.go            # Parquet reading
│   │   └── writer.go            # Parquet writing
│   └── json/                    # JSON support
│       ├── reader.go            # JSON reading
│       ├── writer.go            # JSON writing
│       └── ndjson_reader.go     # Newline-delimited JSON
│
├── internal/                    # Internal packages
│   ├── chunked/                 # Chunked array operations
│   ├── compute/                 # Computational kernels
│   ├── datatypes/               # Data type definitions
│   ├── datetime/                # DateTime operations
│   ├── group/                   # Grouping operations
│   ├── strings/                 # String operations
│   └── window/                  # Window function implementation
│
├── benchmarks/                  # Performance benchmarks
│   ├── README.md                # Benchmark documentation
│   ├── groupby/                 # GroupBy benchmarks
│   ├── filter/                  # Filter benchmarks
│   ├── join/                    # Join benchmarks
│   ├── io/                      # I/O benchmarks
│   └── data/                    # Benchmark data generation
│
├── testutil/                    # Testing utilities
│   ├── assertions.go            # Test assertion helpers
│   ├── fixtures.go              # Test data fixtures
│   └── helpers.go               # General test helpers
│
├── docs/                        # Documentation
│   ├── GETTING_STARTED.md       # Getting started guide
│   ├── PERFORMANCE.md           # Performance guide
│   ├── WINDOW_FUNCTIONS.md      # Window functions guide
│   ├── STRING_OPERATIONS_DESIGN.md  # String operations design
│   ├── DATETIME_DESIGN.md       # DateTime implementation
│   └── ...                      # Other documentation
│
├── tools/                       # Development tools
│   ├── README.md                # Tools documentation
│   └── monitor-agent.scpt       # AppleScript monitoring tool
│
├── claude-docs/                 # Claude AI documentation
│   └── memory_optimization_strategy.md
│
├── polars/                      # Polars reference implementation (git submodule)
│
└── transcripts/                 # Development session transcripts
```

### Key Files

- **`golars.go`**: Main entry point, re-exports public API
- **`dataframe_auto.go`**: Automatic type inference for DataFrame creation
- **`series_auto.go`**: Automatic type inference for Series creation
- **`expr/expr.go`**: Expression API core types and interfaces
- **`frame/dataframe.go`**: DataFrame implementation
- **`series/series.go`**: Series implementation

## API Reference

### DataFrame Operations

| Method | Description |
|--------|-------------|
| `NewDataFrame(series...)` | Create DataFrame from Series |
| `DataFrameFrom(data)` | Create DataFrame with type inference |
| `Select(columns...)` | Select specific columns |
| `Filter(expr)` | Filter rows based on expression |
| `GroupBy(columns...)` | Group by columns |
| `Join(other, on, how)` | Join with another DataFrame |
| `Sort(columns...)` | Sort by columns |
| `Head(n)` | Get first n rows |
| `Tail(n)` | Get last n rows |
| `WithColumn(name, expr)` | Add/replace column |
| `Drop(columns...)` | Remove columns |

### Series Operations

| Method | Description |
|--------|-------------|
| `NewXXXSeries(name, values)` | Type-specific constructors |
| `Len()` | Get length |
| `IsNull(i)` | Check if value is null |
| `Get(i)` | Get value at index |
| `Sum()` | Calculate sum |
| `Mean()` | Calculate mean |
| `Min()` | Find minimum |
| `Max()` | Find maximum |
| `Unique()` | Get unique values |
| `Sort()` | Sort values |

### Expression Methods

| Method | Description |
|--------|-------------|
| `Col(name)` | Reference a column |
| `Lit(value)` | Create literal value |
| `Add(other)` | Addition |
| `Sub(other)` | Subtraction |
| `Mul(other)` | Multiplication |
| `Div(other)` | Division |
| `Gt(other)` | Greater than |
| `Lt(other)` | Less than |
| `Eq(other)` | Equal to |
| `And(other)` | Logical AND |
| `Or(other)` | Logical OR |
| `IsNull()` | Check for null |
| `IsIn(values)` | Check membership |

## Performance

Golars is designed for performance with:

- **Columnar Storage**: Apache Arrow format for efficient memory layout
- **Zero-Copy Operations**: Where possible
- **Parallel Execution**: For many operations
- **Memory Efficiency**: Minimal allocations

### Benchmark Results

Initial benchmarks show promising performance compared to Polars (light dataset, Q1-Q6):

| Query | Golars (ms) | Polars (ms) | Ratio | Status |
|-------|-------------|-------------|-------|--------|
| Q1    | 2.26        | 3.48        | 0.65x | 🟢 Faster |
| Q2    | 5.93        | 6.27        | 0.95x | 🟢 Faster |
| Q3    | 2.90        | 3.12        | 0.93x | 🟢 Faster |
| Q4    | 2.62        | 2.22        | 1.18x | 🟢 Good |
| Q5    | 2.63        | 1.96        | 1.34x | 🟢 Good |
| Q6    | 6.83        | 7.19        | 0.95x | 🟢 Faster |
| **Total** | **23.17** | **24.24** | **0.96x** | **🟢 Faster overall** |

*Note: These are preliminary results from light benchmarks (10,000 rows). Polars is a mature, highly-optimized Rust library, so achieving competitive performance demonstrates Golars' promising foundation. Full benchmark suite results coming soon.*

See [benchmarks/README.md](benchmarks/README.md) for detailed performance comparisons and methodology.

### Running Benchmarks

```bash
# Run all benchmarks
cd benchmarks
just run-full-golars

# Run specific benchmark
go test -bench=BenchmarkGroupByQ1 ./benchmarks/groupby -benchmem
```

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/tnn1t1s/golars.git
   cd golars
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Run tests:
   ```bash
   go test ./...
   ```

4. Run benchmarks:
   ```bash
   cd benchmarks
   make benchmark-all
   ```

### Code Style

- Follow standard Go conventions
- Run `go fmt` before committing
- Add tests for new features
- Update documentation as needed

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Polars](https://github.com/pola-rs/polars) - Inspiration for the API and functionality
- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format
- The Go community for excellent tooling and libraries