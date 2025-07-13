# Window Functions Usage Guide

This guide provides comprehensive documentation on using window functions in Golars.

## Table of Contents
- [Overview](#overview)
- [Basic Concepts](#basic-concepts)
- [Window Specification](#window-specification)
- [Ranking Functions](#ranking-functions)
- [Aggregate Functions](#aggregate-functions)
- [Offset Functions](#offset-functions)
- [Frame Specifications](#frame-specifications)
- [Complete Examples](#complete-examples)
- [Performance Considerations](#performance-considerations)

## Overview

Window functions perform calculations across a set of rows that are related to the current row. Unlike GROUP BY operations, window functions preserve individual rows while adding computed values.

```go
import (
    "github.com/tnn1t1s/golars"
    "github.com/tnn1t1s/golars/dataframe"
    "github.com/tnn1t1s/golars/expr"
)

// Basic window function usage
df.WithColumn("row_num", 
    expr.WindowFunc(golars.RowNumber().Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary"),
    )),
)
```

## Basic Concepts

### Window Specification
A window specification defines how rows are grouped and ordered for the window function:
- **PARTITION BY**: Divides rows into groups
- **ORDER BY**: Defines the order within each partition
- **Frame**: Specifies which rows to include in calculations

### Window Frame
The frame determines which rows are included in the window calculation:
- **ROWS**: Physical offset from current row
- **RANGE**: Logical offset based on values (future enhancement)

## Window Specification

### Creating a Window Spec

```go
// Basic window spec
spec := golars.NewSpec()

// With partition
spec = golars.NewSpec().PartitionBy("department")

// With ordering
spec = golars.NewSpec().OrderBy("salary", false) // descending

// With multiple partitions and ordering
spec = golars.NewSpec().
    PartitionBy("department", "location").
    OrderBy("hire_date", true).
    OrderBy("salary", false)

// With frame specification
spec = golars.NewSpec().
    PartitionBy("department").
    OrderBy("date").
    RowsBetween(-2, 0) // 2 preceding to current row
```

## Ranking Functions

### ROW_NUMBER()
Assigns a unique sequential number to each row within a partition.

```go
// Add row numbers within each department
df = df.WithColumn("dept_row_num",
    expr.WindowFunc(golars.RowNumber().Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary", false),
    )),
)
```

### RANK()
Assigns ranks with gaps for ties.

```go
// Rank employees by salary within department
df = df.WithColumn("salary_rank",
    expr.WindowFunc(golars.Rank().Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary", false),
    )),
)
```

### DENSE_RANK()
Assigns ranks without gaps for ties.

```go
// Dense rank by performance score
df = df.WithColumn("performance_rank",
    expr.WindowFunc(golars.DenseRank().Over(
        golars.NewSpec().PartitionBy("team").OrderBy("score", false),
    )),
)
```

### PERCENT_RANK()
Calculates the relative rank as a percentage (0 to 1).

```go
// Percentile rank within department
df = df.WithColumn("salary_percentile",
    expr.WindowFunc(golars.PercentRank().Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary"),
    )),
)
```

### NTILE(n)
Divides rows into n buckets.

```go
// Divide employees into 4 quartiles by salary
df = df.WithColumn("salary_quartile",
    expr.WindowFunc(golars.NTile(4).Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary"),
    )),
)
```

## Aggregate Functions

Window aggregate functions calculate values over a frame of rows.

### SUM()
```go
// Running total of sales
df = df.WithColumn("running_total",
    expr.WindowFunc(golars.Sum("amount").Over(
        golars.NewSpec().PartitionBy("product").OrderBy("date"),
    )),
)

// Sum over custom frame (last 7 days)
df = df.WithColumn("week_total",
    expr.WindowFunc(golars.Sum("amount").Over(
        golars.NewSpec().
            PartitionBy("product").
            OrderBy("date").
            RowsBetween(-6, 0),
    )),
)
```

### AVG()
```go
// Moving average over last 3 rows
df = df.WithColumn("moving_avg",
    expr.WindowFunc(golars.Avg("price").Over(
        golars.NewSpec().
            OrderBy("date").
            RowsBetween(-2, 0),
    )),
)
```

### MIN() and MAX()
```go
// Running minimum and maximum
df = df.WithColumn("running_min",
    expr.WindowFunc(golars.Min("temperature").Over(
        golars.NewSpec().OrderBy("timestamp"),
    )),
)

df = df.WithColumn("running_max",
    expr.WindowFunc(golars.Max("temperature").Over(
        golars.NewSpec().OrderBy("timestamp"),
    )),
)
```

### COUNT()
```go
// Count rows in frame
df = df.WithColumn("window_count",
    expr.WindowFunc(golars.Count("*").Over(
        golars.NewSpec().
            PartitionBy("category").
            RowsBetween(-2, 2), // 5-row window
    )),
)
```

## Offset Functions

Offset functions access values from other rows in the partition.

### LAG()
Accesses values from previous rows.

```go
// Previous day's price
df = df.WithColumn("prev_price",
    expr.WindowFunc(golars.Lag("price", 1, 0.0).Over(
        golars.NewSpec().OrderBy("date"),
    )),
)

// Calculate daily change
df = df.WithColumn("daily_change",
    expr.Col("price").Sub(expr.Col("prev_price")),
)
```

### LEAD()
Accesses values from following rows.

```go
// Next month's forecast
df = df.WithColumn("next_forecast",
    expr.WindowFunc(golars.Lead("forecast", 1, nil).Over(
        golars.NewSpec().OrderBy("month"),
    )),
)
```

### FIRST_VALUE() and LAST_VALUE()
```go
// First and last values in partition
df = df.WithColumn("first_sale",
    expr.WindowFunc(golars.FirstValue("amount").Over(
        golars.NewSpec().PartitionBy("product").OrderBy("date"),
    )),
)

df = df.WithColumn("last_sale",
    expr.WindowFunc(golars.LastValue("amount").Over(
        golars.NewSpec().PartitionBy("product").OrderBy("date"),
    )),
)
```

## Frame Specifications

### ROWS Frame
Physical row-based frames.

```go
// Different frame specifications
spec1 := golars.NewSpec().RowsBetween(-1, 1)  // 1 preceding to 1 following
spec2 := golars.NewSpec().RowsBetween(-2, 0)  // 2 preceding to current
spec3 := golars.NewSpec().RowsBetween(0, 2)   // current to 2 following

// Special frame boundaries
// Unbounded preceding to current (default for ordered windows)
spec4 := golars.NewSpec().OrderBy("date")

// For unbounded frames, use the frame specification directly
spec5 := golars.NewSpec()
spec5.frame = &golars.FrameSpec{
    Type:  golars.RowsFrame,
    Start: golars.FrameBound{Type: golars.UnboundedPreceding},
    End:   golars.FrameBound{Type: golars.UnboundedFollowing},
}
```

### Default Frames
- **Ordered window**: UNBOUNDED PRECEDING to CURRENT ROW
- **Unordered window**: UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING

## Complete Examples

### Sales Analysis
```go
// Load sales data
df, _ := dataframe.New(
    series.NewStringSeries("product", []string{"A", "A", "A", "B", "B", "B"}),
    series.NewStringSeries("date", []string{"2024-01", "2024-02", "2024-03", "2024-01", "2024-02", "2024-03"}),
    series.NewInt32Series("amount", []int32{100, 150, 120, 200, 180, 220}),
)

// Add multiple window calculations
df = df.WithColumn("running_total",
    expr.WindowFunc(golars.Sum("amount").Over(
        golars.NewSpec().PartitionBy("product").OrderBy("date"),
    )),
).WithColumn("pct_of_total",
    expr.Col("amount").Div(
        expr.WindowFunc(golars.Sum("amount").Over(
            golars.NewSpec().PartitionBy("product"),
        )),
    ),
).WithColumn("rank",
    expr.WindowFunc(golars.Rank().Over(
        golars.NewSpec().PartitionBy("product").OrderBy("amount", false),
    )),
)
```

### Time Series Analysis
```go
// Moving averages and trends
df = df.WithColumn("ma_7day",
    expr.WindowFunc(golars.Avg("value").Over(
        golars.NewSpec().OrderBy("date").RowsBetween(-6, 0),
    )),
).WithColumn("ma_30day",
    expr.WindowFunc(golars.Avg("value").Over(
        golars.NewSpec().OrderBy("date").RowsBetween(-29, 0),
    )),
).WithColumn("prev_value",
    expr.WindowFunc(golars.Lag("value", 1, 0.0).Over(
        golars.NewSpec().OrderBy("date"),
    )),
).WithColumn("change_pct",
    expr.Col("value").Sub(expr.Col("prev_value")).
        Div(expr.Col("prev_value")).Mul(expr.Lit(100)),
)
```

### Employee Rankings
```go
// Complex ranking with ties
df = df.WithColumn("dept_salary_rank",
    expr.WindowFunc(golars.Rank().Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary", false),
    )),
).WithColumn("overall_salary_rank", 
    expr.WindowFunc(golars.Rank().Over(
        golars.NewSpec().OrderBy("salary", false),
    )),
).WithColumn("dept_size",
    expr.WindowFunc(golars.Count("employee_id").Over(
        golars.NewSpec().PartitionBy("department"),
    )),
).WithColumn("salary_percentile",
    expr.WindowFunc(golars.PercentRank().Over(
        golars.NewSpec().PartitionBy("department").OrderBy("salary"),
    )),
)
```

## Performance Considerations

### Partitioning
- Smaller partitions generally perform better
- Consider partition size distribution
- Multiple small partitions can be processed in parallel (future enhancement)

### Ordering
- Ordering adds computational overhead
- Use ordering only when necessary
- Consider pre-sorting data for better performance

### Frame Specifications
- Smaller frames are more efficient
- Unbounded frames require accessing all partition rows
- Row-based frames are currently more efficient than range-based

### Memory Usage
- Window functions materialize results for entire partitions
- Large partitions may require significant memory
- Consider breaking large datasets into smaller chunks

### Best Practices
1. Use appropriate data types (int32 vs int64)
2. Minimize the number of window functions in a single query
3. Consider pre-aggregating data when possible
4. Use partitioning to limit calculation scope

## Limitations and Future Enhancements

### Current Limitations
- RANGE frames only support basic functionality
- No support for EXCLUDE clause
- Limited support for custom aggregate functions
- No parallel execution of partitions

### Planned Enhancements
- Full RANGE frame support with value-based boundaries
- GROUPS frame type
- Custom window aggregate functions
- Parallel partition processing
- More offset functions (NTH_VALUE)
- Window function chaining optimizations