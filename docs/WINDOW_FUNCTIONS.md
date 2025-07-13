# Window Functions Design Document

## Table of Contents
- [Overview & Motivation](#overview--motivation)
- [API Design](#api-design)
- [Architecture Design](#architecture-design)
- [Implementation Plan](#implementation-plan)
- [Technical Details](#technical-details)
- [Examples](#examples)
- [Performance Considerations](#performance-considerations)
- [Testing Strategy](#testing-strategy)

## Current Implementation Status

As of December 2024, Golars has successfully implemented comprehensive window function capabilities:

### ✅ Completed Features

1. **Core Infrastructure**
   - Window specification builder with fluent API
   - Partition and ordering support
   - DataFrame integration via `WithColumn` and `WithColumns`
   - Efficient partition management
   - Expression system integration

2. **Ranking Functions**
   - `ROW_NUMBER()` - Sequential numbering within partitions
   - `RANK()` - Ranking with gaps for ties
   - `DENSE_RANK()` - Ranking without gaps for ties
   - `PERCENT_RANK()` - Relative rank as percentage
   - `NTILE(n)` - Dividing rows into n buckets

3. **Offset Functions**
   - `LAG(column, offset, default)` - Access previous row values
   - `LEAD(column, offset, default)` - Access following row values
   - `FIRST_VALUE(column)` - First value in window
   - `LAST_VALUE(column)` - Last value in window

4. **Aggregate Window Functions**
   - `SUM(column)` - Running or windowed sum
   - `AVG(column)` - Running or windowed average
   - `MIN(column)` - Running or windowed minimum
   - `MAX(column)` - Running or windowed maximum
   - `COUNT(column)` - Running or windowed count

5. **Frame Specifications**
   - ROWS BETWEEN support with all boundary types
   - Unbounded preceding/following
   - Current row references
   - Offset-based boundaries

6. **Testing**
   - Comprehensive unit tests for all functions
   - Partitioning behavior tests
   - Frame boundary tests
   - Edge case coverage

### 📋 Future Enhancements
- Range-based frames (RANGE BETWEEN with value offsets)
- GROUPS frame type
- NTH_VALUE() function
- EXCLUDE clause support
- Parallel partition processing
- Integration with lazy evaluation for optimization

## Overview & Motivation

### What are Window Functions?

Window functions perform calculations across a set of rows that are related to the current row. Unlike aggregate functions that return a single result per group, window functions return a result for every row while still being able to access data from other rows in the window.

### Why Window Functions Matter

Window functions are essential for:
- **Time Series Analysis**: Calculate moving averages, running totals, period-over-period comparisons
- **Ranking**: Assign row numbers, ranks, or percentiles within groups
- **Data Enrichment**: Access previous/next values (lag/lead) for trend analysis
- **Analytics**: Complex calculations that require row-by-row context

### Window Functions vs GroupBy

| Aspect | GroupBy | Window Functions |
|--------|---------|------------------|
| Result Rows | One per group | One per input row |
| Access Pattern | Only aggregated values | Individual row access within window |
| Use Case | Summarization | Row-wise calculations with context |

## API Design

### Core API

```go
// Basic window function usage
df.WithColumn("running_total", 
    golars.Col("sales").Sum().Over(
        golars.Window().
            PartitionBy("region").
            OrderBy("date"),
    ))

// Ranking within groups
df.WithColumn("rank",
    golars.RowNumber().Over(
        golars.Window().
            PartitionBy("category").
            OrderBy("score", false), // descending
    ))

// Moving average
df.WithColumn("ma7",
    golars.Col("price").Mean().Over(
        golars.Window().
            OrderBy("date").
            RowsBetween(-6, 0), // 7-day window
    ))

// Lag/Lead operations
df.WithColumn("prev_value",
    golars.Col("value").Lag(1, 0).Over(
        golars.Window().OrderBy("timestamp"),
    ))
```

### Window Specification API

```go
// Window specification builder
type WindowSpec interface {
    // Define partitioning columns
    PartitionBy(cols ...string) WindowSpec
    
    // Define ordering
    OrderBy(col string, ascending ...bool) WindowSpec
    OrderByExpr(expr Expr) WindowSpec
    
    // Define frame boundaries
    RowsBetween(start, end int) WindowSpec
    RangeBetween(start, end interface{}) WindowSpec
    
    // Convenience methods
    UnboundedPreceding() WindowSpec
    UnboundedFollowing() WindowSpec
    CurrentRow() WindowSpec
}

// Create a window specification
func Window() WindowSpec
```

### Window Functions

```go
// Ranking functions
func RowNumber() WindowExpr
func Rank() WindowExpr
func DenseRank() WindowExpr
func PercentRank() WindowExpr
func NTile(n int) WindowExpr

// Value functions
func FirstValue(expr Expr) WindowExpr
func LastValue(expr Expr) WindowExpr
func NthValue(expr Expr, n int) WindowExpr
func Lag(expr Expr, offset int, defaultValue ...interface{}) WindowExpr
func Lead(expr Expr, offset int, defaultValue ...interface{}) WindowExpr

// Aggregate functions (window-aware)
// These are extensions of existing aggregate functions
expr.Sum().Over(window)
expr.Mean().Over(window)
expr.Min().Over(window)
expr.Max().Over(window)
expr.Count().Over(window)
expr.StdDev().Over(window)
expr.Var().Over(window)
```

### DataFrame Integration

```go
// Add or replace columns with window calculations
func (df *DataFrame) WithColumn(name string, expr Expr) (*DataFrame, error)

// Multiple window calculations at once
func (df *DataFrame) WithColumns(exprs map[string]Expr) (*DataFrame, error)
```

## Architecture Design

### Package Structure

```
window/
├── spec.go          // WindowSpec interface and implementation
├── expr.go          // Window expression types
├── functions.go     // Window function implementations
├── partition.go     // Partitioning logic
├── frame.go         // Frame boundary calculations
├── executor.go      // Window function execution engine
├── executor_test.go
├── benchmarks_test.go
└── doc.go
```

### Core Types

```go
// window/spec.go
type WindowSpec struct {
    partitionBy []string
    orderBy     []OrderClause
    frameSpec   *FrameSpec
}

type OrderClause struct {
    column    string
    expr      expr.Expr
    ascending bool
}

type FrameSpec struct {
    frameType  FrameType  // ROWS or RANGE
    start      FrameBound
    end        FrameBound
}

type FrameType int
const (
    RowsFrame FrameType = iota
    RangeFrame
)

type FrameBound struct {
    boundType BoundType
    offset    interface{} // int for ROWS, comparable value for RANGE
}

type BoundType int
const (
    UnboundedPreceding BoundType = iota
    Preceding
    CurrentRow
    Following
    UnboundedFollowing
)
```

### Window Expression Types

```go
// window/expr.go
type WindowExpr interface {
    expr.Expr
    Over(spec WindowSpec) expr.Expr
}

// Concrete window expression that wraps a function and window spec
type windowExpr struct {
    function WindowFunction
    spec     WindowSpec
    alias    string
}

// Window function interface
type WindowFunction interface {
    // Compute the function over a partition
    Compute(partition WindowPartition) (series.Series, error)
    
    // Return the expected output data type
    DataType(inputType datatypes.DataType) datatypes.DataType
    
    // Function name for display
    Name() string
}
```

### Window Partition

```go
// window/partition.go
type WindowPartition interface {
    // Get the series for this partition
    Series() series.Series
    
    // Get the sort indices (if ordered)
    SortIndices() []int
    
    // Get the frame bounds for a specific row
    FrameBounds(row int) (start, end int)
    
    // Partition metadata
    Size() int
    IsOrdered() bool
}
```

### Integration with Expression System

```go
// Extend the existing Expr interface
type Expr interface {
    // ... existing methods ...
    
    // For aggregate expressions, add Over method
    Over(spec WindowSpec) Expr
}

// Extend AggExpr to support window specs
type AggExpr struct {
    expr   Expr
    aggOp  AggOp
    window *WindowSpec // nil for regular aggregation
}
```

## Implementation Plan

### Phase 1: Core Infrastructure ✅
- [x] Create window package structure
- [x] Implement WindowSpec builder
- [x] Define WindowFunction interface
- [x] Implement WindowPartition
- [x] Create basic window expression type
- [x] Add WithColumn method to DataFrame

### Phase 2: Ranking Functions ✅
- [x] Implement row_number()
- [x] Implement rank() with proper tie handling
- [x] Implement dense_rank() with proper tie handling
- [x] Implement percent_rank()
- [x] Implement ntile()
- [ ] Add comprehensive tests

### Phase 3: Offset Functions ✅
- [x] Implement lag()
- [x] Implement lead()
- [x] Implement first_value()
- [x] Implement last_value()
- [ ] Implement nth_value()
- [x] Handle nulls and default values

### Phase 4: Rolling Aggregations
- [ ] Extend existing aggregation functions with Over()
- [ ] Implement efficient rolling computation
- [ ] Add support for custom frame boundaries
- [ ] Optimize for common patterns (fixed-size windows)

### Phase 5: Advanced Features
- [ ] Range-based window frames
- [ ] Exclude current row option
- [ ] Groups within windows
- [ ] Window function chaining
- [ ] Lazy evaluation integration

## Technical Details

### Partitioning Strategy

```go
// Efficient partitioning using hash-based grouping
func partitionData(df *DataFrame, partitionBy []string) ([]Partition, error) {
    if len(partitionBy) == 0 {
        // Single partition containing all rows
        return []Partition{{indices: allIndices(df.Height())}}, nil
    }
    
    // Reuse GroupBy logic for partitioning
    gb, err := group.NewGroupBy(df, partitionBy)
    if err != nil {
        return nil, err
    }
    
    // Convert groups to partitions
    partitions := make([]Partition, 0, gb.NumGroups())
    for _, group := range gb.Groups() {
        partitions = append(partitions, Partition{
            key:     group.Key,
            indices: group.Indices,
        })
    }
    
    return partitions, nil
}
```

### Sorting Within Partitions

```go
// Sort indices within each partition
func sortPartition(series series.Series, indices []int, orderBy []OrderClause) []int {
    // Create a copy of indices to sort
    sorted := make([]int, len(indices))
    copy(sorted, indices)
    
    // Multi-column stable sort
    sort.SliceStable(sorted, func(i, j int) bool {
        idx1, idx2 := sorted[i], sorted[j]
        
        for _, clause := range orderBy {
            val1 := clause.expr.Evaluate(idx1)
            val2 := clause.expr.Evaluate(idx2)
            
            cmp := compare(val1, val2)
            if cmp != 0 {
                return (cmp < 0) == clause.ascending
            }
        }
        
        return false
    })
    
    return sorted
}
```

### Frame Boundary Calculation

```go
// Calculate frame boundaries for ROWS frame type
func calculateRowBounds(currentRow int, frameSpec *FrameSpec, partitionSize int) (start, end int) {
    start = 0
    end = partitionSize
    
    // Calculate start boundary
    switch frameSpec.start.boundType {
    case UnboundedPreceding:
        start = 0
    case Preceding:
        offset := frameSpec.start.offset.(int)
        start = max(0, currentRow - offset)
    case CurrentRow:
        start = currentRow
    case Following:
        offset := frameSpec.start.offset.(int)
        start = min(partitionSize, currentRow + offset)
    }
    
    // Calculate end boundary
    switch frameSpec.end.boundType {
    case UnboundedFollowing:
        end = partitionSize
    case Following:
        offset := frameSpec.end.offset.(int)
        end = min(partitionSize, currentRow + offset + 1)
    case CurrentRow:
        end = currentRow + 1
    case Preceding:
        offset := frameSpec.end.offset.(int)
        end = max(0, currentRow - offset + 1)
    }
    
    return start, end
}
```

### Memory Management

```go
// Efficient window buffer for rolling calculations
type WindowBuffer struct {
    values []interface{}
    start  int
    end    int
    size   int
}

func (w *WindowBuffer) Add(value interface{}) {
    if w.end < len(w.values) {
        w.values[w.end] = value
    } else {
        w.values = append(w.values, value)
    }
    w.end++
    
    // Maintain window size
    if w.end - w.start > w.size {
        w.start++
    }
}

func (w *WindowBuffer) Values() []interface{} {
    return w.values[w.start:w.end]
}
```

## Examples

### Running Total

```go
// Calculate cumulative sales by region
df.WithColumn("cumulative_sales",
    golars.Col("sales").Sum().Over(
        golars.Window().
            PartitionBy("region").
            OrderBy("date").
            UnboundedPreceding().CurrentRow(),
    ))
```

### Moving Average

```go
// 7-day moving average of stock prices
df.WithColumn("ma7",
    golars.Col("close_price").Mean().Over(
        golars.Window().
            OrderBy("date").
            RowsBetween(-6, 0),
    ))

// 30-day moving average excluding current day
df.WithColumn("ma30_prev",
    golars.Col("close_price").Mean().Over(
        golars.Window().
            OrderBy("date").
            RowsBetween(-30, -1),
    ))
```

### Ranking Within Groups

```go
// Rank products by sales within each category
df.WithColumn("sales_rank",
    golars.Rank().Over(
        golars.Window().
            PartitionBy("category").
            OrderBy("sales", false), // descending
    ))

// Dense rank for handling ties
df.WithColumn("sales_dense_rank",
    golars.DenseRank().Over(
        golars.Window().
            PartitionBy("category").
            OrderBy("sales", false),
    ))
```

### Year-over-Year Calculations

```go
// Compare with same period last year
df.WithColumn("sales_last_year",
    golars.Col("sales").Lag(365).Over(
        golars.Window().
            PartitionBy("store_id").
            OrderBy("date"),
    )).
WithColumn("yoy_growth",
    (golars.Col("sales") - golars.Col("sales_last_year")) / 
    golars.Col("sales_last_year") * 100)
```

### Session Analysis

```go
// Identify session boundaries (gap > 30 minutes)
df.WithColumn("prev_timestamp",
    golars.Col("timestamp").Lag(1).Over(
        golars.Window().
            PartitionBy("user_id").
            OrderBy("timestamp"),
    )).
WithColumn("new_session",
    golars.When(
        golars.Col("timestamp") - golars.Col("prev_timestamp") > 30 * 60,
    ).Then(1).Otherwise(0)).
WithColumn("session_id",
    golars.Col("new_session").Sum().Over(
        golars.Window().
            PartitionBy("user_id").
            OrderBy("timestamp").
            UnboundedPreceding().CurrentRow(),
    ))
```

## Performance Considerations

### Optimization Strategies

1. **Partition Pruning**: Skip partitions that don't match filter criteria
2. **Lazy Materialization**: Don't compute window results until needed
3. **Shared Sorting**: Reuse sort order across multiple window functions
4. **Vectorized Operations**: Use Arrow's columnar format for efficient computation
5. **Parallel Processing**: Process independent partitions concurrently

### Memory Usage Patterns

```go
// Memory-efficient processing for large partitions
func processLargePartition(partition WindowPartition, windowFunc WindowFunction) (series.Series, error) {
    const chunkSize = 10000
    
    results := make([]series.Series, 0)
    
    for start := 0; start < partition.Size(); start += chunkSize {
        end := min(start + chunkSize, partition.Size())
        
        // Process chunk
        chunkResult := windowFunc.ComputeChunk(partition, start, end)
        results = append(results, chunkResult)
    }
    
    // Concatenate results
    return series.Concat(results...)
}
```

### Common Optimizations

```go
// Optimize fixed-size rolling windows
type RollingSum struct {
    windowSize int
    buffer     *WindowBuffer
}

func (r *RollingSum) Compute(partition WindowPartition) (series.Series, error) {
    series := partition.Series()
    result := make([]float64, series.Len())
    
    // Use running sum for efficiency
    sum := 0.0
    for i := 0; i < series.Len(); i++ {
        // Add new value
        if i < series.Len() {
            sum += series.Get(i).(float64)
        }
        
        // Remove old value
        if i >= r.windowSize {
            sum -= series.Get(i - r.windowSize).(float64)
        }
        
        result[i] = sum
    }
    
    return golars.NewFloat64Series("rolling_sum", result), nil
}
```

## Testing Strategy

### Unit Tests

```go
// Test each window function independently
func TestRowNumber(t *testing.T) {
    // Test data
    df := createTestDataFrame()
    
    // Apply row_number
    result := df.WithColumn("rn",
        golars.RowNumber().Over(
            golars.Window().
                PartitionBy("group").
                OrderBy("value"),
        ))
    
    // Verify results
    expected := []int{1, 2, 3, 1, 2, 3}
    actual := result.Column("rn").ToSlice().([]int)
    assert.Equal(t, expected, actual)
}
```

### Integration Tests

```go
// Test complex window operations
func TestMultipleWindowFunctions(t *testing.T) {
    df := loadSalesData()
    
    result := df.
        WithColumn("rank", golars.Rank().Over(salesWindow)).
        WithColumn("running_total", golars.Col("amount").Sum().Over(salesWindow)).
        WithColumn("pct_of_total", 
            golars.Col("amount") / golars.Col("amount").Sum().Over(partitionWindow) * 100)
    
    // Verify all window calculations
    verifyRanking(t, result)
    verifyRunningTotals(t, result)
    verifyPercentages(t, result)
}
```

### Performance Benchmarks

```go
func BenchmarkWindowFunctions(b *testing.B) {
    sizes := []int{1000, 10000, 100000, 1000000}
    
    for _, size := range sizes {
        df := generateDataFrame(size)
        
        b.Run(fmt.Sprintf("RowNumber_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = df.WithColumn("rn", golars.RowNumber().Over(basicWindow))
            }
        })
        
        b.Run(fmt.Sprintf("RollingMean_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = df.WithColumn("ma", golars.Col("value").Mean().Over(rollingWindow))
            }
        })
    }
}
```

### Edge Cases

1. **Empty Windows**: Test behavior with empty partitions
2. **Null Handling**: Verify null propagation in window functions
3. **Boundary Conditions**: Test frame boundaries at partition edges
4. **Large Windows**: Test with window size > partition size
5. **Single Row Partitions**: Ensure correct behavior with single-row groups
6. **Type Safety**: Verify type checking and conversions

## Implementation Notes

### Lazy Evaluation Support

Window functions should integrate with the lazy evaluation framework:

```go
// Add window node to logical plan
type WindowNode struct {
    input   LogicalPlan
    columns map[string]WindowExpr
}

func (n *WindowNode) Schema() (*datatypes.Schema, error) {
    inputSchema, err := n.input.Schema()
    if err != nil {
        return nil, err
    }
    
    // Add window columns to schema
    fields := inputSchema.Fields
    for name, expr := range n.columns {
        fields = append(fields, datatypes.Field{
            Name:     name,
            DataType: expr.DataType(),
        })
    }
    
    return datatypes.NewSchema(fields...), nil
}
```

### Error Handling

Consistent error handling throughout:

```go
// Common error types
var (
    ErrInvalidWindowSpec = errors.New("invalid window specification")
    ErrNoOrderBy        = errors.New("window function requires ORDER BY")
    ErrInvalidFrame     = errors.New("invalid frame specification")
)

// Validation in window functions
func (f *RankFunction) Validate(spec WindowSpec) error {
    if len(spec.orderBy) == 0 {
        return fmt.Errorf("%w: rank() requires ORDER BY clause", ErrNoOrderBy)
    }
    return nil
}
```

## Conclusion

Window functions are a powerful addition to Golars that enable sophisticated analytical queries while maintaining the library's focus on performance and usability. This design provides:

1. **Intuitive API** that follows SQL window function patterns
2. **Efficient Implementation** leveraging Arrow's columnar format
3. **Extensibility** for adding new window functions
4. **Integration** with existing DataFrame and expression systems
5. **Performance** through careful memory management and optimization

The phased implementation approach allows for incremental development while ensuring each component is thoroughly tested before moving to the next phase.