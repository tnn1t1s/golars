# Window Functions Package

This package implements SQL-style window functions for the Golars DataFrame library.

## Features

### Ranking Functions
- `RowNumber()` - Assigns sequential numbers to rows
- `Rank()` - Assigns ranks with gaps for ties
- `DenseRank()` - Assigns ranks without gaps
- `PercentRank()` - Calculates relative rank (0-1)
- `NTile(n)` - Divides rows into n buckets

### Aggregate Functions
- `Sum(column)` - Sum over window frame
- `Avg(column)` - Average over window frame
- `Min(column)` - Minimum over window frame
- `Max(column)` - Maximum over window frame
- `Count(column)` - Count over window frame

### Offset Functions
- `Lag(column, offset, default)` - Access previous rows
- `Lead(column, offset, default)` - Access following rows
- `FirstValue(column)` - First value in window
- `LastValue(column)` - Last value in window

## Usage

```go
import (
    "github.com/tnn1t1s/golars"
    "github.com/tnn1t1s/golars/dataframe"
    "github.com/tnn1t1s/golars/expr"
)

// Create window specification
spec := golars.NewSpec().
    PartitionBy("department").
    OrderBy("salary", false)

// Apply window function
df = df.WithColumn("salary_rank",
    expr.WindowFunc(golars.Rank().Over(spec)),
)
```

## Architecture

### Key Components

1. **WindowSpec** (`spec.go`)
   - Defines partitioning, ordering, and frame boundaries
   - Fluent API for specification building

2. **WindowPartition** (`partition.go`)
   - Manages data within a partition
   - Handles ordering and frame calculations

3. **Function Interface** (`interface.go`)
   - Common interface for all window functions
   - Validates specifications and computes results

4. **Expression Integration** (`expr.go`)
   - Integrates with Golars expression system
   - Enables use in DataFrame operations

### Implementation Details

#### Partitioning
Data is partitioned by specified columns, creating independent groups for window calculations.

#### Ordering
Within each partition, rows can be ordered by one or more columns in ascending or descending order.

#### Frame Specifications
- **ROWS**: Physical offset from current row
- **Default frames**:
  - Ordered: UNBOUNDED PRECEDING to CURRENT ROW
  - Unordered: UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING

## Testing

Run tests with:
```bash
go test ./window -v
```

Check coverage:
```bash
go test ./window -cover
```

## Performance Considerations

1. **Memory**: Window functions materialize partition data
2. **Ordering**: Adds computational overhead
3. **Partitioning**: Smaller partitions generally perform better
4. **Frame Size**: Smaller frames are more efficient

## Future Enhancements

- RANGE frames with value-based boundaries
- GROUPS frame type
- Custom aggregate functions
- Parallel partition processing
- More offset functions (NTH_VALUE)