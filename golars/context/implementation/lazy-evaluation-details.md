# Lazy Evaluation Implementation Details

## Overview

The lazy evaluation framework has been implemented to allow for query optimization and efficient execution of DataFrame operations.

## Architecture

### Core Components

1. **LogicalPlan Interface** (`lazy/plan.go`)
   - Base interface for all plan nodes
   - Methods: Schema(), Children(), WithChildren(), String()
   - Node types: ScanNode, FilterNode, ProjectNode, GroupByNode, JoinNode, SortNode, LimitNode

2. **LazyFrame** (`lazy/frame.go`)
   - Represents a lazy computation on a DataFrame
   - Thread-safe with read/write mutex
   - Stores logical plan and optimizers
   - Methods mirror DataFrame operations but build a plan instead of executing

3. **Executor** (`lazy/executor.go`)
   - Executes logical plans to produce DataFrames
   - Handles each node type with appropriate execution logic
   - Special handling for GroupBy with named aggregations

4. **Optimizer Interface** (`lazy/optimizer.go`)
   - Base interface for query optimizers
   - Implemented: PredicatePushdown (basic)
   - Placeholder for: ProjectionPushdown, CommonSubexpressionElimination

### Key Features

1. **Lazy Operations**
   - Select/SelectColumns - Column projection
   - Filter - Row filtering with predicates
   - GroupBy with aggregations (Sum, Mean, Min, Max, Count)
   - Join operations (all types)
   - Sort/SortBy - Single and multi-column sorting
   - Limit/Head - Row limiting

2. **Query Planning**
   - Builds a tree of logical plan nodes
   - Each operation adds a node to the plan
   - Plan can be inspected with Explain()
   - Optimized plan available via ExplainOptimized()

3. **Execution**
   - Collect() optimizes and executes the plan
   - Recursive execution of plan nodes
   - Efficient reuse of existing DataFrame operations

## Implementation Details

### Expression Handling
- Column expressions represented as "col(name)"
- Aggregation expressions as "col(name).sum()" etc.
- Helper function getExprName() extracts column names from expressions

### GroupBy Aggregations
- GroupByNode stores explicit aggregation names
- LazyGroupBy.Agg() preserves the mapping from names to expressions
- Executor uses executeGroupByWithNames() to maintain naming

### Type Safety
- Uses generics where possible
- Type erasure at Series interface boundary
- Consistent with existing DataFrame patterns

### Thread Safety
- LazyFrame protected by sync.RWMutex
- Immutable operations (new LazyFrame for each operation)
- Safe for concurrent use

## Usage Examples

```go
// Basic lazy operations
df := golars.LazyFromDataFrame(existingDF).
    Filter(expr.ColBuilder("age").Gt(25).Build()).
    SelectColumns("name", "age", "salary").
    Sort("salary", true).
    Limit(10).
    Collect()

// Complex query with grouping
result := golars.ScanCSV("data.csv").
    Filter(expr.ColBuilder("active").Eq(expr.Lit(true)).Build()).
    GroupBy("department").
    Agg(map[string]expr.Expr{
        "avg_salary": expr.ColBuilder("salary").Mean().Build(),
        "count": expr.ColBuilder("").Count().Build(),
    }).
    Sort("avg_salary", true).
    Collect()
```

## Testing

Comprehensive test suite in `lazy/frame_test.go`:
- Basic operations (select, filter, sort)
- Chained operations
- GroupBy with aggregations
- Multi-column sorting
- Query explanation
- Clone functionality
- Benchmarks comparing lazy vs eager evaluation

All tests passing ✅

## Limitations

1. **Optimizer Coverage**
   - Only basic predicate pushdown implemented
   - No projection pushdown or CSE yet
   - No cost-based optimization

2. **Expression Evaluation**
   - Limited to column selections for projections
   - Full expression evaluation not implemented

3. **Streaming**
   - No streaming execution yet
   - All operations materialize full results

4. **CSV Scanning**
   - CSV schema inference not implemented in lazy mode
   - Falls back to eager reading

## Integration Points

- DataFrame.Lazy() method avoided due to circular imports
- Helper function golars.LazyFromDataFrame() provided instead
- golars.ScanCSV() for lazy CSV reading
- Reuses existing DataFrame operations for execution

## Performance

- Lazy evaluation avoids intermediate DataFrame creation
- Query optimization can reduce data movement
- Benchmarks show performance improvements for chained operations
- Greatest benefit when filtering early in the pipeline

## Future Enhancements

1. **More Optimizers**
   - Projection pushdown
   - Common subexpression elimination
   - Join reordering
   - Constant folding

2. **Streaming Execution**
   - Process data in batches
   - Support for out-of-core operations

3. **Better Expression Support**
   - Full expression evaluation in projections
   - Computed columns
   - Window functions

4. **Physical Planning**
   - Convert logical plan to physical plan
   - Choose optimal algorithms (hash join vs sort-merge join)
   - Parallel execution strategies