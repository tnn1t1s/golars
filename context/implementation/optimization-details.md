# Query Optimization Implementation Details

## Overview

Golars implements a query optimization framework that automatically improves the performance of lazy queries. The optimizer transforms logical query plans to reduce data movement and computation.

## Architecture

### Core Components

1. **Optimizer Interface** (`lazy/optimizer.go`)
```go
type Optimizer interface {
    Optimize(plan LogicalPlan) (LogicalPlan, error)
}
```

2. **Logical Plan Nodes** (`lazy/plan.go`)
- ScanNode: Reads data from source
- FilterNode: Applies predicates
- ProjectNode: Selects columns
- GroupByNode: Groups and aggregates
- JoinNode: Combines DataFrames
- SortNode: Orders data
- LimitNode: Limits rows

3. **Optimization Pipeline** (`lazy/frame.go`)
```go
optimizers: []Optimizer{
    NewPredicatePushdown(),
    NewProjectionPushdown(),
}
```

## Implemented Optimizations

### 1. Predicate Pushdown

**Purpose**: Push filters as close to the data source as possible to reduce data movement.

**Implementation**:
```go
func (opt *PredicatePushdown) pushDown(plan LogicalPlan, predicates []expr.Expr) (LogicalPlan, error) {
    switch node := plan.(type) {
    case *FilterNode:
        // Accumulate predicates
        predicates = append(predicates, node.predicate)
        return opt.pushDown(node.input, predicates)
        
    case *ScanNode:
        // Push predicates into scan
        if len(predicates) > 0 {
            newNode := &ScanNode{
                source:  node.source,
                columns: node.columns,
                filters: append(node.filters, predicates...),
            }
            return newNode, nil
        }
    // ... handle other nodes
    }
}
```

**Transformations**:
- Pushes through: Project, Sort, Limit
- Stops at: GroupBy, Join (cannot push past aggregations)
- Combines multiple filters with AND

**Example**:
```
Before: Limit -> Filter(b>10) -> Sort -> Filter(a>5) -> Scan
After:  Limit -> Sort -> Scan[filters: (a>5) AND (b>10)]
```

### 2. Projection Pushdown

**Purpose**: Read only the columns that are actually needed by the query.

**Implementation**:
```go
func (opt *ProjectionPushdown) pushDown(plan LogicalPlan, neededColumns map[string]bool) (LogicalPlan, error) {
    switch node := plan.(type) {
    case *ScanNode:
        // Restrict columns to only those needed
        if node.columns == nil {
            columns := make([]string, 0, len(neededColumns))
            for col := range neededColumns {
                columns = append(columns, col)
            }
            return &ScanNode{
                source:  node.source,
                columns: columns,
                filters: node.filters,
            }, nil
        }
    // ... handle other nodes
    }
}
```

**Column Collection**:
```go
func (opt *ProjectionPushdown) collectExprColumns(expr expr.Expr, needed map[string]bool) {
    exprStr := expr.String()
    // Extract all column references from expression
    // Handles: col(name), col(name).sum(), complex expressions
}
```

**Special Handling**:
- Preserves columns needed by filters in subtree
- Handles aggregation expressions correctly
- Recursive filter column collection

**Example**:
```
Query: df.Filter(store=="NY").GroupBy("product").Sum("quantity").Select("product", "quantity_sum")
Scan reads only: product, quantity, store (not other columns)
```

### 3. Expression Optimization

**Purpose**: Combine and simplify expressions for efficient evaluation.

**Implementation**:
```go
func combinePredicates(predicates []expr.Expr) expr.Expr {
    if len(predicates) <= 1 {
        return predicates[0]
    }
    
    // Combine with AND using builder pattern
    result := predicates[0]
    for i := 1; i < len(predicates); i++ {
        builder := expr.NewBuilder(result)
        result = builder.And(predicates[i]).Build()
    }
    return result
}
```

**Optimizations**:
- Combines multiple filters into single AND expression
- Reduces evaluation overhead
- Enables efficient pushdown

## Implementation Patterns

### 1. Visitor Pattern
Each optimizer visits plan nodes recursively:
```go
func (opt *Optimizer) Optimize(plan LogicalPlan) (LogicalPlan, error) {
    return opt.visit(plan, context)
}
```

### 2. Immutable Transformations
Plans are never modified in place:
```go
// Create new node with modifications
newNode := &FilterNode{
    input:     optimizedInput,
    predicate: node.predicate,
}
```

### 3. Bottom-Up Processing
Most optimizations process children first:
```go
// Optimize input first
optimizedInput, err := opt.pushDown(node.input, predicates)
// Then handle current node
return &ProjectNode{input: optimizedInput, ...}
```

## Performance Characteristics

### Overhead
- Predicate pushdown: ~286ns per optimization
- Projection pushdown: ~2.7μs per optimization
- Negligible compared to query execution time

### Benefits
- Reduced data scanning (predicate pushdown)
- Reduced memory usage (projection pushdown)
- Fewer intermediate materializations
- Combined filter evaluation

## Testing Strategy

### Unit Tests
Each optimizer has comprehensive tests:
```go
func TestPredicatePushdown_ThroughProjection(t *testing.T)
func TestProjectionPushdown_FilterBelowGroupBy(t *testing.T)
```

### Integration Tests
Combined optimizations:
```go
func TestOptimizers_Combined(t *testing.T)
```

### Benchmarks
Performance validation:
```go
func BenchmarkOptimization_PredicatePushdown(b *testing.B)
func BenchmarkOptimization_ProjectionPushdown(b *testing.B)
```

## Known Limitations

1. **No Cost-Based Optimization**
   - Rules-based only
   - No statistics collection
   - Fixed optimization order

2. **Limited Pushdown**
   - Cannot push past aggregations
   - No join reordering
   - No subquery optimization

3. **Expression Limitations**
   - No constant folding
   - No common subexpression elimination
   - No algebraic simplification

## Future Enhancements

### Near Term
1. **Common Subexpression Elimination**
   - Detect repeated expressions
   - Compute once, reuse results

2. **Constant Folding**
   - Evaluate constant expressions at plan time
   - Simplify expressions like `lit(5) + lit(10)` to `lit(15)`

3. **Filter Ordering**
   - Order filters by selectivity
   - Apply most selective filters first

### Long Term
1. **Cost-Based Optimization**
   - Collect table statistics
   - Estimate operation costs
   - Choose optimal plan based on cost

2. **Join Optimization**
   - Join reordering based on size
   - Broadcast joins for small tables
   - Hash join vs sort-merge join selection

3. **Physical Plan Generation**
   - Separate logical and physical planning
   - Choose execution strategies
   - Parallel execution planning

## Integration Points

### With LazyFrame
```go
// In NewLazyFrame
optimizers: []Optimizer{
    NewPredicatePushdown(),
    NewProjectionPushdown(),
}

// In Collect
for _, optimizer := range lf.optimizers {
    optimized, err = optimizer.Optimize(optimized)
}
```

### With Executor
Optimized plans are executed normally:
```go
executor := NewExecutor()
return executor.Execute(optimizedPlan)
```

## Debugging

### Plan Inspection
```go
// View unoptimized plan
fmt.Println(lf.Explain())

// View optimized plan
optimized, _ := lf.ExplainOptimized()
fmt.Println(optimized)
```

### Example Output
```
Unoptimized:
Limit [10]
  Filter [(col(price) < lit(100))]
    Project [col(product), col(price)]
      Filter [(col(category) == lit(electronics))]
        Scan DataFrame[100 × 5]

Optimized:
Limit [10]
  Project [col(product), col(price)]
    Scan DataFrame[100 × 5]
      Columns: [product, price, category]
      Filters: (col(category) == lit(electronics)) AND (col(price) < lit(100))
```

## Best Practices

1. **Enable All Optimizers**: They work together for best results
2. **Use Lazy Evaluation**: For complex queries with multiple operations
3. **Inspect Plans**: Use Explain() to understand optimization
4. **Profile Results**: Measure actual performance improvements

## Conclusion

The query optimization framework in Golars provides significant performance improvements with minimal overhead. The modular design allows for easy extension with new optimizations while maintaining correctness through comprehensive testing.