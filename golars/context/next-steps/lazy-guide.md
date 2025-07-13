# Lazy Evaluation Framework Implementation Guide

## Overview

Implement lazy evaluation to:
- Optimize query execution
- Enable query planning and optimization
- Reduce memory usage
- Allow for parallel execution
- Support out-of-core operations

## Architecture

### Core Components

```go
// golars/lazy/frame.go
package lazy

type LazyFrame struct {
    plan        LogicalPlan
    optimizers  []Optimizer
    mu          sync.RWMutex
}

// golars/lazy/plan.go
type LogicalPlan interface {
    Schema() (*datatypes.Schema, error)
    Children() []LogicalPlan
    WithChildren(children []LogicalPlan) LogicalPlan
    String() string
}

// Node types
type ScanNode struct {
    source   DataSource
    columns  []string
    filters  []expr.Expr
}

type FilterNode struct {
    input     LogicalPlan
    predicate expr.Expr
}

type ProjectNode struct {
    input   LogicalPlan
    exprs   []expr.Expr
    aliases []string
}

type GroupByNode struct {
    input    LogicalPlan
    keys     []expr.Expr
    aggs     []expr.Expr
}

type JoinNode struct {
    left     LogicalPlan
    right    LogicalPlan
    on       []string
    how      JoinType
}

type SortNode struct {
    input   LogicalPlan
    by      []expr.Expr
    reverse []bool
}
```

## Implementation Steps

### 1. LazyFrame Creation

```go
// frame/dataframe.go
func (df *DataFrame) Lazy() *lazy.LazyFrame {
    return lazy.NewLazyFrame(lazy.NewDataFrameScan(df))
}

// io/csv/reader.go
func ScanCSV(path string, options CSVReadOptions) *lazy.LazyFrame {
    return lazy.NewLazyFrame(lazy.NewCSVScan(path, options))
}

// lazy/frame.go
func NewLazyFrame(plan LogicalPlan) *LazyFrame {
    return &LazyFrame{
        plan: plan,
        optimizers: []Optimizer{
            NewProjectionPushdown(),
            NewPredicatePushdown(),
            NewCommonSubexpressionElimination(),
            NewColumnPruning(),
        },
    }
}
```

### 2. Lazy Operations

```go
func (lf *LazyFrame) Select(exprs ...expr.Expr) *LazyFrame {
    lf.mu.Lock()
    defer lf.mu.Unlock()
    
    aliases := make([]string, len(exprs))
    for i, e := range exprs {
        aliases[i] = e.Name()
    }
    
    return &LazyFrame{
        plan: &ProjectNode{
            input:   lf.plan,
            exprs:   exprs,
            aliases: aliases,
        },
        optimizers: lf.optimizers,
    }
}

func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
    lf.mu.Lock()
    defer lf.mu.Unlock()
    
    return &LazyFrame{
        plan: &FilterNode{
            input:     lf.plan,
            predicate: predicate,
        },
        optimizers: lf.optimizers,
    }
}

func (lf *LazyFrame) GroupBy(keys ...string) *LazyGroupBy {
    keyExprs := make([]expr.Expr, len(keys))
    for i, k := range keys {
        keyExprs[i] = expr.Col(k)
    }
    
    return &LazyGroupBy{
        lf:   lf,
        keys: keyExprs,
    }
}

func (lf *LazyFrame) Join(other *LazyFrame, on string, how JoinType) *LazyFrame {
    return &LazyFrame{
        plan: &JoinNode{
            left:  lf.plan,
            right: other.plan,
            on:    []string{on},
            how:   how,
        },
        optimizers: lf.optimizers,
    }
}

func (lf *LazyFrame) Sort(by string, reverse bool) *LazyFrame {
    return &LazyFrame{
        plan: &SortNode{
            input:   lf.plan,
            by:      []expr.Expr{expr.Col(by)},
            reverse: []bool{reverse},
        },
        optimizers: lf.optimizers,
    }
}
```

### 3. Query Optimization

```go
// lazy/optimizer.go
type Optimizer interface {
    Optimize(plan LogicalPlan) (LogicalPlan, error)
}

// Predicate Pushdown
type PredicatePushdown struct{}

func (opt *PredicatePushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
    return opt.pushDown(plan, nil)
}

func (opt *PredicatePushdown) pushDown(plan LogicalPlan, predicates []expr.Expr) (LogicalPlan, error) {
    switch node := plan.(type) {
    case *FilterNode:
        // Accumulate predicates
        predicates = append(predicates, node.predicate)
        optimized, err := opt.pushDown(node.input, predicates)
        if err != nil {
            return nil, err
        }
        
        // If all predicates were pushed down, remove this node
        if len(predicates) == 0 {
            return optimized, nil
        }
        
        return &FilterNode{
            input:     optimized,
            predicate: combinePredicates(predicates),
        }, nil
        
    case *ScanNode:
        // Push predicates into scan
        node.filters = append(node.filters, predicates...)
        return node, nil
        
    case *ProjectNode:
        // Try to push through projection
        pushed, remaining := opt.splitPredicates(predicates, node.exprs)
        
        optimized, err := opt.pushDown(node.input, pushed)
        if err != nil {
            return nil, err
        }
        
        node.input = optimized
        
        if len(remaining) > 0 {
            return &FilterNode{
                input:     node,
                predicate: combinePredicates(remaining),
            }, nil
        }
        
        return node, nil
        
    case *JoinNode:
        // Split predicates by join side
        leftPreds, rightPreds, joinPreds := opt.splitJoinPredicates(predicates, node)
        
        left, err := opt.pushDown(node.left, leftPreds)
        if err != nil {
            return nil, err
        }
        
        right, err := opt.pushDown(node.right, rightPreds)
        if err != nil {
            return nil, err
        }
        
        node.left = left
        node.right = right
        
        if len(joinPreds) > 0 {
            return &FilterNode{
                input:     node,
                predicate: combinePredicates(joinPreds),
            }, nil
        }
        
        return node, nil
        
    default:
        // Can't push through this node
        if len(predicates) > 0 {
            return &FilterNode{
                input:     plan,
                predicate: combinePredicates(predicates),
            }, nil
        }
        return plan, nil
    }
}

// Projection Pushdown
type ProjectionPushdown struct{}

func (opt *ProjectionPushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
    // Find all columns needed at the top
    needed := opt.collectNeededColumns(plan)
    
    // Push down projections
    return opt.pushDown(plan, needed)
}

// Common Subexpression Elimination
type CommonSubexpressionElimination struct{}

func (opt *CommonSubexpressionElimination) Optimize(plan LogicalPlan) (LogicalPlan, error) {
    // Find common expressions
    exprMap := make(map[string]expr.Expr)
    aliases := make(map[expr.Expr]string)
    
    plan = opt.replaceCommonExpressions(plan, exprMap, aliases)
    
    return plan, nil
}
```

### 4. Query Execution

```go
// lazy/executor.go
type Executor struct {
    concurrency int
}

func (lf *LazyFrame) Collect() (*frame.DataFrame, error) {
    lf.mu.RLock()
    defer lf.mu.RUnlock()
    
    // Optimize plan
    optimized := lf.plan
    for _, optimizer := range lf.optimizers {
        var err error
        optimized, err = optimizer.Optimize(optimized)
        if err != nil {
            return nil, fmt.Errorf("optimization failed: %w", err)
        }
    }
    
    // Create physical plan
    physical := createPhysicalPlan(optimized)
    
    // Execute
    executor := &Executor{concurrency: runtime.NumCPU()}
    return executor.Execute(physical)
}

func (e *Executor) Execute(plan PhysicalPlan) (*frame.DataFrame, error) {
    switch node := plan.(type) {
    case *PhysicalScan:
        return e.executeScan(node)
        
    case *PhysicalFilter:
        input, err := e.Execute(node.input)
        if err != nil {
            return nil, err
        }
        return input.Filter(node.predicate)
        
    case *PhysicalProject:
        input, err := e.Execute(node.input)
        if err != nil {
            return nil, err
        }
        return e.executeProject(input, node.exprs)
        
    case *PhysicalHashJoin:
        // Execute both sides in parallel
        var left, right *frame.DataFrame
        var leftErr, rightErr error
        
        var wg sync.WaitGroup
        wg.Add(2)
        
        go func() {
            defer wg.Done()
            left, leftErr = e.Execute(node.left)
        }()
        
        go func() {
            defer wg.Done()
            right, rightErr = e.Execute(node.right)
        }()
        
        wg.Wait()
        
        if leftErr != nil {
            return nil, leftErr
        }
        if rightErr != nil {
            return nil, rightErr
        }
        
        return left.Join(right, node.on[0], node.how)
        
    default:
        return nil, fmt.Errorf("unknown physical plan node")
    }
}
```

### 5. Streaming Execution

```go
// lazy/streaming.go
type StreamingExecutor struct {
    batchSize int
}

func (lf *LazyFrame) Stream(batchSize int) (*DataFrameStream, error) {
    // Create streaming physical plan
    plan := createStreamingPlan(lf.plan, batchSize)
    
    return &DataFrameStream{
        plan:      plan,
        batchSize: batchSize,
    }, nil
}

type DataFrameStream struct {
    plan      StreamingPlan
    batchSize int
    current   *frame.DataFrame
}

func (s *DataFrameStream) Next() (*frame.DataFrame, error) {
    batch, err := s.plan.NextBatch()
    if err != nil {
        return nil, err
    }
    
    if batch == nil {
        return nil, io.EOF
    }
    
    return batch, nil
}
```

## Usage Examples

```go
// Basic lazy operations
df := golars.ScanCSV("large_file.csv").
    Filter(golars.Col("age").Gt(25)).
    Select(
        golars.Col("name"),
        golars.Col("age"),
        golars.Col("salary").Mul(1.1).Alias("adjusted_salary"),
    ).
    Sort("salary", true).
    Collect()

// Complex query with optimization
result := golars.ScanParquet("sales.parquet").
    Filter(golars.Col("year").Eq(2023)).
    Join(
        golars.ScanCSV("products.csv"),
        "product_id",
        InnerJoin,
    ).
    GroupBy("category").
    Agg(
        golars.Col("amount").Sum().Alias("total_sales"),
        golars.Col("amount").Mean().Alias("avg_sale"),
    ).
    Sort("total_sales", true).
    Limit(10).
    Collect()

// Streaming large dataset
stream, _ := golars.ScanCSV("huge_file.csv").
    Filter(golars.Col("active").Eq(true)).
    Stream(10000)  // 10k rows per batch

for {
    batch, err := stream.Next()
    if err == io.EOF {
        break
    }
    // Process batch
}

// Query explanation
plan := df.Lazy().
    Filter(expr).
    Select(cols...).
    Explain()  // Returns optimized plan string
```

## Optimization Examples

### Before Optimization
```
Project [name, age * 2]
  Filter [age > 25]
    Filter [active = true]
      Scan CSV [all columns]
```

### After Optimization
```
Project [name, age]  // age * 2 computed here
  Scan CSV [name, age, active]  // Only needed columns
    Filters: [age > 25 AND active = true]  // Pushed down
```

## Performance Benefits

1. **Predicate Pushdown**: Filter early, scan less data
2. **Projection Pushdown**: Read only needed columns
3. **Lazy Evaluation**: No intermediate DataFrames
4. **Query Planning**: Optimal join order, filter placement
5. **Parallelism**: Execute independent operations concurrently
6. **Memory Efficiency**: Process in batches, not all at once

## Testing

```go
func TestLazyFrame(t *testing.T) {
    // Create test data
    df := createTestDataFrame()
    
    // Test lazy operations produce same result
    eager := df.Filter(expr).Select("a", "b")
    lazy := df.Lazy().Filter(expr).Select("a", "b").Collect()
    
    assert.Equal(t, eager, lazy)
    
    // Test optimization
    plan := df.Lazy().
        Filter(golars.Col("a").Gt(5)).
        Filter(golars.Col("b").Lt(10)).
        plan
    
    optimized := NewPredicatePushdown().Optimize(plan)
    
    // Should combine filters
    assert.IsType(t, &FilterNode{}, optimized)
    filter := optimized.(*FilterNode)
    assert.IsType(t, &BinaryExpr{}, filter.predicate)  // AND expression
}

func BenchmarkLazyVsEager(b *testing.B) {
    df := createLargeDataFrame(1000000)
    
    b.Run("Eager", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            _ = df.Filter(expr1).Filter(expr2).Select(cols...)
        }
    })
    
    b.Run("Lazy", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            _ = df.Lazy().Filter(expr1).Filter(expr2).Select(cols...).Collect()
        }
    })
}
```

## Future Enhancements

1. **More Optimizations**: Constant folding, join reordering
2. **Caching**: Cache intermediate results
3. **Cost-Based Optimization**: Use statistics for better plans
4. **Distributed Execution**: Execute on multiple machines
5. **GPU Acceleration**: Offload compute to GPU
6. **Incremental Computation**: Update results as data changes