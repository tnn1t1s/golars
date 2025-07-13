# Optimizer Implementation Patterns

## Overview
This document captures the patterns used in implementing query optimizers for Golars, which can be reused for future optimizers.

## Core Patterns

### 1. Optimizer Interface
```go
type Optimizer interface {
    Optimize(plan LogicalPlan) (LogicalPlan, error)
}
```

### 2. Recursive Tree Traversal
Most optimizers follow this pattern:
```go
func (opt *OptimizerName) optimize(plan LogicalPlan, context Context) (LogicalPlan, error) {
    switch node := plan.(type) {
    case *SpecificNode:
        // Handle specific node type
        // Usually recurse on children first
        optimizedChild, err := opt.optimize(node.input, newContext)
        if err != nil {
            return nil, err
        }
        // Transform current node
        return &SpecificNode{
            input: optimizedChild,
            // ... modified fields
        }, nil
    
    default:
        // For unhandled nodes, optimize children
        children := plan.Children()
        if len(children) > 0 {
            optimizedChildren := make([]LogicalPlan, len(children))
            for i, child := range children {
                optimized, err := opt.optimize(child, context)
                if err != nil {
                    return nil, err
                }
                optimizedChildren[i] = optimized
            }
            return plan.WithChildren(optimizedChildren), nil
        }
        return plan, nil
    }
}
```

### 3. Context Accumulation
For pushdown optimizers, accumulate context while traversing:
```go
// Predicate pushdown accumulates filters
func (opt *PredicatePushdown) pushDown(plan LogicalPlan, predicates []expr.Expr) (LogicalPlan, error) {
    case *FilterNode:
        // Accumulate predicate
        predicates = append(predicates, node.predicate)
        // Continue pushing down
        return opt.pushDown(node.input, predicates)
```

### 4. Bottom-Up Collection
For pullup optimizers, collect information from children first:
```go
func (opt *ProjectionPushdown) collectNeededColumns(plan LogicalPlan) map[string]bool {
    needed := make(map[string]bool)
    
    switch node := plan.(type) {
    case *ProjectNode:
        // Collect from this node's expressions
        for _, expr := range node.exprs {
            opt.collectExprColumns(expr, needed)
        }
    }
    return needed
}
```

### 5. Expression Analysis
Since expressions are opaque, use string parsing:
```go
func (opt *Optimizer) collectExprColumns(expr expr.Expr, needed map[string]bool) {
    exprStr := expr.String()
    
    // Find all column references
    start := 0
    for {
        idx := strings.Index(exprStr[start:], "col(")
        if idx == -1 {
            break
        }
        idx += start
        
        // Find matching closing paren
        depth := 0
        for i := idx + 4; i < len(exprStr); i++ {
            if exprStr[i] == '(' {
                depth++
            } else if exprStr[i] == ')' {
                if depth == 0 {
                    colName := exprStr[idx+4:i]
                    needed[colName] = true
                    break
                }
                depth--
            }
        }
        start = idx + 4
    }
}
```

### 6. Immutable Transformations
Never modify nodes in place:
```go
// Wrong
node.filters = newFilters

// Correct
return &ScanNode{
    source:  node.source,
    columns: node.columns,
    filters: newFilters,
}
```

### 7. Recursive Helper Collection
For complex analysis, use recursive helpers:
```go
func (opt *Optimizer) collectFilterColumnsRecursive(plan LogicalPlan, needed map[string]bool) {
    switch node := plan.(type) {
    case *FilterNode:
        opt.collectExprColumns(node.predicate, needed)
        opt.collectFilterColumnsRecursive(node.input, needed)
        
    case *ScanNode:
        for _, filter := range node.filters {
            opt.collectExprColumns(filter, needed)
        }
        
    default:
        for _, child := range plan.Children() {
            opt.collectFilterColumnsRecursive(child, needed)
        }
    }
}
```

## Common Optimizer Types

### 1. Pushdown Optimizers
Push operations closer to data source:
- Predicate pushdown
- Projection pushdown
- Limit pushdown

Pattern: Accumulate context while traversing down

### 2. Pullup Optimizers
Pull operations up the tree:
- Projection pullup
- Common subexpression elimination

Pattern: Collect information bottom-up

### 3. Rewrite Optimizers
Transform expressions or plan structure:
- Expression simplification
- Constant folding
- Join reordering

Pattern: Pattern matching and replacement

## Testing Patterns

### 1. Simple Transformation Tests
```go
func TestOptimizer_BasicCase(t *testing.T) {
    // Build plan
    plan := NewFilterNode(
        NewScanNode(source),
        predicate,
    )
    
    // Optimize
    optimizer := NewOptimizer()
    optimized, err := optimizer.Optimize(plan)
    require.NoError(t, err)
    
    // Verify structure
    scanNode, ok := optimized.(*ScanNode)
    require.True(t, ok)
    assert.Len(t, scanNode.filters, 1)
}
```

### 2. Complex Chain Tests
Test optimizer behavior through multiple nodes:
```go
func TestOptimizer_ThroughMultipleNodes(t *testing.T) {
    plan := NewLimitNode(
        NewSortNode(
            NewFilterNode(
                NewScanNode(source),
                predicate,
            ),
            sortExprs,
        ),
        100,
    )
    // ... verify optimization worked correctly
}
```

### 3. Edge Case Tests
- Empty predicates
- Already optimized plans
- Plans that shouldn't be optimized

## Performance Considerations

1. **Minimize Allocations**: Reuse slices where possible
2. **Early Termination**: Stop when no more optimization possible
3. **Cache Results**: Don't recompute same analysis
4. **Benchmark**: Always benchmark optimizer overhead

## Future Optimizer Ideas

Based on patterns observed:

1. **Common Subexpression Elimination**
   - Pattern: Bottom-up expression collection
   - Cache computed expressions

2. **Constant Folding**
   - Pattern: Expression rewriting
   - Evaluate constant expressions

3. **Join Reordering**
   - Pattern: Cost-based transformation
   - Need statistics collection first

4. **Filter Ordering**
   - Pattern: Selectivity-based reordering
   - Apply most selective filters first

## Integration Checklist

When adding a new optimizer:
1. Implement Optimizer interface
2. Add to default optimizers in LazyFrame
3. Write comprehensive tests
4. Add benchmarks
5. Document in optimization-details.md
6. Add example usage