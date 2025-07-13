# Optimizer Testing Guide

## Overview
This document captures testing strategies and patterns for query optimizers in Golars.

## Test Categories

### 1. Basic Transformation Tests
Verify the optimizer performs its core transformation correctly.

```go
func TestPredicatePushdown_Basic(t *testing.T) {
    // Setup: Filter -> Scan
    scan := NewScanNode(NewDataFrameSource(nil))
    filter := NewFilterNode(scan, expr.ColBuilder("a").Gt(5).Build())
    
    // Act: Optimize
    optimizer := NewPredicatePushdown()
    optimized, err := optimizer.Optimize(filter)
    require.NoError(t, err)
    
    // Assert: Filter pushed to scan
    scanNode, ok := optimized.(*ScanNode)
    require.True(t, ok, "Expected ScanNode at root")
    assert.Len(t, scanNode.filters, 1)
}
```

### 2. Multiple Operations Tests
Test combining multiple operations of the same type.

```go
func TestPredicatePushdown_CombineFilters(t *testing.T) {
    // Multiple filters should be combined with AND
    scan := NewScanNode(source)
    filter1 := NewFilterNode(scan, pred1)
    filter2 := NewFilterNode(filter1, pred2)
    
    optimized, _ := optimizer.Optimize(filter2)
    
    // Check combined predicate
    scanNode := optimized.(*ScanNode)
    assert.Contains(t, scanNode.filters[0].String(), "AND")
}
```

### 3. Pushdown Through Nodes Tests
Verify optimizer correctly pushes through compatible nodes.

```go
func TestPredicatePushdown_ThroughProjection(t *testing.T) {
    // Filter should push through projection
    scan := NewScanNode(source)
    project := NewProjectNode(scan, exprs)
    filter := NewFilterNode(project, predicate)
    
    optimized, _ := optimizer.Optimize(filter)
    
    // Structure: Project -> Scan with filter
    projectNode, ok := optimized.(*ProjectNode)
    require.True(t, ok)
    scanNode, ok := projectNode.input.(*ScanNode)
    require.True(t, ok)
    assert.Len(t, scanNode.filters, 1)
}
```

### 4. Blocking Node Tests
Test that optimization stops at nodes it cannot push past.

```go
func TestPredicatePushdown_StopAtGroupBy(t *testing.T) {
    // Cannot push filter on aggregated column past groupby
    scan := NewScanNode(source)
    groupBy := NewGroupByNode(scan, keys, aggs)
    filter := NewFilterNode(groupBy, aggregatedColumnPredicate)
    
    optimized, _ := optimizer.Optimize(filter)
    
    // Filter should remain above GroupBy
    filterNode, ok := optimized.(*FilterNode)
    require.True(t, ok)
    _, ok = filterNode.input.(*GroupByNode)
    require.True(t, ok)
}
```

### 5. Complex Interaction Tests
Test interactions between different node types.

```go
func TestProjectionPushdown_FilterBelowGroupBy(t *testing.T) {
    // Critical test that caught the bug
    scan := NewScanNode(source)
    filter := NewFilterNode(scan, expr.ColBuilder("store").Eq(lit("B")).Build())
    groupBy := NewGroupByNode(filter, 
        []expr.Expr{expr.Col("product")},
        []expr.Expr{expr.ColBuilder("quantity").Sum().Build()},
    )
    project := NewProjectNode(groupBy, []expr.Expr{
        expr.Col("product"),
        expr.Col("quantity_sum"),
    })
    
    optimized, _ := optimizer.Optimize(project)
    
    // Navigate to scan and verify filter columns preserved
    // ... navigate through nodes ...
    assert.Contains(t, scanNode.columns, "store") // Critical assertion
}
```

### 6. Edge Case Tests

```go
func TestOptimizer_EmptyPlan(t *testing.T) {
    scan := NewScanNode(source)
    optimized, err := optimizer.Optimize(scan)
    require.NoError(t, err)
    assert.Equal(t, scan, optimized) // No change
}

func TestOptimizer_AlreadyOptimized(t *testing.T) {
    // Plan already has filters at scan level
    scan := NewScanNode(source)
    scan.filters = []expr.Expr{predicate}
    
    optimized, _ := optimizer.Optimize(scan)
    assert.Equal(t, scan, optimized)
}
```

## Testing Patterns

### 1. Plan Building Helpers
Create helpers for common plan structures:

```go
func buildBasicPlan() LogicalPlan {
    return NewFilterNode(
        NewProjectNode(
            NewScanNode(testSource),
            testColumns,
        ),
        testPredicate,
    )
}
```

### 2. Assertion Helpers
Create helpers for common assertions:

```go
func assertPlanStructure(t *testing.T, plan LogicalPlan, expected string) {
    actual := plan.String()
    assert.Contains(t, actual, expected)
}

func assertScanFilters(t *testing.T, plan LogicalPlan, expectedCount int) {
    scan := findScanNode(plan)
    require.NotNil(t, scan)
    assert.Len(t, scan.filters, expectedCount)
}
```

### 3. Navigation Helpers
Navigate complex plans:

```go
func findScanNode(plan LogicalPlan) *ScanNode {
    switch node := plan.(type) {
    case *ScanNode:
        return node
    default:
        for _, child := range plan.Children() {
            if scan := findScanNode(child); scan != nil {
                return scan
            }
        }
    }
    return nil
}
```

## Benchmark Tests

### 1. Simple Optimization Benchmark
```go
func BenchmarkPredicatePushdown_Simple(b *testing.B) {
    plan := NewFilterNode(NewScanNode(source), predicate)
    optimizer := NewPredicatePushdown()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = optimizer.Optimize(plan)
    }
}
```

### 2. Complex Plan Benchmark
```go
func BenchmarkOptimization_ComplexPlan(b *testing.B) {
    // Build realistic complex plan
    plan := buildComplexQueryPlan()
    optimizers := []Optimizer{
        NewProjectionPushdown(),
        NewPredicatePushdown(),
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        optimized := plan
        for _, opt := range optimizers {
            optimized, _ = opt.Optimize(optimized)
        }
    }
}
```

## Test Data Patterns

### 1. Minimal Test Data
Use nil or minimal data for logic tests:
```go
source := NewDataFrameSource(nil) // Logic tests don't need actual data
```

### 2. Realistic Test Plans
Create realistic query patterns:
```go
// Common pattern: filter -> group -> aggregate -> sort -> limit
plan := NewLimitNode(
    NewSortNode(
        NewProjectNode(
            NewGroupByNode(
                NewFilterNode(
                    NewScanNode(source),
                    yearFilter,
                ),
                groupKeys,
                aggregations,
            ),
            projectionExprs,
        ),
        sortExprs,
        descending,
    ),
    100,
)
```

## Debugging Failed Tests

### 1. Plan Visualization
Always log plans when tests fail:
```go
t.Logf("Original plan:\n%s", plan.String())
t.Logf("Optimized plan:\n%s", optimized.String())
```

### 2. Step-by-Step Verification
Break complex assertions into steps:
```go
// Don't just check final result
projectNode, ok := optimized.(*ProjectNode)
require.True(t, ok, "Expected ProjectNode at root")

groupByNode, ok := projectNode.input.(*GroupByNode)
require.True(t, ok, "Expected GroupByNode below project")

// ... continue navigation with clear error messages
```

### 3. Expression Debugging
Log expression strings when debugging:
```go
t.Logf("Filter expression: %s", filter.predicate.String())
t.Logf("Collected columns: %v", collectedColumns)
```

## Coverage Guidelines

### Must Test
1. Basic optimization case
2. Multiple operations combined
3. Pushdown through each compatible node type
4. Blocking at each incompatible node type
5. Empty/nil inputs
6. Already optimized plans

### Should Test
1. Complex realistic queries
2. Performance benchmarks
3. Memory allocation checks
4. Error conditions

### Integration Tests
Test optimizers working together:
```go
func TestOptimizers_Combined(t *testing.T) {
    // Test that multiple optimizers work correctly together
    plan := buildComplexPlan()
    
    // Apply all optimizers
    optimized := plan
    for _, opt := range []Optimizer{
        NewProjectionPushdown(),
        NewPredicatePushdown(),
    } {
        optimized, _ = opt.Optimize(optimized)
    }
    
    // Verify both optimizations applied
    assertProjectionPushed(t, optimized)
    assertPredicatesPushed(t, optimized)
}
```