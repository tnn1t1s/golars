# Bugs and Fixes Log

## Query Optimization Bugs

### 1. Projection Pushdown Filter Column Bug

**Date**: 2024-01-11
**Severity**: High
**Status**: Fixed

#### Problem
Projection pushdown optimizer was removing columns needed by filters in the subtree, causing "column not found" errors.

#### Example Case
```go
df.Filter(expr.ColBuilder("store").Eq(expr.Lit("B")).Build()).
   GroupBy("product").
   Sum("quantity").
   SelectColumns("product", "quantity_sum")
```

Error: `column 'store' not found` because projection pushdown removed the "store" column that was needed by the filter.

#### Root Cause
1. Projection pushdown only collected columns from the GroupBy node itself
2. Did not check if filters below the GroupBy needed additional columns
3. Column extraction couldn't handle complex expressions like `(col(store) == lit(B))`

#### Fix
1. Modified `collectExprColumns` to parse all column references from expression strings:
```go
// Old: Only handled simple col(name) expressions
// New: Extracts all col() references from any expression
func (opt *ProjectionPushdown) collectExprColumns(expr expr.Expr, needed map[string]bool) {
    exprStr := expr.String()
    // Find all instances of "col(" in the string
    // Handle nested parentheses correctly
}
```

2. Added `collectFilterColumnsRecursive` to find all filters in subtree:
```go
func (opt *ProjectionPushdown) collectFilterColumnsRecursive(plan LogicalPlan, needed map[string]bool) {
    // Recursively visit all nodes
    // Collect columns from any FilterNode predicates
    // Also check ScanNode filters
}
```

3. Updated GroupBy optimization to use recursive collection:
```go
case *GroupByNode:
    // ... collect groupby columns ...
    opt.collectFilterColumnsRecursive(node.input, inputNeeded)
```

#### Test Case
Added `TestProjectionPushdown_FilterBelowGroupBy` to prevent regression.

#### Lessons Learned
- Must analyze entire subtree, not just immediate children
- Expression parsing needs to handle complex expressions
- Always test with realistic query patterns

### 2. AND Expression Combination Bug

**Date**: 2024-01-11  
**Severity**: Medium
**Status**: Fixed

#### Problem
Multiple filters were not being combined correctly with AND in predicate pushdown.

#### Root Cause
The `combinePredicates` function wasn't using the expression builder correctly to create AND expressions.

#### Fix
```go
func combinePredicates(predicates []expr.Expr) expr.Expr {
    result := predicates[0]
    for i := 1; i < len(predicates); i++ {
        builder := expr.NewBuilder(result)
        result = builder.And(predicates[i]).Build()
    }
    return result
}
```

### 3. Nil Pointer in DataFrameSource.String()

**Date**: Earlier session
**Severity**: Low
**Status**: Fixed

#### Problem
Calling String() on nil DataFrameSource caused panic.

#### Fix
Added nil checks:
```go
func (s *DataFrameSource) String() string {
    if s == nil || s.df == nil {
        return "DataFrame[nil]"
    }
    return fmt.Sprintf("DataFrame[%d × %d]", s.df.Height(), s.df.Width())
}
```

## Testing Bugs

### 1. Non-Deterministic Map Iteration

**Date**: Earlier session
**Severity**: Low
**Status**: Acknowledged

#### Problem
GroupBy tests fail intermittently due to Go's non-deterministic map iteration order.

#### Workaround
Tests check for value existence rather than exact positions in results.

## Common Bug Patterns

### 1. Incomplete Tree Traversal
- **Pattern**: Only checking immediate children instead of full subtree
- **Prevention**: Use recursive helpers for analysis
- **Example**: Filter column collection bug

### 2. Expression Handling
- **Pattern**: Assuming simple expression structure
- **Prevention**: Parse expression strings defensively
- **Example**: Column extraction from complex expressions

### 3. Nil Safety
- **Pattern**: Not checking for nil pointers in String() methods
- **Prevention**: Always check nil in String() and similar methods
- **Example**: DataFrameSource.String()

### 4. Type Assertions
- **Pattern**: Assuming type assertions will succeed
- **Prevention**: Always check ok value from type assertions
- **Example**: Test assertions without checking

## Debugging Techniques

### 1. Plan Inspection
```go
fmt.Printf("Before optimization:\n%s\n", plan.String())
fmt.Printf("After optimization:\n%s\n", optimized.String())
```

### 2. Expression Debugging
```go
fmt.Printf("Expression: %s\n", expr.String())
fmt.Printf("Collected columns: %v\n", columns)
```

### 3. Step-by-Step Navigation
```go
// Don't assume structure, verify each step
node1, ok := plan.(*ProjectNode)
require.True(t, ok, "Expected ProjectNode")
node2, ok := node1.input.(*GroupByNode)
require.True(t, ok, "Expected GroupByNode")
```

## Prevention Strategies

1. **Comprehensive Tests**: Test complex realistic queries, not just simple cases
2. **Recursive Analysis**: Always consider full tree, not just immediate nodes  
3. **Defensive Parsing**: Handle all expression formats, not just expected ones
4. **Nil Checks**: Add nil checks in all String() and display methods
5. **Type Safety**: Check all type assertions
6. **Integration Tests**: Test features working together, not just in isolation