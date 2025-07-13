# Lazy Evaluation Design for Golars

## Executive Summary

This document outlines a comprehensive design for re-implementing lazy evaluation in Golars, learning from both Polars' sophisticated approach and the issues that led to its removal from Golars. The proposed design addresses the core problems of expression opacity, conditional complexity, and type erasure while maintaining Go idioms and performance.

## Problem Statement

The previous lazy evaluation implementation was removed due to:
1. **Opaque expressions** - Optimizers couldn't analyze expression structure
2. **String-based analysis** - Parsing string representations for optimization
3. **Conditional complexity** - Difficult to handle complex conditionals
4. **Type erasure boundary** - Runtime type checks throughout execution
5. **API incompatibility** - Conflicted with refactored package structure

## Design Goals

1. **Transparent Expression AST** - Full visibility into expression structure
2. **Type-safe optimization** - Compile-time type information where possible
3. **Efficient memory model** - Arena allocation for expressions
4. **Extensible optimizer pipeline** - Easy to add new optimizations
5. **Go-idiomatic API** - Natural for Go developers
6. **Zero-copy where possible** - Minimize data movement

## Proposed Architecture

### 1. Expression AST with Visitor Pattern

```go
// expr/ast.go
package expr

// ExprKind represents the type of expression node
type ExprKind int

const (
    KindColumn ExprKind = iota
    KindLiteral
    KindBinary
    KindUnary
    KindAggregation
    KindWindow
    KindTernary
    KindFunction
    KindCast
    KindSort
    KindFilter
    KindSlice
)

// ExprNode represents a node in the expression AST
type ExprNode interface {
    Kind() ExprKind
    Accept(visitor ExprVisitor) error
    Children() []ExprNode
    WithChildren(children []ExprNode) ExprNode
    DataType(schema *Schema) (DataType, error)
    String() string
}

// ExprVisitor allows traversal and transformation of expression trees
type ExprVisitor interface {
    VisitColumn(*ColumnNode) error
    VisitLiteral(*LiteralNode) error
    VisitBinary(*BinaryNode) error
    VisitUnary(*UnaryNode) error
    VisitAggregation(*AggregationNode) error
    VisitWindow(*WindowNode) error
    VisitTernary(*TernaryNode) error
    VisitFunction(*FunctionNode) error
    VisitCast(*CastNode) error
    VisitSort(*SortNode) error
    VisitFilter(*FilterNode) error
    VisitSlice(*SliceNode) error
}
```

### 2. Arena-Based Expression Storage

```go
// expr/arena.go
package expr

// ExprArena manages expression nodes with efficient memory allocation
type ExprArena struct {
    nodes    []arenaNode
    strings  []string      // String interning
    metadata []interface{} // Additional metadata
}

// NodeID represents an expression in the arena
type NodeID uint32

// arenaNode stores the actual expression data
type arenaNode struct {
    kind     ExprKind
    data     nodeData // Union-like structure
    children []NodeID
    name     uint32   // Index into strings
    dtype    DataType // Cached data type
}

// AExpr is an arena-allocated expression
type AExpr struct {
    arena *ExprArena
    node  NodeID
}

func (a *ExprArena) AddColumn(name string) NodeID
func (a *ExprArena) AddBinary(op BinaryOp, left, right NodeID) NodeID
func (a *ExprArena) GetNode(id NodeID) *arenaNode
func (a *ExprArena) Transform(id NodeID, f func(*arenaNode) *arenaNode) NodeID
```

### 3. Logical Plan with Rich Metadata

```go
// lazy/plan.go
package lazy

// LogicalPlan represents a lazy query plan
type LogicalPlan interface {
    Schema() (*expr.Schema, error)
    Optimize(optimizer Optimizer) (LogicalPlan, error)
    Execute(ctx context.Context) (*dataframe.DataFrame, error)
    Explain() string
    Children() []LogicalPlan
    WithChildren(children []LogicalPlan) LogicalPlan
}

// Common plan nodes
type ScanNode struct {
    source   DataSource
    schema   *expr.Schema
    projections []expr.NodeID // Which columns to read
    predicates  []expr.NodeID // Filters to push down
}

type SelectNode struct {
    input    LogicalPlan
    exprs    []expr.NodeID
    arena    *expr.ExprArena
}

type FilterNode struct {
    input     LogicalPlan
    predicate expr.NodeID
    arena     *expr.ExprArena
}

type GroupByNode struct {
    input    LogicalPlan
    keys     []expr.NodeID
    aggs     []expr.NodeID
    arena    *expr.ExprArena
}

type JoinNode struct {
    left      LogicalPlan
    right     LogicalPlan
    leftOn    []expr.NodeID
    rightOn   []expr.NodeID
    joinType  JoinType
    arena     *expr.ExprArena
}
```

### 4. Optimizer Pipeline

```go
// lazy/optimizer.go
package lazy

// Optimizer transforms logical plans
type Optimizer interface {
    Name() string
    Optimize(plan LogicalPlan) (LogicalPlan, error)
}

// OptimizerPipeline runs multiple optimizers
type OptimizerPipeline struct {
    optimizers []Optimizer
    maxPasses  int
}

// Standard optimizers
type PredicatePushdown struct {
    arena *expr.ExprArena
}

func (p *PredicatePushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
    // Use visitor pattern to analyze predicates
    // Push predicates through joins, projections, etc.
}

type ProjectionPushdown struct {
    arena *expr.ExprArena
}

type CommonSubexpressionElimination struct {
    arena *expr.ExprArena
    cache map[string]expr.NodeID
}

type TypeCoercion struct {
    arena *expr.ExprArena
}

type ConstantFolding struct {
    arena *expr.ExprArena
}
```

### 5. Expression Analysis Framework

```go
// expr/analysis.go
package expr

// ExpressionAnalyzer provides utilities for analyzing expressions
type ExpressionAnalyzer struct {
    arena *ExprArena
}

// ExtractColumns returns all column references in an expression
func (a *ExpressionAnalyzer) ExtractColumns(expr NodeID) []string

// HasAggregation checks if expression contains aggregations
func (a *ExpressionAnalyzer) HasAggregation(expr NodeID) bool

// IsConstant checks if expression is constant
func (a *ExpressionAnalyzer) IsConstant(expr NodeID) bool

// Dependencies returns expressions this one depends on
func (a *ExpressionAnalyzer) Dependencies(expr NodeID) []NodeID

// CanPushDown checks if expression can be pushed through operation
func (a *ExpressionAnalyzer) CanPushDown(expr NodeID, op PlanOp) bool
```

### 6. Physical Execution Engine

```go
// lazy/physical.go
package lazy

// PhysicalExpr represents executable expression
type PhysicalExpr interface {
    Evaluate(batch *dataframe.DataFrame) (series.Series, error)
    EvaluateScalar(batch *dataframe.DataFrame) (interface{}, error)
    RequiredColumns() []string
}

// ExpressionCompiler converts logical to physical expressions
type ExpressionCompiler struct {
    arena    *ExprArena
    schema   *Schema
    context  EvalContext
}

func (c *ExpressionCompiler) Compile(expr NodeID) (PhysicalExpr, error) {
    node := c.arena.GetNode(expr)
    switch node.kind {
    case KindColumn:
        return &columnExpr{name: c.arena.GetString(node.name)}, nil
    case KindBinary:
        left := c.Compile(node.children[0])
        right := c.Compile(node.children[1])
        return &binaryExpr{op: node.data.binaryOp, left: left, right: right}, nil
    // ... other cases
    }
}
```

### 7. LazyFrame API

```go
// lazy/lazyframe.go
package lazy

type LazyFrame struct {
    plan  LogicalPlan
    arena *expr.ExprArena
}

// Builder methods maintain lazy evaluation
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
    exprNodes := make([]expr.NodeID, len(exprs))
    for i, e := range exprs {
        exprNodes[i] = e.ToNode(lf.arena)
    }
    return &LazyFrame{
        plan:  &SelectNode{input: lf.plan, exprs: exprNodes, arena: lf.arena},
        arena: lf.arena,
    }
}

func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
    return &LazyFrame{
        plan:  &FilterNode{input: lf.plan, predicate: predicate.ToNode(lf.arena), arena: lf.arena},
        arena: lf.arena,
    }
}

func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
    // Returns specialized type for aggregation context
}

// Terminal operations trigger optimization and execution
func (lf *LazyFrame) Collect(ctx context.Context) (*dataframe.DataFrame, error) {
    // Run optimizer pipeline
    optimized := lf.optimize()
    
    // Execute physical plan
    return optimized.Execute(ctx)
}

func (lf *LazyFrame) Explain(verbose bool) string {
    // Show logical plan and optimizations
}
```

### 8. Expression Builder Enhancement

```go
// expr/builder.go
package expr

// Expr is the user-facing expression interface
type Expr interface {
    // Convert to arena node for lazy evaluation
    ToNode(arena *ExprArena) NodeID
    
    // Existing interface maintained for compatibility
    String() string
    DataType() DataType
    Alias(name string) Expr
}

// Enhanced column builder
type columnBuilder struct {
    name string
}

func (c *columnBuilder) ToNode(arena *ExprArena) NodeID {
    return arena.AddColumn(c.name)
}

// Support for complex expressions
func When(condition Expr) *WhenBuilder {
    return &WhenBuilder{condition: condition}
}

type WhenBuilder struct {
    condition Expr
    cases     []whenCase
}

func (w *WhenBuilder) Then(expr Expr) *WhenBuilder
func (w *WhenBuilder) Otherwise(expr Expr) Expr
```

## Implementation Strategy

### Phase 1: Expression AST (Week 1-2)
1. Implement ExprNode interface and concrete types
2. Add visitor pattern support
3. Create arena allocator
4. Migrate existing expressions to new AST

### Phase 2: Logical Plans (Week 3-4)
1. Define LogicalPlan interface
2. Implement basic plan nodes (Scan, Select, Filter)
3. Add plan builder methods
4. Create plan validation

### Phase 3: Basic Optimizers (Week 5-6)
1. Implement predicate pushdown
2. Add projection pushdown
3. Create optimizer pipeline
4. Add plan explanation

### Phase 4: Physical Execution (Week 7-8)
1. Create physical expression compiler
2. Implement execution operators
3. Add parallel execution support
4. Integrate with existing DataFrame

### Phase 5: Advanced Features (Week 9-10)
1. Window functions
2. Join optimization
3. Common subexpression elimination
4. Memory management improvements

### Phase 6: Testing and Documentation (Week 11-12)
1. Comprehensive test suite
2. Performance benchmarks
3. User documentation
4. Migration guide

## Design Decisions

### 1. Arena Allocation
**Rationale**: Efficient memory usage, fast cloning, cache-friendly traversal
**Trade-off**: More complex than pointer-based trees but significantly faster

### 2. Visitor Pattern
**Rationale**: Allows external analysis without modifying expression types
**Trade-off**: More boilerplate but enables clean separation of concerns

### 3. Separate Logical and Physical Plans
**Rationale**: Enables optimization without considering execution details
**Trade-off**: Additional translation step but more flexible optimization

### 4. NodeID Instead of Pointers
**Rationale**: Enables safe concurrent access and efficient serialization
**Trade-off**: Indirect access but better for parallel processing

### 5. Type Information in AST
**Rationale**: Enables type-based optimizations during planning
**Trade-off**: More memory usage but avoids runtime type checks

## Integration Points

### With Existing DataFrame
- Physical execution produces standard DataFrames
- Expressions can be evaluated eagerly or lazily
- Backward compatibility maintained

### With Series Package
- Physical expressions work with existing Series interface
- Type-safe operations where possible
- Efficient batch processing

### With I/O Package
- Scan nodes integrate with readers
- Predicate pushdown to file readers
- Projection pushdown for column selection

## Performance Considerations

1. **Memory Efficiency**
   - Arena allocation reduces allocations
   - Expression sharing through NodeIDs
   - Lazy materialization of results

2. **CPU Efficiency**
   - Vectorized execution where possible
   - Parallel execution of independent operations
   - Cache-friendly data structures

3. **I/O Efficiency**
   - Predicate pushdown to reduce data read
   - Projection pushdown to read only needed columns
   - Lazy evaluation to combine operations

## Error Handling

1. **Planning Errors**
   - Type mismatches caught during planning
   - Invalid operations detected early
   - Clear error messages with expression context

2. **Execution Errors**
   - Runtime errors properly propagated
   - Partial results not returned
   - Resource cleanup on failure

## Future Extensions

1. **Query Caching**
   - Cache computed results
   - Incremental computation

2. **Distributed Execution**
   - Partition-aware planning
   - Distributed joins

3. **Custom Functions**
   - User-defined functions in expressions
   - Plugin system for optimizers

4. **SQL Interface**
   - SQL parser producing logical plans
   - SQL optimization rules

## Conclusion

This design addresses the core issues that led to the removal of lazy evaluation while providing a solid foundation for future enhancements. The arena-based expression system with visitor pattern support enables sophisticated optimizations while maintaining Go idioms and performance characteristics.

The phased implementation approach allows for incremental development and testing, ensuring each component is solid before building the next layer. The design is extensible and maintainable, setting Golars up for long-term success in providing efficient lazy evaluation capabilities.