# Arena-Based Lazy Evaluation System for Golars

## Executive Summary

This document details the design and implementation of an arena-based lazy evaluation system for Golars, directly inspired by Polars' architecture. The system uses indexed allocation for expressions, avoiding pointer-heavy recursive trees while enabling sophisticated query optimization and execution.

## Core Design Principles

1. **Arena-based expression representation** – Use indexed allocation for expressions to avoid pointer-heavy, recursive trees
2. **Expression decomposition** – Decompose expressions into an AST with clear node types and metadata
3. **Context-sensitive planning** – Support dynamic behavior for expressions depending on context (e.g., filter, group_by)
4. **Pluggable optimizations** – Design optimizations as separate passes over the plan
5. **Deferred execution** – Build the query graph and execute only upon Collect()

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        User API Layer                        │
│                    (LazyFrame, Expr builders)                │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                      Expression System                       │
│                    (Arena, NodeIdx, ExprNode)                │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                       Logical Planning                       │
│              (LogicalPlan, Context, Schema)                  │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                    Optimization Pipeline                     │
│        (PredicatePushdown, ProjectionPushdown, etc.)        │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                     Physical Planning                        │
│              (PhysicalExpr, PhysicalPlan)                   │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                     Execution Engine                         │
│                    (Collect, Execute)                        │
└─────────────────────────────────────────────────────────────┘
```

## 1. Expression System (`expr/`)

### 1.1 Arena Structure

```go
// expr/arena.go
package expr

// NodeIdx is an index into the expression arena
type NodeIdx int

// InvalidNodeIdx represents an invalid node index
const InvalidNodeIdx NodeIdx = -1

// ExprArena manages all expression nodes in a contiguous structure
type ExprArena struct {
    nodes    []ExprNode
    strings  StringInterner // String deduplication
    metadata map[NodeIdx]interface{} // Optional metadata
}

// ExprNode represents a node in the expression tree
type ExprNode struct {
    Kind      ExprKind
    Output    OutputName
    DataType  *DataType
    Children  []NodeIdx
}

// Add inserts a new expression node and returns its index
func (a *ExprArena) Add(node ExprNode) NodeIdx {
    idx := NodeIdx(len(a.nodes))
    a.nodes = append(a.nodes, node)
    return idx
}

// Get retrieves a node by index
func (a *ExprArena) Get(idx NodeIdx) ExprNode {
    if idx < 0 || int(idx) >= len(a.nodes) {
        panic("invalid node index")
    }
    return a.nodes[idx]
}

// Transform creates a new node based on an existing one
func (a *ExprArena) Transform(idx NodeIdx, f func(ExprNode) ExprNode) NodeIdx {
    node := a.Get(idx)
    newNode := f(node)
    return a.Add(newNode)
}
```

### 1.2 Expression Kinds

```go
// expr/kinds.go
package expr

// ExprKind represents different expression types using interfaces
type ExprKind interface {
    Accept(visitor ExprVisitor) error
    String() string
}

// Literal expressions
type LiteralExpr struct {
    Value AnyValue // Can be int64, float64, string, bool, etc.
}

func (l LiteralExpr) Accept(v ExprVisitor) error {
    return v.VisitLiteral(l)
}

// Column reference
type ColumnExpr struct {
    Name string
}

func (c ColumnExpr) Accept(v ExprVisitor) error {
    return v.VisitColumn(c)
}

// Binary operations
type BinaryExpr struct {
    Op    BinaryOp
    Left  NodeIdx
    Right NodeIdx
}

func (b BinaryExpr) Accept(v ExprVisitor) error {
    return v.VisitBinary(b)
}

// Unary operations
type UnaryExpr struct {
    Op    UnaryOp
    Input NodeIdx
}

func (u UnaryExpr) Accept(v ExprVisitor) error {
    return v.VisitUnary(u)
}

// Aggregation
type AggExpr struct {
    Op    AggOp
    Input NodeIdx
}

func (a AggExpr) Accept(v ExprVisitor) error {
    return v.VisitAgg(a)
}

// Window function
type WindowExpr struct {
    Function  NodeIdx
    Partition []NodeIdx
    OrderBy   []NodeIdx
}

func (w WindowExpr) Accept(v ExprVisitor) error {
    return v.VisitWindow(w)
}

// Ternary (if-then-else)
type TernaryExpr struct {
    Condition NodeIdx
    TrueExpr  NodeIdx
    FalseExpr NodeIdx
}

func (t TernaryExpr) Accept(v ExprVisitor) error {
    return v.VisitTernary(t)
}

// Function call
type FunctionExpr struct {
    Name string
    Args []NodeIdx
}

func (f FunctionExpr) Accept(v ExprVisitor) error {
    return v.VisitFunction(f)
}

// Cast operation
type CastExpr struct {
    Input    NodeIdx
    DataType DataType
}

func (c CastExpr) Accept(v ExprVisitor) error {
    return v.VisitCast(c)
}

// Sort operation
type SortExpr struct {
    Input      NodeIdx
    Descending bool
    NullsLast  bool
}

func (s SortExpr) Accept(v ExprVisitor) error {
    return v.VisitSort(s)
}

// Filter operation
type FilterExpr struct {
    Input     NodeIdx
    Predicate NodeIdx
}

func (f FilterExpr) Accept(v ExprVisitor) error {
    return v.VisitFilter(f)
}

// Slice operation
type SliceExpr struct {
    Input  NodeIdx
    Offset int64
    Length int64
}

func (s SliceExpr) Accept(v ExprVisitor) error {
    return v.VisitSlice(s)
}
```

### 1.3 Output Naming

```go
// expr/output.go
package expr

type OutputNameKind int

const (
    Alias OutputNameKind = iota
    ColumnLhs
    LiteralLhs
    Field
)

type OutputName struct {
    Kind  OutputNameKind
    Value string
}

func NewAlias(name string) OutputName {
    return OutputName{Kind: Alias, Value: name}
}

func NewColumnOutput(name string) OutputName {
    return OutputName{Kind: ColumnLhs, Value: name}
}
```

### 1.4 Visitor Pattern

```go
// expr/visitor.go
package expr

// ExprVisitor allows traversal and transformation of expression trees
type ExprVisitor interface {
    VisitLiteral(LiteralExpr) error
    VisitColumn(ColumnExpr) error
    VisitBinary(BinaryExpr) error
    VisitUnary(UnaryExpr) error
    VisitAgg(AggExpr) error
    VisitWindow(WindowExpr) error
    VisitTernary(TernaryExpr) error
    VisitFunction(FunctionExpr) error
    VisitCast(CastExpr) error
    VisitSort(SortExpr) error
    VisitFilter(FilterExpr) error
    VisitSlice(SliceExpr) error
}

// BaseVisitor provides default implementations
type BaseVisitor struct{}

func (b BaseVisitor) VisitLiteral(LiteralExpr) error { return nil }
func (b BaseVisitor) VisitColumn(ColumnExpr) error { return nil }
// ... other default implementations

// Walk traverses expression tree without recursion
func Walk(arena *ExprArena, root NodeIdx, visit func(NodeIdx, ExprNode)) {
    stack := []NodeIdx{root}
    visited := make(map[NodeIdx]bool)
    
    for len(stack) > 0 {
        curr := stack[len(stack)-1]
        stack = stack[:len(stack)-1]
        
        if visited[curr] {
            continue
        }
        visited[curr] = true
        
        node := arena.Get(curr)
        visit(curr, node)
        
        // Add children to stack
        for i := len(node.Children) - 1; i >= 0; i-- {
            stack = append(stack, node.Children[i])
        }
    }
}

// WalkPostOrder traverses expression tree in post-order
func WalkPostOrder(arena *ExprArena, root NodeIdx, visit func(NodeIdx, ExprNode)) {
    var postOrder func(NodeIdx)
    visited := make(map[NodeIdx]bool)
    
    postOrder = func(idx NodeIdx) {
        if visited[idx] {
            return
        }
        visited[idx] = true
        
        node := arena.Get(idx)
        for _, child := range node.Children {
            postOrder(child)
        }
        visit(idx, node)
    }
    
    postOrder(root)
}
```

### 1.5 Expression Analysis

```go
// expr/analysis.go
package expr

// ExpressionAnalyzer provides utilities for analyzing expressions
type ExpressionAnalyzer struct {
    arena *ExprArena
}

// ExtractColumns returns all column references in an expression
func (a *ExpressionAnalyzer) ExtractColumns(expr NodeIdx) []string {
    columns := make(map[string]bool)
    
    Walk(a.arena, expr, func(idx NodeIdx, node ExprNode) {
        if col, ok := node.Kind.(ColumnExpr); ok {
            columns[col.Name] = true
        }
    })
    
    result := make([]string, 0, len(columns))
    for col := range columns {
        result = append(result, col)
    }
    return result
}

// HasAggregation checks if expression contains aggregations
func (a *ExpressionAnalyzer) HasAggregation(expr NodeIdx) bool {
    found := false
    
    Walk(a.arena, expr, func(idx NodeIdx, node ExprNode) {
        if _, ok := node.Kind.(AggExpr); ok {
            found = true
        }
    })
    
    return found
}

// IsConstant checks if expression is constant
func (a *ExpressionAnalyzer) IsConstant(expr NodeIdx) bool {
    constant := true
    
    Walk(a.arena, expr, func(idx NodeIdx, node ExprNode) {
        switch node.Kind.(type) {
        case ColumnExpr, AggExpr, WindowExpr:
            constant = false
        }
    })
    
    return constant
}

// Dependencies returns expressions this one depends on
func (a *ExpressionAnalyzer) Dependencies(expr NodeIdx) []NodeIdx {
    deps := make(map[NodeIdx]bool)
    
    node := a.arena.Get(expr)
    for _, child := range node.Children {
        deps[child] = true
        // Recursively get dependencies
        childDeps := a.Dependencies(child)
        for _, dep := range childDeps {
            deps[dep] = true
        }
    }
    
    result := make([]NodeIdx, 0, len(deps))
    for dep := range deps {
        result = append(result, dep)
    }
    return result
}

// CanPushDown checks if expression can be pushed through operation
func (a *ExpressionAnalyzer) CanPushDown(expr NodeIdx, op PlanOp) bool {
    switch op {
    case OpJoin:
        // Can push down if doesn't contain aggregations
        return !a.HasAggregation(expr)
    case OpGroupBy:
        // Can push down if only references grouping columns
        return a.IsGroupingCompatible(expr)
    default:
        return true
    }
}
```

## 2. Logical Planning (`logical/`)

### 2.1 Logical Plan Interface

```go
// logical/plan.go
package logical

import "golars/expr"

// LogicalPlan represents a node in the query plan
type LogicalPlan interface {
    Schema() Schema
    Inputs() []LogicalPlan
    Describe() string
    Accept(visitor PlanVisitor) error
}

// Schema represents the output schema of a plan node
type Schema struct {
    Fields []Field
}

type Field struct {
    Name     string
    DataType expr.DataType
}

// PlanVisitor allows traversal of logical plans
type PlanVisitor interface {
    VisitScan(*Scan) error
    VisitProjection(*Projection) error
    VisitFilter(*Filter) error
    VisitAggregate(*Aggregate) error
    VisitJoin(*Join) error
    VisitSort(*Sort) error
    VisitLimit(*Limit) error
    VisitUnion(*Union) error
}
```

### 2.2 Logical Plan Nodes

```go
// logical/nodes.go
package logical

// Scan reads from a data source
type Scan struct {
    Source      DataSource
    Projections []string // Column names to read
    Predicates  []expr.NodeIdx // Filters to push down
    Arena       *expr.ExprArena
}

func (s *Scan) Schema() Schema {
    return s.Source.Schema()
}

func (s *Scan) Inputs() []LogicalPlan {
    return nil
}

func (s *Scan) Accept(v PlanVisitor) error {
    return v.VisitScan(s)
}

// Projection selects and transforms columns
type Projection struct {
    Input LogicalPlan
    Exprs []expr.NodeIdx
    Arena *expr.ExprArena
}

func (p *Projection) Schema() Schema {
    // Build schema from expressions
    fields := make([]Field, len(p.Exprs))
    for i, exprIdx := range p.Exprs {
        node := p.Arena.Get(exprIdx)
        fields[i] = Field{
            Name:     node.Output.Value,
            DataType: *node.DataType,
        }
    }
    return Schema{Fields: fields}
}

func (p *Projection) Inputs() []LogicalPlan {
    return []LogicalPlan{p.Input}
}

func (p *Projection) Accept(v PlanVisitor) error {
    return v.VisitProjection(p)
}

// Filter applies predicates
type Filter struct {
    Input LogicalPlan
    Pred  expr.NodeIdx
    Arena *expr.ExprArena
}

func (f *Filter) Schema() Schema {
    return f.Input.Schema()
}

func (f *Filter) Inputs() []LogicalPlan {
    return []LogicalPlan{f.Input}
}

func (f *Filter) Accept(v PlanVisitor) error {
    return v.VisitFilter(f)
}

// Aggregate performs grouping and aggregation
type Aggregate struct {
    Input     LogicalPlan
    GroupExpr []expr.NodeIdx
    AggExprs  []expr.NodeIdx
    Arena     *expr.ExprArena
}

func (a *Aggregate) Schema() Schema {
    // Schema includes group columns + aggregation results
    inputSchema := a.Input.Schema()
    fields := make([]Field, 0, len(a.GroupExpr)+len(a.AggExprs))
    
    // Add group columns
    for _, groupIdx := range a.GroupExpr {
        node := a.Arena.Get(groupIdx)
        fields = append(fields, Field{
            Name:     node.Output.Value,
            DataType: *node.DataType,
        })
    }
    
    // Add aggregation columns
    for _, aggIdx := range a.AggExprs {
        node := a.Arena.Get(aggIdx)
        fields = append(fields, Field{
            Name:     node.Output.Value,
            DataType: *node.DataType,
        })
    }
    
    return Schema{Fields: fields}
}

func (a *Aggregate) Inputs() []LogicalPlan {
    return []LogicalPlan{a.Input}
}

func (a *Aggregate) Accept(v PlanVisitor) error {
    return v.VisitAggregate(a)
}

// Join combines two inputs
type Join struct {
    Left     LogicalPlan
    Right    LogicalPlan
    LeftOn   []expr.NodeIdx
    RightOn  []expr.NodeIdx
    JoinType JoinType
    Arena    *expr.ExprArena
}

type JoinType int

const (
    InnerJoin JoinType = iota
    LeftJoin
    RightJoin
    OuterJoin
    CrossJoin
)

func (j *Join) Schema() Schema {
    // Combine schemas based on join type
    leftSchema := j.Left.Schema()
    rightSchema := j.Right.Schema()
    
    fields := make([]Field, 0, len(leftSchema.Fields)+len(rightSchema.Fields))
    fields = append(fields, leftSchema.Fields...)
    
    // Add right fields, handling name conflicts
    for _, field := range rightSchema.Fields {
        fields = append(fields, field)
    }
    
    return Schema{Fields: fields}
}

func (j *Join) Inputs() []LogicalPlan {
    return []LogicalPlan{j.Left, j.Right}
}

func (j *Join) Accept(v PlanVisitor) error {
    return v.VisitJoin(j)
}

// Sort orders the output
type Sort struct {
    Input   LogicalPlan
    SortBy  []expr.NodeIdx
    Arena   *expr.ExprArena
}

func (s *Sort) Schema() Schema {
    return s.Input.Schema()
}

func (s *Sort) Inputs() []LogicalPlan {
    return []LogicalPlan{s.Input}
}

func (s *Sort) Accept(v PlanVisitor) error {
    return v.VisitSort(s)
}

// Limit restricts output rows
type Limit struct {
    Input  LogicalPlan
    Offset int64
    Limit  int64
}

func (l *Limit) Schema() Schema {
    return l.Input.Schema()
}

func (l *Limit) Inputs() []LogicalPlan {
    return []LogicalPlan{l.Input}
}

func (l *Limit) Accept(v PlanVisitor) error {
    return v.VisitLimit(l)
}
```

### 2.3 Evaluation Context

```go
// logical/context.go
package logical

type EvalContext int

const (
    DefaultContext EvalContext = iota
    FilterContext
    AggregationContext
    GroupByContext
    WindowContext
)

// ContextInfo provides context-specific behavior
type ContextInfo struct {
    Context         EvalContext
    GroupingColumns []string
    WindowSpec      *WindowSpec
}

type WindowSpec struct {
    PartitionBy []expr.NodeIdx
    OrderBy     []expr.NodeIdx
    Frame       WindowFrame
}

type WindowFrame struct {
    Type  FrameType
    Start FrameBound
    End   FrameBound
}

type FrameType int

const (
    RowsFrame FrameType = iota
    RangeFrame
)

type FrameBound struct {
    Type   BoundType
    Offset int64
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

## 3. Optimization System (`planner/`)

### 3.1 Optimizer Interface

```go
// planner/optimizer.go
package planner

import (
    "golars/expr"
    "golars/logical"
)

// Optimizer transforms logical plans
type Optimizer interface {
    Name() string
    Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error)
}

// OptimizerPipeline runs multiple optimizers
type OptimizerPipeline struct {
    optimizers []Optimizer
    maxPasses  int
}

func NewOptimizerPipeline() *OptimizerPipeline {
    return &OptimizerPipeline{
        optimizers: []Optimizer{
            &PredicatePushdown{},
            &ProjectionPushdown{},
            &ExprSimplification{},
            &CommonSubplanElim{},
            &TypeCoercion{},
            &ConstantFolding{},
        },
        maxPasses: 10,
    }
}

func (p *OptimizerPipeline) Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error) {
    current := plan
    
    for pass := 0; pass < p.maxPasses; pass++ {
        changed := false
        
        for _, optimizer := range p.optimizers {
            optimized, err := optimizer.Optimize(current, arena)
            if err != nil {
                return nil, err
            }
            
            if optimized != current {
                changed = true
                current = optimized
            }
        }
        
        if !changed {
            // Fixed point reached
            break
        }
    }
    
    return current, nil
}
```

### 3.2 Predicate Pushdown

```go
// planner/predicate_pushdown.go
package planner

type PredicatePushdown struct{}

func (p *PredicatePushdown) Name() string {
    return "PredicatePushdown"
}

func (p *PredicatePushdown) Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error) {
    pushdown := &predicatePushdownVisitor{
        arena:      arena,
        analyzer:   &expr.ExpressionAnalyzer{Arena: arena},
        predicates: make([]expr.NodeIdx, 0),
    }
    
    return pushdown.pushDown(plan), nil
}

type predicatePushdownVisitor struct {
    arena      *expr.ExprArena
    analyzer   *expr.ExpressionAnalyzer
    predicates []expr.NodeIdx
}

func (v *predicatePushdownVisitor) pushDown(plan logical.LogicalPlan) logical.LogicalPlan {
    switch node := plan.(type) {
    case *logical.Filter:
        // Collect predicate and continue pushing down
        v.predicates = append(v.predicates, node.Pred)
        return v.pushDown(node.Input)
        
    case *logical.Projection:
        // Push predicates that only reference projected columns
        canPush := make([]expr.NodeIdx, 0)
        cantPush := make([]expr.NodeIdx, 0)
        
        for _, pred := range v.predicates {
            columns := v.analyzer.ExtractColumns(pred)
            if v.canPushThroughProjection(columns, node) {
                canPush = append(canPush, pred)
            } else {
                cantPush = append(cantPush, pred)
            }
        }
        
        v.predicates = canPush
        pushed := v.pushDown(node.Input)
        
        result := &logical.Projection{
            Input: pushed,
            Exprs: node.Exprs,
            Arena: node.Arena,
        }
        
        // Apply predicates that couldn't be pushed
        for _, pred := range cantPush {
            result = &logical.Filter{
                Input: result,
                Pred:  pred,
                Arena: v.arena,
            }
        }
        
        return result
        
    case *logical.Scan:
        // Push predicates into scan
        node.Predicates = append(node.Predicates, v.predicates...)
        v.predicates = nil
        return node
        
    case *logical.Join:
        // Split predicates by which side they reference
        leftPreds := make([]expr.NodeIdx, 0)
        rightPreds := make([]expr.NodeIdx, 0)
        joinPreds := make([]expr.NodeIdx, 0)
        
        leftSchema := node.Left.Schema()
        rightSchema := node.Right.Schema()
        
        for _, pred := range v.predicates {
            columns := v.analyzer.ExtractColumns(pred)
            leftOnly := v.referencesOnly(columns, leftSchema)
            rightOnly := v.referencesOnly(columns, rightSchema)
            
            if leftOnly {
                leftPreds = append(leftPreds, pred)
            } else if rightOnly {
                rightPreds = append(rightPreds, pred)
            } else {
                joinPreds = append(joinPreds, pred)
            }
        }
        
        // Push down to children
        v.predicates = leftPreds
        node.Left = v.pushDown(node.Left)
        
        v.predicates = rightPreds
        node.Right = v.pushDown(node.Right)
        
        // Apply join predicates after join
        result := logical.LogicalPlan(node)
        for _, pred := range joinPreds {
            result = &logical.Filter{
                Input: result,
                Pred:  pred,
                Arena: v.arena,
            }
        }
        
        return result
        
    default:
        // Can't push through this node, apply filters above it
        result := plan
        for _, pred := range v.predicates {
            result = &logical.Filter{
                Input: result,
                Pred:  pred,
                Arena: v.arena,
            }
        }
        v.predicates = nil
        return result
    }
}
```

### 3.3 Projection Pushdown

```go
// planner/projection_pushdown.go
package planner

type ProjectionPushdown struct{}

func (p *ProjectionPushdown) Name() string {
    return "ProjectionPushdown"
}

func (p *ProjectionPushdown) Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error) {
    // First pass: collect required columns from top
    required := p.collectRequired(plan, arena)
    
    // Second pass: push down projections
    return p.pushDown(plan, required, arena), nil
}

func (p *ProjectionPushdown) collectRequired(plan logical.LogicalPlan, arena *expr.ExprArena) map[string]bool {
    collector := &projectionCollector{
        arena:    arena,
        analyzer: &expr.ExpressionAnalyzer{Arena: arena},
        required: make(map[string]bool),
    }
    
    collector.collect(plan)
    return collector.required
}

type projectionCollector struct {
    arena    *expr.ExprArena
    analyzer *expr.ExpressionAnalyzer
    required map[string]bool
}

func (c *projectionCollector) collect(plan logical.LogicalPlan) {
    switch node := plan.(type) {
    case *logical.Projection:
        // Collect columns used in projection expressions
        for _, exprIdx := range node.Exprs {
            columns := c.analyzer.ExtractColumns(exprIdx)
            for _, col := range columns {
                c.required[col] = true
            }
        }
        
    case *logical.Filter:
        // Collect columns used in filter predicate
        columns := c.analyzer.ExtractColumns(node.Pred)
        for _, col := range columns {
            c.required[col] = true
        }
        c.collect(node.Input)
        
    case *logical.Aggregate:
        // Collect columns used in grouping and aggregations
        for _, groupIdx := range node.GroupExpr {
            columns := c.analyzer.ExtractColumns(groupIdx)
            for _, col := range columns {
                c.required[col] = true
            }
        }
        for _, aggIdx := range node.AggExprs {
            columns := c.analyzer.ExtractColumns(aggIdx)
            for _, col := range columns {
                c.required[col] = true
            }
        }
        
    default:
        // Visit all inputs
        for _, input := range plan.Inputs() {
            c.collect(input)
        }
    }
}

func (p *ProjectionPushdown) pushDown(plan logical.LogicalPlan, required map[string]bool, arena *expr.ExprArena) logical.LogicalPlan {
    switch node := plan.(type) {
    case *logical.Scan:
        // Only read required columns
        projections := make([]string, 0, len(required))
        for col := range required {
            projections = append(projections, col)
        }
        node.Projections = projections
        return node
        
    default:
        // Recursively push down to inputs
        inputs := plan.Inputs()
        for i, input := range inputs {
            inputs[i] = p.pushDown(input, required, arena)
        }
        // Rebuild node with updated inputs
        return plan
    }
}
```

### 3.4 Expression Simplification

```go
// planner/expr_simplification.go
package planner

type ExprSimplification struct{}

func (e *ExprSimplification) Name() string {
    return "ExprSimplification"
}

func (e *ExprSimplification) Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error) {
    simplifier := &exprSimplifier{
        arena: arena,
    }
    
    return simplifier.simplifyPlan(plan), nil
}

type exprSimplifier struct {
    arena *expr.ExprArena
}

func (s *exprSimplifier) simplifyPlan(plan logical.LogicalPlan) logical.LogicalPlan {
    // Apply simplification to all expressions in the plan
    switch node := plan.(type) {
    case *logical.Projection:
        simplified := make([]expr.NodeIdx, len(node.Exprs))
        for i, exprIdx := range node.Exprs {
            simplified[i] = s.simplifyExpr(exprIdx)
        }
        node.Exprs = simplified
        
    case *logical.Filter:
        node.Pred = s.simplifyExpr(node.Pred)
        
    case *logical.Aggregate:
        for i, groupIdx := range node.GroupExpr {
            node.GroupExpr[i] = s.simplifyExpr(groupIdx)
        }
        for i, aggIdx := range node.AggExprs {
            node.AggExprs[i] = s.simplifyExpr(aggIdx)
        }
    }
    
    // Recursively simplify inputs
    inputs := plan.Inputs()
    for i, input := range inputs {
        inputs[i] = s.simplifyPlan(input)
    }
    
    return plan
}

func (s *exprSimplifier) simplifyExpr(idx expr.NodeIdx) expr.NodeIdx {
    node := s.arena.Get(idx)
    
    switch kind := node.Kind.(type) {
    case expr.BinaryExpr:
        // Simplify boolean expressions
        if kind.Op == expr.And {
            left := s.arena.Get(kind.Left)
            right := s.arena.Get(kind.Right)
            
            // true AND x => x
            if lit, ok := left.Kind.(expr.LiteralExpr); ok && lit.Value == true {
                return kind.Right
            }
            // x AND true => x
            if lit, ok := right.Kind.(expr.LiteralExpr); ok && lit.Value == true {
                return kind.Left
            }
            // false AND x => false
            if lit, ok := left.Kind.(expr.LiteralExpr); ok && lit.Value == false {
                return s.arena.Add(expr.ExprNode{
                    Kind:     expr.LiteralExpr{Value: false},
                    DataType: &expr.BoolType,
                })
            }
        }
        
        // Simplify arithmetic
        if kind.Op == expr.Multiply {
            left := s.arena.Get(kind.Left)
            right := s.arena.Get(kind.Right)
            
            // x * 1 => x
            if lit, ok := right.Kind.(expr.LiteralExpr); ok {
                if f, ok := lit.Value.(float64); ok && f == 1.0 {
                    return kind.Left
                }
            }
            // 1 * x => x
            if lit, ok := left.Kind.(expr.LiteralExpr); ok {
                if f, ok := lit.Value.(float64); ok && f == 1.0 {
                    return kind.Right
                }
            }
        }
    }
    
    return idx
}
```

### 3.5 Common Subexpression Elimination

```go
// planner/cse.go
package planner

type CommonSubplanElim struct{}

func (c *CommonSubplanElim) Name() string {
    return "CommonSubplanElim"
}

func (c *CommonSubplanElim) Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error) {
    cse := &cseOptimizer{
        arena:      arena,
        exprCache:  make(map[string]expr.NodeIdx),
        planCache:  make(map[string]logical.LogicalPlan),
    }
    
    return cse.eliminateCommon(plan), nil
}

type cseOptimizer struct {
    arena     *expr.ExprArena
    exprCache map[string]expr.NodeIdx
    planCache map[string]logical.LogicalPlan
}

func (c *cseOptimizer) eliminateCommon(plan logical.LogicalPlan) logical.LogicalPlan {
    // Check if we've seen this plan before
    planKey := c.planFingerprint(plan)
    if cached, ok := c.planCache[planKey]; ok {
        return cached
    }
    
    // Process expressions in this node
    switch node := plan.(type) {
    case *logical.Projection:
        for i, exprIdx := range node.Exprs {
            node.Exprs[i] = c.deduplicateExpr(exprIdx)
        }
        
    case *logical.Filter:
        node.Pred = c.deduplicateExpr(node.Pred)
    }
    
    // Recursively process inputs
    inputs := plan.Inputs()
    for i, input := range inputs {
        inputs[i] = c.eliminateCommon(input)
    }
    
    c.planCache[planKey] = plan
    return plan
}

func (c *cseOptimizer) deduplicateExpr(idx expr.NodeIdx) expr.NodeIdx {
    // Generate fingerprint for expression
    fingerprint := c.exprFingerprint(idx)
    
    // Check cache
    if cached, ok := c.exprCache[fingerprint]; ok {
        return cached
    }
    
    // Cache and return
    c.exprCache[fingerprint] = idx
    return idx
}

func (c *cseOptimizer) exprFingerprint(idx expr.NodeIdx) string {
    // Generate a unique string representation of the expression
    var builder strings.Builder
    
    expr.Walk(c.arena, idx, func(nodeIdx expr.NodeIdx, node expr.ExprNode) {
        builder.WriteString(fmt.Sprintf("%T:", node.Kind))
        
        switch kind := node.Kind.(type) {
        case expr.LiteralExpr:
            builder.WriteString(fmt.Sprintf("%v", kind.Value))
        case expr.ColumnExpr:
            builder.WriteString(kind.Name)
        case expr.BinaryExpr:
            builder.WriteString(string(kind.Op))
        }
        
        builder.WriteString("|")
    })
    
    return builder.String()
}
```

## 4. Type System (`types/`)

### 4.1 Data Types

```go
// types/datatypes.go
package types

import "golars/expr"

// DataType represents the type of data in a column or expression
type DataType interface {
    String() string
    Equals(other DataType) bool
    IsNumeric() bool
    IsString() bool
    IsTemporal() bool
    IsNested() bool
}

// Concrete types
type Int64Type struct{}
func (Int64Type) String() string { return "Int64" }
func (Int64Type) Equals(other DataType) bool { _, ok := other.(Int64Type); return ok }
func (Int64Type) IsNumeric() bool { return true }
func (Int64Type) IsString() bool { return false }
func (Int64Type) IsTemporal() bool { return false }
func (Int64Type) IsNested() bool { return false }

type Float64Type struct{}
func (Float64Type) String() string { return "Float64" }
func (Float64Type) Equals(other DataType) bool { _, ok := other.(Float64Type); return ok }
func (Float64Type) IsNumeric() bool { return true }
func (Float64Type) IsString() bool { return false }
func (Float64Type) IsTemporal() bool { return false }
func (Float64Type) IsNested() bool { return false }

type StringType struct{}
func (StringType) String() string { return "String" }
func (StringType) Equals(other DataType) bool { _, ok := other.(StringType); return ok }
func (StringType) IsNumeric() bool { return false }
func (StringType) IsString() bool { return true }
func (StringType) IsTemporal() bool { return false }
func (StringType) IsNested() bool { return false }

type BoolType struct{}
func (BoolType) String() string { return "Bool" }
func (BoolType) Equals(other DataType) bool { _, ok := other.(BoolType); return ok }
func (BoolType) IsNumeric() bool { return false }
func (BoolType) IsString() bool { return false }
func (BoolType) IsTemporal() bool { return false }
func (BoolType) IsNested() bool { return false }

type DateTimeType struct{}
func (DateTimeType) String() string { return "DateTime" }
func (DateTimeType) Equals(other DataType) bool { _, ok := other.(DateTimeType); return ok }
func (DateTimeType) IsNumeric() bool { return false }
func (DateTimeType) IsString() bool { return false }
func (DateTimeType) IsTemporal() bool { return true }
func (DateTimeType) IsNested() bool { return false }

type ListType struct {
    Element DataType
}
func (l ListType) String() string { return fmt.Sprintf("List<%s>", l.Element.String()) }
func (l ListType) Equals(other DataType) bool {
    o, ok := other.(ListType)
    return ok && l.Element.Equals(o.Element)
}
func (ListType) IsNumeric() bool { return false }
func (ListType) IsString() bool { return false }
func (ListType) IsTemporal() bool { return false }
func (ListType) IsNested() bool { return true }

type StructType struct {
    Fields []StructField
}

type StructField struct {
    Name string
    Type DataType
}

func (s StructType) String() string {
    var fields []string
    for _, f := range s.Fields {
        fields = append(fields, fmt.Sprintf("%s: %s", f.Name, f.Type.String()))
    }
    return fmt.Sprintf("Struct{%s}", strings.Join(fields, ", "))
}

func (s StructType) Equals(other DataType) bool {
    o, ok := other.(StructType)
    if !ok || len(s.Fields) != len(o.Fields) {
        return false
    }
    for i, f := range s.Fields {
        if f.Name != o.Fields[i].Name || !f.Type.Equals(o.Fields[i].Type) {
            return false
        }
    }
    return true
}

func (StructType) IsNumeric() bool { return false }
func (StructType) IsString() bool { return false }
func (StructType) IsTemporal() bool { return false }
func (StructType) IsNested() bool { return true }
```

### 4.2 Type Coercion

```go
// types/coercion.go
package types

// Coerce finds a common type for two data types
func Coerce(left, right DataType) (DataType, error) {
    // Same type, no coercion needed
    if left.Equals(right) {
        return left, nil
    }
    
    // Numeric coercion
    if left.IsNumeric() && right.IsNumeric() {
        // Int64 + Float64 => Float64
        if _, ok := left.(Int64Type); ok {
            if _, ok := right.(Float64Type); ok {
                return Float64Type{}, nil
            }
        }
        if _, ok := left.(Float64Type); ok {
            if _, ok := right.(Int64Type); ok {
                return Float64Type{}, nil
            }
        }
    }
    
    // String coercion (everything can be converted to string)
    if left.IsString() || right.IsString() {
        return StringType{}, nil
    }
    
    return nil, fmt.Errorf("cannot coerce %s and %s", left.String(), right.String())
}

// CanCoerce checks if one type can be coerced to another
func CanCoerce(from, to DataType) bool {
    if from.Equals(to) {
        return true
    }
    
    // Int64 can be coerced to Float64
    if _, ok := from.(Int64Type); ok {
        if _, ok := to.(Float64Type); ok {
            return true
        }
    }
    
    // Everything can be coerced to String
    if _, ok := to.(StringType); ok {
        return true
    }
    
    return false
}
```

### 4.3 Type Inference

```go
// types/inference.go
package types

import "golars/expr"

// TypeInference performs type inference on expressions
type TypeInference struct {
    arena  *expr.ExprArena
    schema logical.Schema
}

func NewTypeInference(arena *expr.ExprArena, schema logical.Schema) *TypeInference {
    return &TypeInference{
        arena:  arena,
        schema: schema,
    }
}

// InferType determines the type of an expression
func (t *TypeInference) InferType(idx expr.NodeIdx) (DataType, error) {
    node := t.arena.Get(idx)
    
    // Check cached type
    if node.DataType != nil {
        return *node.DataType, nil
    }
    
    var dtype DataType
    var err error
    
    switch kind := node.Kind.(type) {
    case expr.LiteralExpr:
        dtype = t.inferLiteralType(kind.Value)
        
    case expr.ColumnExpr:
        dtype, err = t.inferColumnType(kind.Name)
        
    case expr.BinaryExpr:
        left, err := t.InferType(kind.Left)
        if err != nil {
            return nil, err
        }
        right, err := t.InferType(kind.Right)
        if err != nil {
            return nil, err
        }
        dtype, err = t.inferBinaryType(kind.Op, left, right)
        
    case expr.UnaryExpr:
        input, err := t.InferType(kind.Input)
        if err != nil {
            return nil, err
        }
        dtype = t.inferUnaryType(kind.Op, input)
        
    case expr.AggExpr:
        input, err := t.InferType(kind.Input)
        if err != nil {
            return nil, err
        }
        dtype = t.inferAggType(kind.Op, input)
        
    case expr.CastExpr:
        dtype = kind.DataType
        
    case expr.TernaryExpr:
        trueType, err := t.InferType(kind.TrueExpr)
        if err != nil {
            return nil, err
        }
        falseType, err := t.InferType(kind.FalseExpr)
        if err != nil {
            return nil, err
        }
        dtype, err = Coerce(trueType, falseType)
        
    default:
        return nil, fmt.Errorf("cannot infer type for expression kind %T", kind)
    }
    
    if err != nil {
        return nil, err
    }
    
    // Cache the type
    node.DataType = &dtype
    t.arena.nodes[idx] = node
    
    return dtype, nil
}

func (t *TypeInference) inferLiteralType(value interface{}) DataType {
    switch value.(type) {
    case int64:
        return Int64Type{}
    case float64:
        return Float64Type{}
    case string:
        return StringType{}
    case bool:
        return BoolType{}
    default:
        return nil
    }
}

func (t *TypeInference) inferColumnType(name string) (DataType, error) {
    for _, field := range t.schema.Fields {
        if field.Name == name {
            return field.DataType, nil
        }
    }
    return nil, fmt.Errorf("column %s not found in schema", name)
}

func (t *TypeInference) inferBinaryType(op expr.BinaryOp, left, right DataType) (DataType, error) {
    switch op {
    case expr.Add, expr.Subtract, expr.Multiply, expr.Divide:
        // Arithmetic operations
        return Coerce(left, right)
        
    case expr.Equal, expr.NotEqual, expr.Less, expr.LessEqual, expr.Greater, expr.GreaterEqual:
        // Comparison operations always return bool
        return BoolType{}, nil
        
    case expr.And, expr.Or:
        // Boolean operations
        return BoolType{}, nil
        
    default:
        return nil, fmt.Errorf("unknown binary operation %v", op)
    }
}

func (t *TypeInference) inferUnaryType(op expr.UnaryOp, input DataType) DataType {
    switch op {
    case expr.Not:
        return BoolType{}
    case expr.Negate:
        return input // Negation preserves type
    default:
        return input
    }
}

func (t *TypeInference) inferAggType(op expr.AggOp, input DataType) DataType {
    switch op {
    case expr.Count:
        return Int64Type{}
    case expr.Sum, expr.Mean, expr.Min, expr.Max:
        return input // Preserve input type
    case expr.Std, expr.Var:
        return Float64Type{} // Always float for statistical functions
    default:
        return input
    }
}
```

### 4.4 Type Coercion Optimizer

```go
// planner/type_coercion.go
package planner

type TypeCoercion struct{}

func (t *TypeCoercion) Name() string {
    return "TypeCoercion"
}

func (t *TypeCoercion) Optimize(plan logical.LogicalPlan, arena *expr.ExprArena) (logical.LogicalPlan, error) {
    inference := types.NewTypeInference(arena, plan.Schema())
    coercer := &typeCoercer{
        arena:     arena,
        inference: inference,
    }
    
    return coercer.coercePlan(plan)
}

type typeCoercer struct {
    arena     *expr.ExprArena
    inference *types.TypeInference
}

func (c *typeCoercer) coercePlan(plan logical.LogicalPlan) (logical.LogicalPlan, error) {
    // Apply type coercion to expressions in the plan
    switch node := plan.(type) {
    case *logical.Filter:
        // Ensure predicate is boolean
        predType, err := c.inference.InferType(node.Pred)
        if err != nil {
            return nil, err
        }
        
        if _, ok := predType.(types.BoolType); !ok {
            return nil, fmt.Errorf("filter predicate must be boolean, got %s", predType.String())
        }
        
    case *logical.Join:
        // Ensure join keys have compatible types
        if len(node.LeftOn) != len(node.RightOn) {
            return nil, fmt.Errorf("join key count mismatch")
        }
        
        for i := range node.LeftOn {
            leftType, err := c.inference.InferType(node.LeftOn[i])
            if err != nil {
                return nil, err
            }
            
            rightType, err := c.inference.InferType(node.RightOn[i])
            if err != nil {
                return nil, err
            }
            
            // Try to coerce to common type
            commonType, err := types.Coerce(leftType, rightType)
            if err != nil {
                return nil, fmt.Errorf("incompatible join key types: %v", err)
            }
            
            // Insert cast nodes if needed
            if !leftType.Equals(commonType) {
                node.LeftOn[i] = c.insertCast(node.LeftOn[i], commonType)
            }
            if !rightType.Equals(commonType) {
                node.RightOn[i] = c.insertCast(node.RightOn[i], commonType)
            }
        }
    }
    
    // Recursively process inputs
    inputs := plan.Inputs()
    for i, input := range inputs {
        coerced, err := c.coercePlan(input)
        if err != nil {
            return nil, err
        }
        inputs[i] = coerced
    }
    
    return plan, nil
}

func (c *typeCoercer) insertCast(expr expr.NodeIdx, targetType types.DataType) expr.NodeIdx {
    return c.arena.Add(expr.ExprNode{
        Kind: expr.CastExpr{
            Input:    expr,
            DataType: targetType,
        },
        DataType: &targetType,
    })
}
```

## 5. Physical Planning (`physical/`)

### 5.1 Physical Expression Interface

```go
// physical/expr.go
package physical

import (
    "golars/dataframe"
    "golars/series"
)

// PhysicalExpr represents an executable expression
type PhysicalExpr interface {
    // Evaluate returns a series for the entire batch
    Evaluate(batch *dataframe.DataFrame) (series.Series, error)
    
    // EvaluateScalar returns a single value
    EvaluateScalar(batch *dataframe.DataFrame) (interface{}, error)
    
    // RequiredColumns returns columns needed for evaluation
    RequiredColumns() []string
}

// Base implementation
type basePhysicalExpr struct{}

func (b basePhysicalExpr) EvaluateScalar(batch *dataframe.DataFrame) (interface{}, error) {
    // Default implementation: evaluate and get first value
    result, err := b.Evaluate(batch)
    if err != nil {
        return nil, err
    }
    
    if result.Len() == 0 {
        return nil, nil
    }
    
    return result.Get(0), nil
}
```

### 5.2 Physical Expression Types

```go
// physical/expr_types.go
package physical

// Column reference
type columnExpr struct {
    basePhysicalExpr
    name string
}

func (c *columnExpr) Evaluate(batch *dataframe.DataFrame) (series.Series, error) {
    return batch.Column(c.name)
}

func (c *columnExpr) RequiredColumns() []string {
    return []string{c.name}
}

// Literal value
type literalExpr struct {
    basePhysicalExpr
    value interface{}
    dtype types.DataType
}

func (l *literalExpr) Evaluate(batch *dataframe.DataFrame) (series.Series, error) {
    // Create a series filled with the literal value
    length := batch.Height()
    values := make([]interface{}, length)
    for i := range values {
        values[i] = l.value
    }
    
    return series.NewSeries(l.dtype, values)
}

func (l *literalExpr) RequiredColumns() []string {
    return nil
}

// Binary operation
type binaryExpr struct {
    basePhysicalExpr
    op    expr.BinaryOp
    left  PhysicalExpr
    right PhysicalExpr
}

func (b *binaryExpr) Evaluate(batch *dataframe.DataFrame) (series.Series, error) {
    leftResult, err := b.left.Evaluate(batch)
    if err != nil {
        return nil, err
    }
    
    rightResult, err := b.right.Evaluate(batch)
    if err != nil {
        return nil, err
    }
    
    // Apply operation based on type
    switch b.op {
    case expr.Add:
        return leftResult.Add(rightResult)
    case expr.Subtract:
        return leftResult.Subtract(rightResult)
    case expr.Multiply:
        return leftResult.Multiply(rightResult)
    case expr.Divide:
        return leftResult.Divide(rightResult)
    case expr.Equal:
        return leftResult.Equal(rightResult)
    case expr.NotEqual:
        return leftResult.NotEqual(rightResult)
    case expr.Less:
        return leftResult.Less(rightResult)
    case expr.Greater:
        return leftResult.Greater(rightResult)
    case expr.And:
        return leftResult.And(rightResult)
    case expr.Or:
        return leftResult.Or(rightResult)
    default:
        return nil, fmt.Errorf("unsupported binary operation: %v", b.op)
    }
}

func (b *binaryExpr) RequiredColumns() []string {
    cols := make(map[string]bool)
    for _, col := range b.left.RequiredColumns() {
        cols[col] = true
    }
    for _, col := range b.right.RequiredColumns() {
        cols[col] = true
    }
    
    result := make([]string, 0, len(cols))
    for col := range cols {
        result = append(result, col)
    }
    return result
}

// Aggregation
type aggExpr struct {
    basePhysicalExpr
    op    expr.AggOp
    input PhysicalExpr
}

func (a *aggExpr) Evaluate(batch *dataframe.DataFrame) (series.Series, error) {
    input, err := a.input.Evaluate(batch)
    if err != nil {
        return nil, err
    }
    
    var result interface{}
    switch a.op {
    case expr.Sum:
        result = input.Sum()
    case expr.Mean:
        result = input.Mean()
    case expr.Min:
        result = input.Min()
    case expr.Max:
        result = input.Max()
    case expr.Count:
        result = int64(input.Len())
    case expr.Std:
        result = input.Std()
    case expr.Var:
        result = input.Var()
    default:
        return nil, fmt.Errorf("unsupported aggregation: %v", a.op)
    }
    
    // Return scalar as single-element series
    return series.NewSeries(input.DataType(), []interface{}{result})
}

func (a *aggExpr) RequiredColumns() []string {
    return a.input.RequiredColumns()
}
```

### 5.3 Expression Compiler

```go
// physical/compiler.go
package physical

import (
    "golars/expr"
    "golars/logical"
)

// ExpressionCompiler converts logical expressions to physical
type ExpressionCompiler struct {
    arena   *expr.ExprArena
    schema  logical.Schema
    context logical.EvalContext
}

func NewExpressionCompiler(arena *expr.ExprArena, schema logical.Schema, context logical.EvalContext) *ExpressionCompiler {
    return &ExpressionCompiler{
        arena:   arena,
        schema:  schema,
        context: context,
    }
}

// Compile converts a logical expression to physical
func (c *ExpressionCompiler) Compile(idx expr.NodeIdx) (PhysicalExpr, error) {
    node := c.arena.Get(idx)
    
    switch kind := node.Kind.(type) {
    case expr.LiteralExpr:
        return &literalExpr{
            value: kind.Value,
            dtype: *node.DataType,
        }, nil
        
    case expr.ColumnExpr:
        return &columnExpr{
            name: kind.Name,
        }, nil
        
    case expr.BinaryExpr:
        left, err := c.Compile(kind.Left)
        if err != nil {
            return nil, err
        }
        
        right, err := c.Compile(kind.Right)
        if err != nil {
            return nil, err
        }
        
        return &binaryExpr{
            op:    kind.Op,
            left:  left,
            right: right,
        }, nil
        
    case expr.UnaryExpr:
        input, err := c.Compile(kind.Input)
        if err != nil {
            return nil, err
        }
        
        return &unaryExpr{
            op:    kind.Op,
            input: input,
        }, nil
        
    case expr.AggExpr:
        if c.context != logical.AggregationContext {
            return nil, fmt.Errorf("aggregation expression outside aggregation context")
        }
        
        input, err := c.Compile(kind.Input)
        if err != nil {
            return nil, err
        }
        
        return &aggExpr{
            op:    kind.Op,
            input: input,
        }, nil
        
    case expr.CastExpr:
        input, err := c.Compile(kind.Input)
        if err != nil {
            return nil, err
        }
        
        return &castExpr{
            input:    input,
            dataType: kind.DataType,
        }, nil
        
    case expr.TernaryExpr:
        condition, err := c.Compile(kind.Condition)
        if err != nil {
            return nil, err
        }
        
        trueExpr, err := c.Compile(kind.TrueExpr)
        if err != nil {
            return nil, err
        }
        
        falseExpr, err := c.Compile(kind.FalseExpr)
        if err != nil {
            return nil, err
        }
        
        return &ternaryExpr{
            condition: condition,
            trueExpr:  trueExpr,
            falseExpr: falseExpr,
        }, nil
        
    default:
        return nil, fmt.Errorf("unsupported expression kind: %T", kind)
    }
}

// CompileMultiple compiles multiple expressions
func (c *ExpressionCompiler) CompileMultiple(indices []expr.NodeIdx) ([]PhysicalExpr, error) {
    result := make([]PhysicalExpr, len(indices))
    
    for i, idx := range indices {
        expr, err := c.Compile(idx)
        if err != nil {
            return nil, err
        }
        result[i] = expr
    }
    
    return result, nil
}
```

### 5.4 Physical Plan Interface

```go
// physical/plan.go
package physical

import (
    "context"
    "golars/dataframe"
)

// PhysicalPlan represents an executable query plan
type PhysicalPlan interface {
    // Execute runs the plan and returns results
    Execute(ctx context.Context) (*dataframe.DataFrame, error)
    
    // Schema returns the output schema
    Schema() logical.Schema
    
    // Children returns child plans
    Children() []PhysicalPlan
}

// Base implementation
type basePlan struct {
    schema   logical.Schema
    children []PhysicalPlan
}

func (b *basePlan) Schema() logical.Schema {
    return b.schema
}

func (b *basePlan) Children() []PhysicalPlan {
    return b.children
}
```

### 5.5 Physical Plan Nodes

```go
// physical/nodes.go
package physical

// Scan reads from a data source
type ScanExec struct {
    basePlan
    source      DataSource
    projections []string
    predicates  []PhysicalExpr
}

func (s *ScanExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Read data from source with projections
    df, err := s.source.Read(ctx, s.projections)
    if err != nil {
        return nil, err
    }
    
    // Apply predicates
    for _, pred := range s.predicates {
        mask, err := pred.Evaluate(df)
        if err != nil {
            return nil, err
        }
        
        df, err = df.Filter(mask)
        if err != nil {
            return nil, err
        }
    }
    
    return df, nil
}

// Projection transforms columns
type ProjectionExec struct {
    basePlan
    input PhysicalPlan
    exprs []PhysicalExpr
    names []string
}

func (p *ProjectionExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Execute input
    input, err := p.input.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    // Evaluate expressions
    columns := make([]series.Series, len(p.exprs))
    for i, expr := range p.exprs {
        col, err := expr.Evaluate(input)
        if err != nil {
            return nil, err
        }
        columns[i] = col
    }
    
    // Build new dataframe
    return dataframe.NewDataFrame(columns, p.names), nil
}

// Filter applies predicates
type FilterExec struct {
    basePlan
    input     PhysicalPlan
    predicate PhysicalExpr
}

func (f *FilterExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Execute input
    input, err := f.input.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    // Evaluate predicate
    mask, err := f.predicate.Evaluate(input)
    if err != nil {
        return nil, err
    }
    
    // Apply filter
    return input.Filter(mask)
}

// Aggregate performs grouping and aggregation
type AggregateExec struct {
    basePlan
    input     PhysicalPlan
    groupKeys []PhysicalExpr
    aggExprs  []PhysicalExpr
}

func (a *AggregateExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Execute input
    input, err := a.input.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    // Evaluate group keys
    groupCols := make([]series.Series, len(a.groupKeys))
    for i, key := range a.groupKeys {
        col, err := key.Evaluate(input)
        if err != nil {
            return nil, err
        }
        groupCols[i] = col
    }
    
    // Group data
    groups := input.GroupBy(groupCols)
    
    // Apply aggregations to each group
    results := make([]*dataframe.DataFrame, 0)
    
    for _, group := range groups {
        // Evaluate aggregations
        aggResults := make([]series.Series, len(a.aggExprs))
        for i, agg := range a.aggExprs {
            result, err := agg.Evaluate(group)
            if err != nil {
                return nil, err
            }
            aggResults[i] = result
        }
        
        // Combine group keys and aggregation results
        allCols := append(groupCols, aggResults...)
        groupResult := dataframe.NewDataFrame(allCols, a.schema.FieldNames())
        results = append(results, groupResult)
    }
    
    // Concatenate all groups
    return dataframe.Concat(results), nil
}

// Join combines two inputs
type JoinExec struct {
    basePlan
    left      PhysicalPlan
    right     PhysicalPlan
    leftKeys  []PhysicalExpr
    rightKeys []PhysicalExpr
    joinType  logical.JoinType
}

func (j *JoinExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Execute both inputs
    leftDF, err := j.left.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    rightDF, err := j.right.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    // Evaluate join keys
    leftKeyCols := make([]series.Series, len(j.leftKeys))
    for i, key := range j.leftKeys {
        col, err := key.Evaluate(leftDF)
        if err != nil {
            return nil, err
        }
        leftKeyCols[i] = col
    }
    
    rightKeyCols := make([]series.Series, len(j.rightKeys))
    for i, key := range j.rightKeys {
        col, err := key.Evaluate(rightDF)
        if err != nil {
            return nil, err
        }
        rightKeyCols[i] = col
    }
    
    // Perform join based on type
    switch j.joinType {
    case logical.InnerJoin:
        return leftDF.InnerJoin(rightDF, leftKeyCols, rightKeyCols)
    case logical.LeftJoin:
        return leftDF.LeftJoin(rightDF, leftKeyCols, rightKeyCols)
    case logical.RightJoin:
        return leftDF.RightJoin(rightDF, leftKeyCols, rightKeyCols)
    case logical.OuterJoin:
        return leftDF.OuterJoin(rightDF, leftKeyCols, rightKeyCols)
    default:
        return nil, fmt.Errorf("unsupported join type: %v", j.joinType)
    }
}

// Sort orders the output
type SortExec struct {
    basePlan
    input   PhysicalPlan
    sortBy  []PhysicalExpr
    reverse []bool
}

func (s *SortExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Execute input
    input, err := s.input.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    // Evaluate sort expressions
    sortCols := make([]series.Series, len(s.sortBy))
    for i, expr := range s.sortBy {
        col, err := expr.Evaluate(input)
        if err != nil {
            return nil, err
        }
        sortCols[i] = col
    }
    
    // Perform sort
    return input.Sort(sortCols, s.reverse)
}

// Limit restricts output rows
type LimitExec struct {
    basePlan
    input  PhysicalPlan
    offset int64
    limit  int64
}

func (l *LimitExec) Execute(ctx context.Context) (*dataframe.DataFrame, error) {
    // Execute input
    input, err := l.input.Execute(ctx)
    if err != nil {
        return nil, err
    }
    
    // Apply offset and limit
    start := int(l.offset)
    end := start + int(l.limit)
    
    if start >= input.Height() {
        // Return empty dataframe with same schema
        return dataframe.NewEmpty(l.schema), nil
    }
    
    if end > input.Height() {
        end = input.Height()
    }
    
    return input.Slice(start, end)
}
```

### 5.6 Plan Compiler

```go
// physical/plan_compiler.go
package physical

import (
    "golars/expr"
    "golars/logical"
)

// PlanCompiler converts logical plans to physical plans
type PlanCompiler struct {
    arena *expr.ExprArena
}

func NewPlanCompiler(arena *expr.ExprArena) *PlanCompiler {
    return &PlanCompiler{
        arena: arena,
    }
}

// Compile converts a logical plan to physical
func (c *PlanCompiler) Compile(plan logical.LogicalPlan) (PhysicalPlan, error) {
    switch node := plan.(type) {
    case *logical.Scan:
        predicates := make([]PhysicalExpr, len(node.Predicates))
        compiler := NewExpressionCompiler(c.arena, node.Schema(), logical.DefaultContext)
        
        for i, pred := range node.Predicates {
            compiled, err := compiler.Compile(pred)
            if err != nil {
                return nil, err
            }
            predicates[i] = compiled
        }
        
        return &ScanExec{
            basePlan: basePlan{
                schema: node.Schema(),
            },
            source:      node.Source,
            projections: node.Projections,
            predicates:  predicates,
        }, nil
        
    case *logical.Projection:
        input, err := c.Compile(node.Input)
        if err != nil {
            return nil, err
        }
        
        compiler := NewExpressionCompiler(c.arena, node.Input.Schema(), logical.DefaultContext)
        exprs, err := compiler.CompileMultiple(node.Exprs)
        if err != nil {
            return nil, err
        }
        
        names := make([]string, len(node.Exprs))
        for i, exprIdx := range node.Exprs {
            exprNode := c.arena.Get(exprIdx)
            names[i] = exprNode.Output.Value
        }
        
        return &ProjectionExec{
            basePlan: basePlan{
                schema:   node.Schema(),
                children: []PhysicalPlan{input},
            },
            input: input,
            exprs: exprs,
            names: names,
        }, nil
        
    case *logical.Filter:
        input, err := c.Compile(node.Input)
        if err != nil {
            return nil, err
        }
        
        compiler := NewExpressionCompiler(c.arena, node.Input.Schema(), logical.FilterContext)
        predicate, err := compiler.Compile(node.Pred)
        if err != nil {
            return nil, err
        }
        
        return &FilterExec{
            basePlan: basePlan{
                schema:   node.Schema(),
                children: []PhysicalPlan{input},
            },
            input:     input,
            predicate: predicate,
        }, nil
        
    case *logical.Aggregate:
        input, err := c.Compile(node.Input)
        if err != nil {
            return nil, err
        }
        
        // Compile group keys
        keyCompiler := NewExpressionCompiler(c.arena, node.Input.Schema(), logical.GroupByContext)
        groupKeys, err := keyCompiler.CompileMultiple(node.GroupExpr)
        if err != nil {
            return nil, err
        }
        
        // Compile aggregations
        aggCompiler := NewExpressionCompiler(c.arena, node.Input.Schema(), logical.AggregationContext)
        aggExprs, err := aggCompiler.CompileMultiple(node.AggExprs)
        if err != nil {
            return nil, err
        }
        
        return &AggregateExec{
            basePlan: basePlan{
                schema:   node.Schema(),
                children: []PhysicalPlan{input},
            },
            input:     input,
            groupKeys: groupKeys,
            aggExprs:  aggExprs,
        }, nil
        
    case *logical.Join:
        left, err := c.Compile(node.Left)
        if err != nil {
            return nil, err
        }
        
        right, err := c.Compile(node.Right)
        if err != nil {
            return nil, err
        }
        
        leftCompiler := NewExpressionCompiler(c.arena, node.Left.Schema(), logical.DefaultContext)
        leftKeys, err := leftCompiler.CompileMultiple(node.LeftOn)
        if err != nil {
            return nil, err
        }
        
        rightCompiler := NewExpressionCompiler(c.arena, node.Right.Schema(), logical.DefaultContext)
        rightKeys, err := rightCompiler.CompileMultiple(node.RightOn)
        if err != nil {
            return nil, err
        }
        
        return &JoinExec{
            basePlan: basePlan{
                schema:   node.Schema(),
                children: []PhysicalPlan{left, right},
            },
            left:      left,
            right:     right,
            leftKeys:  leftKeys,
            rightKeys: rightKeys,
            joinType:  node.JoinType,
        }, nil
        
    case *logical.Sort:
        input, err := c.Compile(node.Input)
        if err != nil {
            return nil, err
        }
        
        compiler := NewExpressionCompiler(c.arena, node.Input.Schema(), logical.DefaultContext)
        sortBy, err := compiler.CompileMultiple(node.SortBy)
        if err != nil {
            return nil, err
        }
        
        // Extract reverse flags from sort expressions
        reverse := make([]bool, len(sortBy))
        for i, exprIdx := range node.SortBy {
            exprNode := c.arena.Get(exprIdx)
            if sortExpr, ok := exprNode.Kind.(expr.SortExpr); ok {
                reverse[i] = sortExpr.Descending
            }
        }
        
        return &SortExec{
            basePlan: basePlan{
                schema:   node.Schema(),
                children: []PhysicalPlan{input},
            },
            input:   input,
            sortBy:  sortBy,
            reverse: reverse,
        }, nil
        
    case *logical.Limit:
        input, err := c.Compile(node.Input)
        if err != nil {
            return nil, err
        }
        
        return &LimitExec{
            basePlan: basePlan{
                schema:   node.Schema(),
                children: []PhysicalPlan{input},
            },
            input:  input,
            offset: node.Offset,
            limit:  node.Limit,
        }, nil
        
    default:
        return nil, fmt.Errorf("unsupported logical plan node: %T", node)
    }
}
```

## 6. Execution Engine (`engine/`)

### 6.1 Query Engine

```go
// engine/engine.go
package engine

import (
    "context"
    "golars/dataframe"
    "golars/expr"
    "golars/logical"
    "golars/physical"
    "golars/planner"
)

// QueryEngine executes lazy queries
type QueryEngine struct {
    optimizers *planner.OptimizerPipeline
    compiler   *physical.PlanCompiler
}

func NewQueryEngine() *QueryEngine {
    return &QueryEngine{
        optimizers: planner.NewOptimizerPipeline(),
        compiler:   nil, // Set during execution when arena is available
    }
}

// Execute runs a logical plan and returns results
func (e *QueryEngine) Execute(ctx context.Context, plan logical.LogicalPlan, arena *expr.ExprArena) (*dataframe.DataFrame, error) {
    // Run optimizations
    optimized, err := e.optimizers.Optimize(plan, arena)
    if err != nil {
        return nil, fmt.Errorf("optimization failed: %w", err)
    }
    
    // Compile to physical plan
    e.compiler = physical.NewPlanCompiler(arena)
    physicalPlan, err := e.compiler.Compile(optimized)
    if err != nil {
        return nil, fmt.Errorf("compilation failed: %w", err)
    }
    
    // Execute physical plan
    result, err := physicalPlan.Execute(ctx)
    if err != nil {
        return nil, fmt.Errorf("execution failed: %w", err)
    }
    
    return result, nil
}

// Explain returns the query plan as a string
func (e *QueryEngine) Explain(plan logical.LogicalPlan, arena *expr.ExprArena, verbose bool) (string, error) {
    var builder strings.Builder
    
    builder.WriteString("=== Logical Plan ===\n")
    builder.WriteString(e.explainLogical(plan, "", verbose))
    
    // Run optimizations
    optimized, err := e.optimizers.Optimize(plan, arena)
    if err != nil {
        return "", err
    }
    
    builder.WriteString("\n=== Optimized Logical Plan ===\n")
    builder.WriteString(e.explainLogical(optimized, "", verbose))
    
    // Compile to physical
    e.compiler = physical.NewPlanCompiler(arena)
    physicalPlan, err := e.compiler.Compile(optimized)
    if err != nil {
        return "", err
    }
    
    builder.WriteString("\n=== Physical Plan ===\n")
    builder.WriteString(e.explainPhysical(physicalPlan, "", verbose))
    
    return builder.String(), nil
}

func (e *QueryEngine) explainLogical(plan logical.LogicalPlan, indent string, verbose bool) string {
    var builder strings.Builder
    
    builder.WriteString(indent)
    builder.WriteString(plan.Describe())
    
    if verbose {
        schema := plan.Schema()
        builder.WriteString(" [")
        for i, field := range schema.Fields {
            if i > 0 {
                builder.WriteString(", ")
            }
            builder.WriteString(field.Name)
            builder.WriteString(": ")
            builder.WriteString(field.DataType.String())
        }
        builder.WriteString("]")
    }
    
    builder.WriteString("\n")
    
    // Explain children
    for _, child := range plan.Inputs() {
        builder.WriteString(e.explainLogical(child, indent+"  ", verbose))
    }
    
    return builder.String()
}

func (e *QueryEngine) explainPhysical(plan physical.PhysicalPlan, indent string, verbose bool) string {
    var builder strings.Builder
    
    builder.WriteString(indent)
    builder.WriteString(fmt.Sprintf("%T", plan))
    
    if verbose {
        schema := plan.Schema()
        builder.WriteString(" [")
        for i, field := range schema.Fields {
            if i > 0 {
                builder.WriteString(", ")
            }
            builder.WriteString(field.Name)
            builder.WriteString(": ")
            builder.WriteString(field.DataType.String())
        }
        builder.WriteString("]")
    }
    
    builder.WriteString("\n")
    
    // Explain children
    for _, child := range plan.Children() {
        builder.WriteString(e.explainPhysical(child, indent+"  ", verbose))
    }
    
    return builder.String()
}
```

### 6.2 LazyFrame API

```go
// arena/lazyframe.go
package arena

import (
    "context"
    "golars/dataframe"
    "golars/engine"
    "golars/expr"
    "golars/logical"
)

// LazyFrame represents a lazy query
type LazyFrame struct {
    plan   logical.LogicalPlan
    arena  *expr.ExprArena
    engine *engine.QueryEngine
}

// NewLazyFrame creates a new lazy frame from a data source
func NewLazyFrame(source logical.DataSource) *LazyFrame {
    arena := &expr.ExprArena{}
    
    return &LazyFrame{
        plan: &logical.Scan{
            Source: source,
            Arena:  arena,
        },
        arena:  arena,
        engine: engine.NewQueryEngine(),
    }
}

// Select transforms columns
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
    exprNodes := make([]expr.NodeIdx, len(exprs))
    for i, e := range exprs {
        exprNodes[i] = e.ToNode(lf.arena)
    }
    
    return &LazyFrame{
        plan: &logical.Projection{
            Input: lf.plan,
            Exprs: exprNodes,
            Arena: lf.arena,
        },
        arena:  lf.arena,
        engine: lf.engine,
    }
}

// Filter applies predicates
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
    return &LazyFrame{
        plan: &logical.Filter{
            Input: lf.plan,
            Pred:  predicate.ToNode(lf.arena),
            Arena: lf.arena,
        },
        arena:  lf.arena,
        engine: lf.engine,
    }
}

// GroupBy starts a grouped operation
func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
    keyNodes := make([]expr.NodeIdx, len(keys))
    for i, k := range keys {
        keyNodes[i] = k.ToNode(lf.arena)
    }
    
    return &LazyGroupBy{
        input:   lf.plan,
        keys:    keyNodes,
        arena:   lf.arena,
        engine:  lf.engine,
    }
}

// Join combines with another lazy frame
func (lf *LazyFrame) Join(other *LazyFrame, leftOn, rightOn []Expr, how string) *LazyFrame {
    leftKeys := make([]expr.NodeIdx, len(leftOn))
    for i, k := range leftOn {
        leftKeys[i] = k.ToNode(lf.arena)
    }
    
    rightKeys := make([]expr.NodeIdx, len(rightOn))
    for i, k := range rightOn {
        rightKeys[i] = k.ToNode(lf.arena)
    }
    
    joinType := logical.InnerJoin
    switch how {
    case "left":
        joinType = logical.LeftJoin
    case "right":
        joinType = logical.RightJoin
    case "outer":
        joinType = logical.OuterJoin
    }
    
    return &LazyFrame{
        plan: &logical.Join{
            Left:     lf.plan,
            Right:    other.plan,
            LeftOn:   leftKeys,
            RightOn:  rightKeys,
            JoinType: joinType,
            Arena:    lf.arena,
        },
        arena:  lf.arena,
        engine: lf.engine,
    }
}

// Sort orders the output
func (lf *LazyFrame) Sort(by ...Expr) *LazyFrame {
    sortNodes := make([]expr.NodeIdx, len(by))
    for i, e := range by {
        sortNodes[i] = e.ToNode(lf.arena)
    }
    
    return &LazyFrame{
        plan: &logical.Sort{
            Input:  lf.plan,
            SortBy: sortNodes,
            Arena:  lf.arena,
        },
        arena:  lf.arena,
        engine: lf.engine,
    }
}

// Limit restricts output rows
func (lf *LazyFrame) Limit(n int) *LazyFrame {
    return &LazyFrame{
        plan: &logical.Limit{
            Input:  lf.plan,
            Offset: 0,
            Limit:  int64(n),
        },
        arena:  lf.arena,
        engine: lf.engine,
    }
}

// Collect executes the query
func (lf *LazyFrame) Collect(ctx context.Context) (*dataframe.DataFrame, error) {
    return lf.engine.Execute(ctx, lf.plan, lf.arena)
}

// Explain shows the query plan
func (lf *LazyFrame) Explain(verbose bool) (string, error) {
    return lf.engine.Explain(lf.plan, lf.arena, verbose)
}

// LazyGroupBy represents a grouped lazy operation
type LazyGroupBy struct {
    input  logical.LogicalPlan
    keys   []expr.NodeIdx
    arena  *expr.ExprArena
    engine *engine.QueryEngine
}

// Agg applies aggregations
func (g *LazyGroupBy) Agg(aggs ...Expr) *LazyFrame {
    aggNodes := make([]expr.NodeIdx, len(aggs))
    for i, a := range aggs {
        aggNodes[i] = a.ToNode(g.arena)
    }
    
    return &LazyFrame{
        plan: &logical.Aggregate{
            Input:     g.input,
            GroupExpr: g.keys,
            AggExprs:  aggNodes,
            Arena:     g.arena,
        },
        arena:  g.arena,
        engine: g.engine,
    }
}
```

### 6.3 Expression Builder API

```go
// arena/expr_api.go
package arena

import "golars/expr"

// Expr is the user-facing expression interface
type Expr interface {
    // Convert to arena node for lazy evaluation
    ToNode(arena *expr.ExprArena) expr.NodeIdx
}

// Column creates a column reference
func Col(name string) Expr {
    return &columnExpr{name: name}
}

// Lit creates a literal value
func Lit(value interface{}) Expr {
    return &literalExpr{value: value}
}

// Expression implementations
type columnExpr struct {
    name string
}

func (c *columnExpr) ToNode(arena *expr.ExprArena) expr.NodeIdx {
    return arena.Add(expr.ExprNode{
        Kind:   expr.ColumnExpr{Name: c.name},
        Output: expr.NewColumnOutput(c.name),
    })
}

type literalExpr struct {
    value interface{}
}

func (l *literalExpr) ToNode(arena *expr.ExprArena) expr.NodeIdx {
    return arena.Add(expr.ExprNode{
        Kind: expr.LiteralExpr{Value: l.value},
    })
}

// Binary expression builder
type binaryExpr struct {
    left  Expr
    right Expr
    op    expr.BinaryOp
}

func (b *binaryExpr) ToNode(arena *expr.ExprArena) expr.NodeIdx {
    leftIdx := b.left.ToNode(arena)
    rightIdx := b.right.ToNode(arena)
    
    return arena.Add(expr.ExprNode{
        Kind: expr.BinaryExpr{
            Op:    b.op,
            Left:  leftIdx,
            Right: rightIdx,
        },
        Children: []expr.NodeIdx{leftIdx, rightIdx},
    })
}

// Add arithmetic operations to Expr
func Add(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Add}
}

func Subtract(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Subtract}
}

func Multiply(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Multiply}
}

func Divide(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Divide}
}

// Add comparison operations
func Equal(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Equal}
}

func NotEqual(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.NotEqual}
}

func Less(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Less}
}

func Greater(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Greater}
}

// Add logical operations
func And(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.And}
}

func Or(left, right Expr) Expr {
    return &binaryExpr{left: left, right: right, op: expr.Or}
}

// Aggregation expressions
type aggExpr struct {
    input Expr
    op    expr.AggOp
}

func (a *aggExpr) ToNode(arena *expr.ExprArena) expr.NodeIdx {
    inputIdx := a.input.ToNode(arena)
    
    return arena.Add(expr.ExprNode{
        Kind: expr.AggExpr{
            Op:    a.op,
            Input: inputIdx,
        },
        Children: []expr.NodeIdx{inputIdx},
    })
}

func Sum(expr Expr) Expr {
    return &aggExpr{input: expr, op: expr.Sum}
}

func Mean(expr Expr) Expr {
    return &aggExpr{input: expr, op: expr.Mean}
}

func Min(expr Expr) Expr {
    return &aggExpr{input: expr, op: expr.Min}
}

func Max(expr Expr) Expr {
    return &aggExpr{input: expr, op: expr.Max}
}

func Count(expr Expr) Expr {
    return &aggExpr{input: expr, op: expr.Count}
}

// Conditional expressions
func When(condition Expr) *WhenBuilder {
    return &WhenBuilder{condition: condition}
}

type WhenBuilder struct {
    condition Expr
    thenExpr  Expr
}

func (w *WhenBuilder) Then(expr Expr) *ThenBuilder {
    return &ThenBuilder{
        condition: w.condition,
        thenExpr:  expr,
    }
}

type ThenBuilder struct {
    condition Expr
    thenExpr  Expr
}

func (t *ThenBuilder) Otherwise(expr Expr) Expr {
    return &ternaryExpr{
        condition: t.condition,
        thenExpr:  t.thenExpr,
        elseExpr:  expr,
    }
}

type ternaryExpr struct {
    condition Expr
    thenExpr  Expr
    elseExpr  Expr
}

func (t *ternaryExpr) ToNode(arena *expr.ExprArena) expr.NodeIdx {
    condIdx := t.condition.ToNode(arena)
    thenIdx := t.thenExpr.ToNode(arena)
    elseIdx := t.elseExpr.ToNode(arena)
    
    return arena.Add(expr.ExprNode{
        Kind: expr.TernaryExpr{
            Condition: condIdx,
            TrueExpr:  thenIdx,
            FalseExpr: elseIdx,
        },
        Children: []expr.NodeIdx{condIdx, thenIdx, elseIdx},
    })
}

// Alias support
type aliasExpr struct {
    expr  Expr
    alias string
}

func (a *aliasExpr) ToNode(arena *expr.ExprArena) expr.NodeIdx {
    idx := a.expr.ToNode(arena)
    
    // Update the output name
    node := arena.Get(idx)
    node.Output = expr.NewAlias(a.alias)
    arena.nodes[idx] = node
    
    return idx
}

func Alias(expr Expr, name string) Expr {
    return &aliasExpr{expr: expr, alias: name}
}
```

## 7. Integration Examples

### 7.1 Simple Query

```go
// Create a lazy frame from CSV
lf := arena.NewLazyFrame(NewCSVSource("data.csv"))

// Build query
result := lf.
    Filter(arena.Greater(arena.Col("age"), arena.Lit(25))).
    Select(
        arena.Col("name"),
        arena.Col("age"),
        arena.Alias(
            arena.Multiply(arena.Col("salary"), arena.Lit(1.1)),
            "new_salary",
        ),
    ).
    Sort(arena.Col("age")).
    Limit(10)

// Execute
df, err := result.Collect(context.Background())
```

### 7.2 Aggregation Query

```go
// Group by department and calculate statistics
result := lf.
    GroupBy(arena.Col("department")).
    Agg(
        arena.Alias(arena.Count(arena.Col("employee_id")), "count"),
        arena.Alias(arena.Mean(arena.Col("salary")), "avg_salary"),
        arena.Alias(arena.Max(arena.Col("age")), "max_age"),
    )

df, err := result.Collect(context.Background())
```

### 7.3 Join Query

```go
// Join two tables
employees := arena.NewLazyFrame(NewCSVSource("employees.csv"))
departments := arena.NewLazyFrame(NewCSVSource("departments.csv"))

result := employees.Join(
    departments,
    []arena.Expr{arena.Col("dept_id")},
    []arena.Expr{arena.Col("id")},
    "left",
).Select(
    arena.Col("employee_name"),
    arena.Col("department_name"),
    arena.Col("salary"),
)

df, err := result.Collect(context.Background())
```

### 7.4 Complex Expression

```go
// Conditional salary adjustment
result := lf.Select(
    arena.Col("name"),
    arena.Col("department"),
    arena.Alias(
        arena.When(arena.Equal(arena.Col("department"), arena.Lit("Engineering"))).
            Then(arena.Multiply(arena.Col("salary"), arena.Lit(1.15))).
            Otherwise(
                arena.When(arena.Equal(arena.Col("department"), arena.Lit("Sales"))).
                    Then(arena.Multiply(arena.Col("salary"), arena.Lit(1.10))).
                    Otherwise(arena.Col("salary")),
            ),
        "adjusted_salary",
    ),
)

df, err := result.Collect(context.Background())
```

## 8. Testing Strategy

### 8.1 Unit Tests

1. **Expression Arena Tests**
   - Node creation and retrieval
   - Expression traversal
   - Arena memory management

2. **Expression Analysis Tests**
   - Column extraction
   - Aggregation detection
   - Constant folding

3. **Type Inference Tests**
   - Basic type inference
   - Type coercion
   - Error cases

4. **Optimizer Tests**
   - Predicate pushdown
   - Projection pushdown
   - Expression simplification
   - CSE

### 8.2 Integration Tests

1. **End-to-End Query Tests**
   - Simple select/filter
   - Aggregations
   - Joins
   - Window functions

2. **Performance Tests**
   - Large dataset handling
   - Optimization effectiveness
   - Memory usage

3. **Compatibility Tests**
   - Existing DataFrame API
   - I/O integration
   - Type system

## 9. Migration Path

### Phase 1: Foundation (Weeks 1-2)
- Implement expression arena
- Add visitor pattern
- Create basic expression types

### Phase 2: Logical Planning (Weeks 3-4)
- Implement logical plan nodes
- Add context system
- Create plan builder

### Phase 3: Optimization (Weeks 5-6)
- Implement core optimizers
- Add optimizer pipeline
- Create plan explanation

### Phase 4: Physical Execution (Weeks 7-8)
- Implement physical expressions
- Add physical plan nodes
- Create execution engine

### Phase 5: API Integration (Weeks 9-10)
- Implement LazyFrame API
- Add expression builders
- Integrate with DataFrame

### Phase 6: Advanced Features (Weeks 11-12)
- Add window functions
- Implement expression expansion
- Add advanced optimizations

## 10. Performance Considerations

1. **Arena Allocation**
   - Pre-allocate arena capacity
   - Use object pools for temporary allocations
   - Implement arena compaction

2. **Expression Caching**
   - Cache type information
   - Memoize expression analysis
   - Share common subexpressions

3. **Parallel Execution**
   - Partition-aware operations
   - Concurrent expression evaluation
   - Pipeline parallelism

4. **Memory Management**
   - Streaming execution where possible
   - Batch size tuning
   - Resource limits

## Conclusion

This arena-based lazy evaluation system addresses all the issues that led to the removal of the previous implementation while providing a solid foundation for advanced query optimization and execution. The design is modular, extensible, and maintains Go idioms throughout.

The key improvements include:
- **Transparent expression AST** with full visitor pattern support
- **Arena-based allocation** for efficient memory usage
- **Context-sensitive evaluation** for proper expression handling
- **Pluggable optimizer pipeline** for extensibility
- **Clean separation** between logical and physical planning

This design enables Golars to match Polars' sophisticated lazy evaluation capabilities while avoiding the pitfalls of the previous implementation.