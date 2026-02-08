# lazy -- Query Planner

## Purpose

Provides lazy evaluation: build a query plan as a tree of logical operations,
optimize it, compile to a physical plan, then execute. This is how
`LazyFrame.Filter().Select().Collect()` works.

## Key Design Decisions

**Arena-based allocation.** All expression nodes and plan nodes are stored in
an `Arena`. The arena has two stores:
- `nodes []Node` -- expression nodes, accessed by `NodeID` (integer index)
- `strings []string` + `stringByID map[string]uint32` -- interned strings

Column names are interned (stored once, referenced by ID) to avoid redundant
allocations during optimization passes that clone and rewrite expression trees.
`Arena.Add(node)` returns a `NodeID`; `Arena.Get(id)` retrieves it.

**Node structure.** Each `Node` has a `Kind` (column, literal, binary op,
aggregate, etc.), optional `Children []NodeID`, and type-specific fields
(string ID, literal value, operator). The tree is implicit in the children
references.

**Logical plans.** The `LogicalPlan` interface has methods: `Kind()`,
`Schema()`, `Children()`, `WithChildren()`. Concrete types:
- `ScanPlan` -- reads from a `DataSource` (CSV file, in-memory DataFrame)
- `FilterPlan` -- applies a predicate
- `ProjectionPlan` -- selects/computes columns
- `AggregatePlan` -- group-by with aggregations
- `JoinPlan` -- join two plans

**Optimizer pipeline.** `Pipeline` runs a list of `Optimizer` passes in order,
repeating up to `MaxPasses` times until no changes occur. Default passes:
1. `ConstantFolding` -- evaluates constant expressions at plan time
2. `BooleanSimplify` -- simplifies boolean logic (AND true -> identity, etc.)
3. `ColumnExpansion` -- expands wildcard column references
4. `TypeCoercion` -- inserts casts for type mismatches
5. `CommonSubexpressionElimination` -- deduplicates repeated expressions
6. `PredicatePushdown` -- moves filters closer to scans
7. `ProjectionPushdown` -- prunes unused columns early

Each optimizer implements `Optimize(LogicalPlan) (LogicalPlan, error)` and
returns a new plan (or the same plan if no changes).

**Compiler.** `Compile(LogicalPlan) (PhysicalPlan, error)` translates each
logical node to a physical node via a recursive type-switch:
- `ScanPlan` -> `PhysicalScan` (wraps an `ExecutableSource`)
- `FilterPlan` -> `PhysicalFilter` (evaluates predicate, delegates to frame)
- `ProjectionPlan` -> `PhysicalProjection`
- etc.

**Physical execution.** Each `PhysicalPlan` implements
`Execute() (*frame.DataFrame, error)`. Execution is pull-based: `Collect()`
on a `LazyFrame` calls `Compile` then `Execute`.

**LazyFrame API.** `LazyFrame` is the user-facing builder. Methods like
`Filter()`, `Select()`, `GroupBy()` append to the logical plan tree.
`Collect()` triggers optimization, compilation, and execution.

**DataSource interface.** `DataSource` provides `Schema()`. `ExecutableSource`
extends it with `Execute() (*frame.DataFrame, error)`. Concrete sources:
`CSVSource` (reads CSV on execute), `FrameSource` (wraps an existing DataFrame).

**Type inference.** `type_infer.go` walks the expression tree and infers output
types, used by plan `Schema()` methods to determine column types without
executing.

**Explain.** `explain.go` produces a human-readable string representation of
the logical or physical plan tree, useful for debugging.
