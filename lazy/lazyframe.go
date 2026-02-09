package lazy

import (
	"context"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
)

// LazyFrame is a lazy query builder.
type LazyFrame struct {
	arena *Arena
	plan  LogicalPlan
}

// NewLazyFrame creates a LazyFrame from a data source.
func NewLazyFrame(source DataSource) *LazyFrame {
	arena := NewArena()
	return &LazyFrame{
		arena: arena,
		plan:  &ScanPlan{Source: source, Arena: arena},
	}
}

// FromDataFrame creates a LazyFrame from a DataFrame.
func FromDataFrame(df *frame.DataFrame) *LazyFrame {
	source := &FrameSource{NameValue: "dataframe", Frame: df}
	return NewLazyFrame(source)
}

// Col creates a column expression.
func (lf *LazyFrame) Col(name string) Expr {
	id := lf.arena.AddColumn(name)
	return Expr{arena: lf.arena, id: id}
}

// ColType creates a column selector expression for a data type.
func (lf *LazyFrame) ColType(dt datatypes.DataType) Expr {
	// Create a function node named "__col_type__" with a literal DataType child
	litID := lf.arena.AddLiteral(dt)
	fnID := lf.arena.AddFunction("__col_type__", []NodeID{litID})
	return Expr{arena: lf.arena, id: fnID}
}

// Lit creates a literal expression.
func (lf *LazyFrame) Lit(value interface{}) Expr {
	id := lf.arena.AddLiteral(value)
	return Expr{arena: lf.arena, id: id}
}

// Filter adds a filter predicate.
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	lf.ensureArena(predicate)
	return &LazyFrame{
		arena: lf.arena,
		plan: &FilterPlan{
			Input:     lf.plan,
			Predicate: predicate.id,
			Arena:     lf.arena,
		},
	}
}

// Select applies a projection.
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	ids := lf.exprIDs(exprs)
	return &LazyFrame{
		arena: lf.arena,
		plan: &ProjectionPlan{
			Input: lf.plan,
			Exprs: ids,
			Arena: lf.arena,
		},
	}
}

// WithColumn adds or replaces a column using an expression.
func (lf *LazyFrame) WithColumn(name string, e Expr) *LazyFrame {
	lf.ensureArena(e)
	aliasID := lf.arena.AddAlias(name, e.id)

	// Get existing schema columns to build a projection with the new/replaced column
	// We use a wildcard + the aliased expression
	starID := lf.arena.AddColumn("*")
	return &LazyFrame{
		arena: lf.arena,
		plan: &ProjectionPlan{
			Input: lf.plan,
			Exprs: []NodeID{starID, aliasID},
			Arena: lf.arena,
		},
	}
}

// WithColumns adds or replaces columns in order using expressions.
func (lf *LazyFrame) WithColumns(exprs map[string]Expr) *LazyFrame {
	result := lf
	for name, e := range exprs {
		result = result.WithColumn(name, e)
	}
	return result
}

// WithColumnsOrdered adds or replaces columns in the given order.
func (lf *LazyFrame) WithColumnsOrdered(names []string, exprs []Expr) *LazyFrame {
	result := lf
	for i, name := range names {
		result = result.WithColumn(name, exprs[i])
	}
	return result
}

// GroupBy starts a group-by operation.
func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
	keyIDs := lf.exprIDs(keys)
	return &LazyGroupBy{
		arena: lf.arena,
		input: lf.plan,
		keys:  keyIDs,
	}
}

// RowNumber builds a ROW_NUMBER window expression.
func (lf *LazyFrame) RowNumber() WindowBuilder {
	fn := window.RowNumber()
	return WindowBuilder{arena: lf.arena, fn: fn.Function, hasInput: false}
}

// Rank builds a RANK window expression.
func (lf *LazyFrame) Rank() WindowBuilder {
	fn := window.Rank()
	return WindowBuilder{arena: lf.arena, fn: fn.Function, hasInput: false}
}

// DenseRank builds a DENSE_RANK window expression.
func (lf *LazyFrame) DenseRank() WindowBuilder {
	fn := window.DenseRank()
	return WindowBuilder{arena: lf.arena, fn: fn.Function, hasInput: false}
}

// PercentRank builds a PERCENT_RANK window expression.
func (lf *LazyFrame) PercentRank() WindowBuilder {
	fn := window.PercentRank()
	return WindowBuilder{arena: lf.arena, fn: fn.Function, hasInput: false}
}

// NTile builds an NTILE window expression.
func (lf *LazyFrame) NTile(buckets int) WindowBuilder {
	fn := window.NTile(buckets)
	return WindowBuilder{arena: lf.arena, fn: fn.Function, hasInput: false}
}

// Optimize applies the default optimizer pipeline.
func (lf *LazyFrame) Optimize() (*LazyFrame, error) {
	pipeline := DefaultPipeline(lf.arena)
	optimized, err := pipeline.Optimize(lf.plan)
	if err != nil {
		return nil, err
	}
	return &LazyFrame{arena: lf.arena, plan: optimized}, nil
}

// Collect executes the plan.
func (lf *LazyFrame) Collect(ctx context.Context) (*frame.DataFrame, error) {
	// Optimize first
	optimized, err := lf.Optimize()
	if err != nil {
		return nil, err
	}

	// Compile to physical plan
	physical, err := Compile(optimized.plan)
	if err != nil {
		return nil, err
	}

	// Execute
	return physical.Execute(ctx)
}

// Explain returns a string summary of the plan.
func (lf *LazyFrame) Explain() string {
	return ExplainPlan(lf.plan)
}

func (lf *LazyFrame) exprIDs(exprs []Expr) []NodeID {
	ids := make([]NodeID, len(exprs))
	for i, e := range exprs {
		lf.ensureArena(e)
		ids[i] = e.id
	}
	return ids
}

func (lf *LazyFrame) ensureArena(e Expr) {
	// In a more robust implementation, we would migrate nodes between arenas.
	// For now, we just verify they share the same arena or the expression
	// was created from this LazyFrame's arena.
	if e.arena != nil && e.arena != lf.arena {
		panic("expressions must share the same arena")
	}
}

// LazyGroupBy holds group-by state before aggregation.
type LazyGroupBy struct {
	arena *Arena
	input LogicalPlan
	keys  []NodeID
}

// Agg applies aggregations and returns a LazyFrame.
func (gb *LazyGroupBy) Agg(aggs ...Expr) *LazyFrame {
	aggIDs := make([]NodeID, len(aggs))
	for i, a := range aggs {
		aggIDs[i] = a.id
	}
	return &LazyFrame{
		arena: gb.arena,
		plan: &AggregatePlan{
			Input: gb.input,
			Keys:  gb.keys,
			Aggs:  aggIDs,
			Arena: gb.arena,
		},
	}
}
