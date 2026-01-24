package lazy

import (
	"context"
	"fmt"

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
	return Expr{arena: lf.arena, id: lf.arena.AddColumn(name)}
}

// ColType creates a column selector expression for a data type.
func (lf *LazyFrame) ColType(dt datatypes.DataType) Expr {
	typeName := dt.String()
	typeNode := lf.arena.AddLiteral(typeName)
	return Expr{arena: lf.arena, id: lf.arena.AddFunction("col_type", []NodeID{typeNode})}
}

// Lit creates a literal expression.
func (lf *LazyFrame) Lit(value interface{}) Expr {
	return Expr{arena: lf.arena, id: lf.arena.AddLiteral(value)}
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
func (lf *LazyFrame) WithColumn(name string, expr Expr) *LazyFrame {
	lf.ensureArena(expr)
	cols := []Expr{lf.Col("*"), expr.Alias(name)}
	return lf.Select(cols...)
}

// WithColumns adds or replaces columns in order using expressions.
func (lf *LazyFrame) WithColumns(exprs map[string]Expr) *LazyFrame {
	cols := make([]Expr, 0, len(exprs)+1)
	cols = append(cols, lf.Col("*"))
	for name, expr := range exprs {
		lf.ensureArena(expr)
		cols = append(cols, expr.Alias(name))
	}
	return lf.Select(cols...)
}

// WithColumnsOrdered adds or replaces columns in the given order.
func (lf *LazyFrame) WithColumnsOrdered(names []string, exprs []Expr) *LazyFrame {
	if len(names) != len(exprs) {
		panic("names and exprs length mismatch")
	}
	cols := make([]Expr, 0, len(exprs)+1)
	cols = append(cols, lf.Col("*"))
	for i, name := range names {
		expr := exprs[i]
		lf.ensureArena(expr)
		cols = append(cols, expr.Alias(name))
	}
	return lf.Select(cols...)
}

// GroupBy starts a group-by operation.
func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
	ids := lf.exprIDs(keys)
	return &LazyGroupBy{
		arena: lf.arena,
		input: lf.plan,
		keys:  ids,
	}
}

// RowNumber builds a ROW_NUMBER window expression.
func (lf *LazyFrame) RowNumber() WindowBuilder {
	fn := window.RowNumber()
	return WindowBuilder{arena: lf.arena, fn: fn.Function}
}

// Rank builds a RANK window expression.
func (lf *LazyFrame) Rank() WindowBuilder {
	fn := window.Rank()
	return WindowBuilder{arena: lf.arena, fn: fn.Function}
}

// DenseRank builds a DENSE_RANK window expression.
func (lf *LazyFrame) DenseRank() WindowBuilder {
	fn := window.DenseRank()
	return WindowBuilder{arena: lf.arena, fn: fn.Function}
}

// PercentRank builds a PERCENT_RANK window expression.
func (lf *LazyFrame) PercentRank() WindowBuilder {
	fn := window.PercentRank()
	return WindowBuilder{arena: lf.arena, fn: fn.Function}
}

// NTile builds an NTILE window expression.
func (lf *LazyFrame) NTile(buckets int) WindowBuilder {
	fn := window.NTile(buckets)
	return WindowBuilder{arena: lf.arena, fn: fn.Function}
}

// Optimize applies the default optimizer pipeline.
func (lf *LazyFrame) Optimize() (*LazyFrame, error) {
	pipeline := DefaultPipeline(lf.arena)
	plan, err := pipeline.Optimize(lf.plan)
	if err != nil {
		return nil, err
	}
	return &LazyFrame{arena: lf.arena, plan: plan}, nil
}

// Collect executes the plan.
func (lf *LazyFrame) Collect(ctx context.Context) (*frame.DataFrame, error) {
	optimized, err := lf.Optimize()
	if err != nil {
		return nil, err
	}
	physical, err := Compile(optimized.plan)
	if err != nil {
		return nil, err
	}
	return physical.Execute(ctx)
}

// Explain returns a string summary of the plan.
func (lf *LazyFrame) Explain() string {
	return ExplainPlan(lf.plan)
}

func (lf *LazyFrame) exprIDs(exprs []Expr) []NodeID {
	ids := make([]NodeID, len(exprs))
	for i, expr := range exprs {
		lf.ensureArena(expr)
		ids[i] = expr.id
	}
	return ids
}

func (lf *LazyFrame) ensureArena(expr Expr) {
	if expr.arena != lf.arena {
		panic(fmt.Sprintf("expression arena mismatch: %p vs %p", expr.arena, lf.arena))
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
	ids := make([]NodeID, len(aggs))
	for i, expr := range aggs {
		if expr.arena != gb.arena {
			panic("expression arena mismatch")
		}
		ids[i] = expr.id
	}

	return &LazyFrame{
		arena: gb.arena,
		plan: &AggregatePlan{
			Input: gb.input,
			Keys:  gb.keys,
			Aggs:  ids,
			Arena: gb.arena,
		},
	}
}
