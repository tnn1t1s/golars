package lazy

import (
	"context"
	_ "fmt"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	_ "github.com/tnn1t1s/golars/internal/window"
)

// LazyFrame is a lazy query builder.
type LazyFrame struct {
	arena *Arena
	plan  LogicalPlan
}

// NewLazyFrame creates a LazyFrame from a data source.
func NewLazyFrame(source DataSource) *LazyFrame {
	panic("not implemented")

}

// FromDataFrame creates a LazyFrame from a DataFrame.
func FromDataFrame(df *frame.DataFrame) *LazyFrame {
	panic("not implemented")

}

// Col creates a column expression.
func (lf *LazyFrame) Col(name string) Expr {
	panic("not implemented")

}

// ColType creates a column selector expression for a data type.
func (lf *LazyFrame) ColType(dt datatypes.DataType) Expr {
	panic("not implemented")

}

// Lit creates a literal expression.
func (lf *LazyFrame) Lit(value interface{}) Expr {
	panic("not implemented")

}

// Filter adds a filter predicate.
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	panic("not implemented")

}

// Select applies a projection.
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	panic("not implemented")

}

// WithColumn adds or replaces a column using an expression.
func (lf *LazyFrame) WithColumn(name string, expr Expr) *LazyFrame {
	panic("not implemented")

}

// WithColumns adds or replaces columns in order using expressions.
func (lf *LazyFrame) WithColumns(exprs map[string]Expr) *LazyFrame {
	panic("not implemented")

}

// WithColumnsOrdered adds or replaces columns in the given order.
func (lf *LazyFrame) WithColumnsOrdered(names []string, exprs []Expr) *LazyFrame {
	panic("not implemented")

}

// GroupBy starts a group-by operation.
func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
	panic("not implemented")

}

// RowNumber builds a ROW_NUMBER window expression.
func (lf *LazyFrame) RowNumber() WindowBuilder {
	panic("not implemented")

}

// Rank builds a RANK window expression.
func (lf *LazyFrame) Rank() WindowBuilder {
	panic("not implemented")

}

// DenseRank builds a DENSE_RANK window expression.
func (lf *LazyFrame) DenseRank() WindowBuilder {
	panic("not implemented")

}

// PercentRank builds a PERCENT_RANK window expression.
func (lf *LazyFrame) PercentRank() WindowBuilder {
	panic("not implemented")

}

// NTile builds an NTILE window expression.
func (lf *LazyFrame) NTile(buckets int) WindowBuilder {
	panic("not implemented")

}

// Optimize applies the default optimizer pipeline.
func (lf *LazyFrame) Optimize() (*LazyFrame, error) {
	panic("not implemented")

}

// Collect executes the plan.
func (lf *LazyFrame) Collect(ctx context.Context) (*frame.DataFrame, error) {
	panic("not implemented")

}

// Explain returns a string summary of the plan.
func (lf *LazyFrame) Explain() string {
	panic("not implemented")

}

func (lf *LazyFrame) exprIDs(exprs []Expr) []NodeID {
	panic("not implemented")

}

func (lf *LazyFrame) ensureArena(expr Expr) {
	panic("not implemented")

}

// LazyGroupBy holds group-by state before aggregation.
type LazyGroupBy struct {
	arena *Arena
	input LogicalPlan
	keys  []NodeID
}

// Agg applies aggregations and returns a LazyFrame.
func (gb *LazyGroupBy) Agg(aggs ...Expr) *LazyFrame {
	panic("not implemented")

}
