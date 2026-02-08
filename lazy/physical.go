package lazy

import (
	"context"
	_ "fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	_ "github.com/tnn1t1s/golars/internal/parallel"
	_ "github.com/tnn1t1s/golars/internal/window"
)

// PhysicalPlan executes to produce a DataFrame.
type PhysicalPlan interface {
	Execute(ctx context.Context) (*frame.DataFrame, error)
}

// PhysicalScan reads from a data source.
type PhysicalScan struct {
	Source ExecutableSource
}

func (p *PhysicalScan) Execute(_ context.Context) (*frame.DataFrame, error) {
	panic("not implemented")

}

// PhysicalFilter applies a predicate.
type PhysicalFilter struct {
	Input     PhysicalPlan
	Predicate NodeID
	Arena     *Arena
}

func (p *PhysicalFilter) Execute(ctx context.Context) (*frame.DataFrame, error) {
	panic("not implemented")

}

// PhysicalProjection selects columns.
type PhysicalProjection struct {
	Input PhysicalPlan
	Exprs []NodeID
	Arena *Arena
}

func (p *PhysicalProjection) Execute(ctx context.Context) (*frame.DataFrame, error) {
	panic("not implemented")

}

// PhysicalAggregate groups and aggregates.
type PhysicalAggregate struct {
	Input PhysicalPlan
	Keys  []NodeID
	Aggs  []NodeID
	Arena *Arena
}

func (p *PhysicalAggregate) Execute(ctx context.Context) (*frame.DataFrame, error) {
	panic("not implemented")

}

func projectionColumns(a *Arena, exprs []NodeID) ([]string, error) {
	panic("not implemented")

}

func aggExprs(a *Arena, aggs []NodeID) (map[string]expr.Expr, error) {
	panic("not implemented")

}

func aggExprFromOp(name string, op AggOp) expr.Expr {
	panic("not implemented")

}

func exprFromNode(a *Arena, id NodeID) (expr.Expr, error) {
	panic("not implemented")

}

func applyBinary(op BinaryOp, left, right expr.Expr) expr.Expr {
	panic("not implemented")

}

func applyUnary(op UnaryOp, child expr.Expr) expr.Expr {
	panic("not implemented")

}
