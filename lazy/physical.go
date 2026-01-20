package lazy

import (
	"context"
	"fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
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
	if p.Source == nil {
		return nil, errMissingSource
	}
	return p.Source.DataFrame()
}

// PhysicalFilter applies a predicate.
type PhysicalFilter struct {
	Input     PhysicalPlan
	Predicate NodeID
	Arena     *Arena
}

func (p *PhysicalFilter) Execute(ctx context.Context) (*frame.DataFrame, error) {
	if p.Input == nil {
		return nil, errMissingInput
	}
	df, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}
	pred, err := exprFromNode(p.Arena, p.Predicate)
	if err != nil {
		return nil, err
	}
	return df.Filter(pred)
}

// PhysicalProjection selects columns.
type PhysicalProjection struct {
	Input PhysicalPlan
	Exprs []NodeID
	Arena *Arena
}

func (p *PhysicalProjection) Execute(ctx context.Context) (*frame.DataFrame, error) {
	if p.Input == nil {
		return nil, errMissingInput
	}
	df, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}

	if p.Arena == nil {
		return nil, errMissingArena
	}

	allColumns := true
	for _, exprID := range p.Exprs {
		node, ok := p.Arena.Get(exprID)
		if !ok || node.Kind != KindColumn {
			allColumns = false
			break
		}
	}
	if allColumns {
		cols, err := projectionColumns(p.Arena, p.Exprs)
		if err != nil {
			return nil, err
		}
		return df.Select(cols...)
	}

	exprs := make(map[string]expr.Expr, len(p.Exprs))
	names := make([]string, 0, len(p.Exprs))
	for _, exprID := range p.Exprs {
		node, ok := p.Arena.Get(exprID)
		if !ok {
			return nil, fmt.Errorf("invalid projection expression")
		}
		switch node.Kind {
		case KindColumn, KindLiteral, KindWindow, KindAlias:
		default:
			return nil, fmt.Errorf("unsupported projection expression")
		}
		name := OutputName(p.Arena, exprID)
		compiled, err := exprFromNode(p.Arena, exprID)
		if err != nil {
			return nil, err
		}
		if _, exists := exprs[name]; exists {
			return nil, fmt.Errorf("duplicate projection name: %s", name)
		}
		exprs[name] = compiled
		names = append(names, name)
	}

	withCols, err := df.WithColumns(exprs)
	if err != nil {
		return nil, err
	}
	return withCols.Select(names...)
}

// PhysicalAggregate groups and aggregates.
type PhysicalAggregate struct {
	Input PhysicalPlan
	Keys  []NodeID
	Aggs  []NodeID
	Arena *Arena
}

func (p *PhysicalAggregate) Execute(ctx context.Context) (*frame.DataFrame, error) {
	if p.Input == nil {
		return nil, errMissingInput
	}
	df, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := projectionColumns(p.Arena, p.Keys)
	if err != nil {
		return nil, err
	}
	grouped, err := df.GroupBy(keys...)
	if err != nil {
		return nil, err
	}
	aggs, err := aggExprs(p.Arena, p.Aggs)
	if err != nil {
		return nil, err
	}
	return grouped.Agg(aggs)
}

func projectionColumns(a *Arena, exprs []NodeID) ([]string, error) {
	if a == nil {
		return nil, errMissingArena
	}
	names := make([]string, len(exprs))
	for i, expr := range exprs {
		node, ok := a.Get(expr)
		if !ok || node.Kind != KindColumn {
			return nil, fmt.Errorf("unsupported projection expression")
		}
		payload, ok := node.Payload.(Column)
		if !ok {
			return nil, fmt.Errorf("invalid column payload")
		}
		name, ok := a.String(payload.NameID)
		if !ok {
			return nil, fmt.Errorf("unknown column name")
		}
		names[i] = name
	}
	return names, nil
}

func aggExprs(a *Arena, aggs []NodeID) (map[string]expr.Expr, error) {
	if a == nil {
		return nil, errMissingArena
	}
	result := make(map[string]expr.Expr, len(aggs))
	for _, aggID := range aggs {
		node, ok := a.Get(aggID)
		if !ok || node.Kind != KindAgg || len(node.Children) != 1 {
			return nil, fmt.Errorf("unsupported aggregation expression")
		}
		child := node.Children[0]
		childNode, ok := a.Get(child)
		if !ok || childNode.Kind != KindColumn {
			return nil, fmt.Errorf("aggregation requires column input")
		}
		colPayload, ok := childNode.Payload.(Column)
		if !ok {
			return nil, fmt.Errorf("invalid column payload")
		}
		name, ok := a.String(colPayload.NameID)
		if !ok {
			return nil, fmt.Errorf("unknown column name")
		}
		aggPayload, ok := node.Payload.(Agg)
		if !ok {
			return nil, fmt.Errorf("invalid agg payload")
		}
		result[OutputName(a, aggID)] = aggExprFromOp(name, aggPayload.Op)
	}
	return result, nil
}

func aggExprFromOp(name string, op AggOp) expr.Expr {
	switch op {
	case AggSum:
		return expr.Col(name).Sum()
	case AggMean:
		return expr.Col(name).Mean()
	case AggMin:
		return expr.Col(name).Min()
	case AggMax:
		return expr.Col(name).Max()
	case AggCount:
		return expr.Col(name).Count()
	case AggStd:
		return expr.Col(name).Std()
	case AggVar:
		return expr.Col(name).Var()
	case AggFirst:
		return expr.Col(name).First()
	case AggLast:
		return expr.Col(name).Last()
	case AggMedian:
		return expr.Col(name).Median()
	default:
		return expr.Col(name).Count()
	}
}

func exprFromNode(a *Arena, id NodeID) (expr.Expr, error) {
	if a == nil {
		return nil, errMissingArena
	}
	node, ok := a.Get(id)
	if !ok {
		return nil, fmt.Errorf("invalid node id")
	}

	switch node.Kind {
	case KindColumn:
		payload, ok := node.Payload.(Column)
		if !ok {
			return nil, fmt.Errorf("invalid column payload")
		}
		name, ok := a.String(payload.NameID)
		if !ok {
			return nil, fmt.Errorf("unknown column name")
		}
		return expr.Col(name), nil
	case KindLiteral:
		payload, ok := node.Payload.(Literal)
		if !ok {
			return nil, fmt.Errorf("invalid literal payload")
		}
		return expr.Lit(payload.Value), nil
	case KindBinary:
		payload, ok := node.Payload.(Binary)
		if !ok {
			return nil, fmt.Errorf("invalid binary payload")
		}
		if len(node.Children) != 2 {
			return nil, fmt.Errorf("binary node missing children")
		}
		left, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		right, err := exprFromNode(a, node.Children[1])
		if err != nil {
			return nil, err
		}
		return applyBinary(payload.Op, left, right), nil
	case KindUnary:
		payload, ok := node.Payload.(Unary)
		if !ok {
			return nil, fmt.Errorf("invalid unary payload")
		}
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("unary node missing child")
		}
		child, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		return applyUnary(payload.Op, child), nil
	case KindCast:
		payload, ok := node.Payload.(Cast)
		if !ok {
			return nil, fmt.Errorf("invalid cast payload")
		}
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("cast node missing child")
		}
		targetName, ok := a.String(payload.TypeID)
		if !ok {
			return nil, fmt.Errorf("unknown cast type")
		}
		target := typeFromName(targetName)
		if target == nil || target.Equals(datatypes.Unknown{}) {
			return nil, fmt.Errorf("unsupported cast type")
		}
		child, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		return expr.Cast(child, target), nil
	case KindAlias:
		payload, ok := node.Payload.(Alias)
		if !ok {
			return nil, fmt.Errorf("invalid alias payload")
		}
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("alias node missing child")
		}
		name, ok := a.String(payload.NameID)
		if !ok {
			return nil, fmt.Errorf("unknown alias name")
		}
		child, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		return child.Alias(name), nil
	case KindWindow:
		payload, ok := node.Payload.(Window)
		if !ok {
			return nil, fmt.Errorf("invalid window payload")
		}
		if payload.Func == nil {
			return nil, fmt.Errorf("missing window function")
		}
		if payload.Spec == nil {
			return nil, fmt.Errorf("missing window spec")
		}
		if len(node.Children) == 1 {
			child, err := exprFromNode(a, node.Children[0])
			if err != nil {
				return nil, err
			}
			return window.NewAggregateExpr(payload.Func, child, payload.Spec), nil
		}
		return window.NewExpr(payload.Func, payload.Spec), nil
	default:
		return nil, fmt.Errorf("unsupported expression kind: %v", node.Kind)
	}
}

func applyBinary(op BinaryOp, left, right expr.Expr) expr.Expr {
	builder := expr.NewBuilder(left)
	switch op {
	case OpAdd:
		return builder.Add(right).Build()
	case OpSub:
		return builder.Sub(right).Build()
	case OpMul:
		return builder.Mul(right).Build()
	case OpDiv:
		return builder.Div(right).Build()
	case OpEq:
		return builder.Eq(right).Build()
	case OpNeq:
		return builder.Ne(right).Build()
	case OpLt:
		return builder.Lt(right).Build()
	case OpLte:
		return builder.Le(right).Build()
	case OpGt:
		return builder.Gt(right).Build()
	case OpGte:
		return builder.Ge(right).Build()
	case OpAnd:
		return builder.And(right).Build()
	case OpOr:
		return builder.Or(right).Build()
	default:
		return builder.Add(right).Build()
	}
}

func applyUnary(op UnaryOp, child expr.Expr) expr.Expr {
	builder := expr.NewBuilder(child)
	switch op {
	case OpNot:
		return builder.Not().Build()
	case OpIsNull:
		return builder.IsNull().Build()
	case OpIsNotNull:
		return builder.IsNotNull().Build()
	default:
		return child
	}
}
