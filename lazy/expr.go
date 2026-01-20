package lazy

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
)

// Expr represents a lazy expression backed by an arena node.
type Expr struct {
	arena *Arena
	id    NodeID
}

func (e Expr) Arena() *Arena { return e.arena }
func (e Expr) ID() NodeID    { return e.id }

// WindowBuilder builds a window expression when a spec is applied.
type WindowBuilder struct {
	arena    *Arena
	fn       window.Function
	input    NodeID
	hasInput bool
}

// Over applies the window specification and returns a window expression.
func (w WindowBuilder) Over(spec *window.Spec) Expr {
	if w.arena == nil || w.fn == nil {
		panic("window builder missing arena or function")
	}
	if spec == nil {
		panic("window spec is nil")
	}
	return Expr{
		arena: w.arena,
		id:    w.arena.AddWindow(Window{Func: w.fn, Spec: spec}, w.input, w.hasInput),
	}
}

func (e Expr) Add(other Expr) Expr { return e.binary(OpAdd, other) }
func (e Expr) Sub(other Expr) Expr { return e.binary(OpSub, other) }
func (e Expr) Mul(other Expr) Expr { return e.binary(OpMul, other) }
func (e Expr) Div(other Expr) Expr { return e.binary(OpDiv, other) }
func (e Expr) Eq(other Expr) Expr  { return e.binary(OpEq, other) }
func (e Expr) Neq(other Expr) Expr { return e.binary(OpNeq, other) }
func (e Expr) Lt(other Expr) Expr  { return e.binary(OpLt, other) }
func (e Expr) Lte(other Expr) Expr { return e.binary(OpLte, other) }
func (e Expr) Gt(other Expr) Expr  { return e.binary(OpGt, other) }
func (e Expr) Gte(other Expr) Expr { return e.binary(OpGte, other) }
func (e Expr) And(other Expr) Expr { return e.binary(OpAnd, other) }
func (e Expr) Or(other Expr) Expr  { return e.binary(OpOr, other) }
func (e Expr) Not() Expr           { return e.unary(OpNot) }
func (e Expr) IsNull() Expr        { return e.unary(OpIsNull) }
func (e Expr) IsNotNull() Expr     { return e.unary(OpIsNotNull) }
func (e Expr) Sum() Expr           { return e.agg(AggSum) }
func (e Expr) Mean() Expr          { return e.agg(AggMean) }
func (e Expr) Min() Expr           { return e.agg(AggMin) }
func (e Expr) Max() Expr           { return e.agg(AggMax) }
func (e Expr) Count() Expr         { return e.agg(AggCount) }
func (e Expr) Std() Expr           { return e.agg(AggStd) }
func (e Expr) Var() Expr           { return e.agg(AggVar) }
func (e Expr) First() Expr         { return e.agg(AggFirst) }
func (e Expr) Last() Expr          { return e.agg(AggLast) }
func (e Expr) Median() Expr        { return e.agg(AggMedian) }
func (e Expr) Alias(name string) Expr {
	return Expr{arena: e.arena, id: e.arena.AddAlias(name, e.id)}
}
func (e Expr) Cast(dt datatypes.DataType) Expr {
	return Expr{arena: e.arena, id: e.arena.AddCast(dt.String(), e.id)}
}

// Over converts an aggregate expression into a window expression.
func (e Expr) Over(spec *window.Spec) Expr {
	node, ok := e.arena.Get(e.id)
	if !ok || node.Kind != KindAgg || len(node.Children) != 1 {
		panic("over requires an aggregate expression")
	}
	if spec == nil {
		panic("window spec is nil")
	}
	agg, ok := node.Payload.(Agg)
	if !ok {
		panic("invalid aggregate payload")
	}
	childID := node.Children[0]
	name, ok := columnName(e.arena, childID)
	if !ok {
		panic("window aggregate requires column input")
	}
	fn, ok := windowFuncFromAgg(agg.Op, name)
	if !ok {
		panic("unsupported window aggregate")
	}
	return Expr{
		arena: e.arena,
		id:    e.arena.AddWindow(Window{Func: fn.Function, Spec: spec}, childID, true),
	}
}

// Lag builds a window LAG expression for the column.
func (e Expr) Lag(offset int, defaultValue ...interface{}) WindowBuilder {
	name, ok := columnName(e.arena, e.id)
	if !ok {
		panic("lag requires a column expression")
	}
	fn := window.Lag(name, offset, defaultValue...)
	return WindowBuilder{
		arena:    e.arena,
		fn:       fn.Function,
		input:    e.id,
		hasInput: true,
	}
}

// Lead builds a window LEAD expression for the column.
func (e Expr) Lead(offset int, defaultValue ...interface{}) WindowBuilder {
	name, ok := columnName(e.arena, e.id)
	if !ok {
		panic("lead requires a column expression")
	}
	fn := window.Lead(name, offset, defaultValue...)
	return WindowBuilder{
		arena:    e.arena,
		fn:       fn.Function,
		input:    e.id,
		hasInput: true,
	}
}

// FirstValue builds a FIRST_VALUE window expression for the column.
func (e Expr) FirstValue() WindowBuilder {
	name, ok := columnName(e.arena, e.id)
	if !ok {
		panic("first value requires a column expression")
	}
	fn := window.FirstValue(name)
	return WindowBuilder{
		arena:    e.arena,
		fn:       fn.Function,
		input:    e.id,
		hasInput: true,
	}
}

// LastValue builds a LAST_VALUE window expression for the column.
func (e Expr) LastValue() WindowBuilder {
	name, ok := columnName(e.arena, e.id)
	if !ok {
		panic("last value requires a column expression")
	}
	fn := window.LastValue(name)
	return WindowBuilder{
		arena:    e.arena,
		fn:       fn.Function,
		input:    e.id,
		hasInput: true,
	}
}

func (e Expr) binary(op BinaryOp, other Expr) Expr {
	e.ensureSameArena(other)
	return Expr{arena: e.arena, id: e.arena.AddBinary(op, e.id, other.id)}
}

func (e Expr) unary(op UnaryOp) Expr {
	return Expr{arena: e.arena, id: e.arena.AddUnary(op, e.id)}
}

func (e Expr) agg(op AggOp) Expr {
	return Expr{arena: e.arena, id: e.arena.AddAgg(op, e.id)}
}

func (e Expr) ensureSameArena(other Expr) {
	if e.arena != other.arena {
		panic(fmt.Sprintf("expression arena mismatch: %p vs %p", e.arena, other.arena))
	}
}

func columnName(a *Arena, id NodeID) (string, bool) {
	if a == nil {
		return "", false
	}
	node, ok := a.Get(id)
	if !ok || node.Kind != KindColumn {
		return "", false
	}
	payload, ok := node.Payload.(Column)
	if !ok {
		return "", false
	}
	return a.String(payload.NameID)
}

func windowFuncFromAgg(op AggOp, column string) (window.WindowFunc, bool) {
	switch op {
	case AggSum:
		return window.Sum(column), true
	case AggMean:
		return window.Avg(column), true
	case AggMin:
		return window.Min(column), true
	case AggMax:
		return window.Max(column), true
	case AggCount:
		return window.Count(column), true
	case AggFirst:
		return window.FirstValue(column), true
	case AggLast:
		return window.LastValue(column), true
	default:
		return window.WindowFunc{}, false
	}
}
