package lazy

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
	"github.com/tnn1t1s/golars/series"
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
	win := Window{
		Func: w.fn,
		Spec: spec,
	}
	id := w.arena.AddWindow(win, w.input, w.hasInput)
	return Expr{arena: w.arena, id: id}
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
	id := e.arena.AddAlias(name, e.id)
	return Expr{arena: e.arena, id: id}
}
func (e Expr) Cast(dt datatypes.DataType) Expr {
	id := e.arena.AddCast(dt.String(), e.id)
	return Expr{arena: e.arena, id: id}
}

// Over converts an aggregate expression into a window expression.
func (e Expr) Over(spec *window.Spec) Expr {
	node := e.arena.MustGet(e.id)
	if node.Kind == KindAgg {
		agg := node.Payload.(Agg)
		// Get the column name from the child
		colName := ""
		if len(node.Children) > 0 {
			colName, _ = columnName(e.arena, node.Children[0])
		}
		wfn, ok := windowFuncFromAgg(agg.Op, colName)
		if ok {
			win := Window{
				Func: wfn,
				Spec: spec,
			}
			id := e.arena.AddWindow(win, node.Children[0], len(node.Children) > 0)
			return Expr{arena: e.arena, id: id}
		}
	}
	// Fallback: wrap in a window node with the spec
	win := Window{Spec: spec}
	id := e.arena.AddWindow(win, e.id, true)
	return Expr{arena: e.arena, id: id}
}

// Lag builds a window LAG expression for the column.
func (e Expr) Lag(offset int, defaultValue ...interface{}) WindowBuilder {
	colName, _ := columnName(e.arena, e.id)
	var defVal interface{}
	if len(defaultValue) > 0 {
		defVal = defaultValue[0]
	}
	fn := &lagFuncAdapter{column: colName, offset: offset, defaultValue: defVal}
	return WindowBuilder{arena: e.arena, fn: fn, input: e.id, hasInput: true}
}

// Lead builds a window LEAD expression for the column.
func (e Expr) Lead(offset int, defaultValue ...interface{}) WindowBuilder {
	colName, _ := columnName(e.arena, e.id)
	var defVal interface{}
	if len(defaultValue) > 0 {
		defVal = defaultValue[0]
	}
	fn := &leadFuncAdapter{column: colName, offset: offset, defaultValue: defVal}
	return WindowBuilder{arena: e.arena, fn: fn, input: e.id, hasInput: true}
}

// FirstValue builds a FIRST_VALUE window expression for the column.
func (e Expr) FirstValue() WindowBuilder {
	colName, _ := columnName(e.arena, e.id)
	fn := &firstValueFuncAdapter{column: colName}
	return WindowBuilder{arena: e.arena, fn: fn, input: e.id, hasInput: true}
}

// LastValue builds a LAST_VALUE window expression for the column.
func (e Expr) LastValue() WindowBuilder {
	colName, _ := columnName(e.arena, e.id)
	fn := &lastValueFuncAdapter{column: colName}
	return WindowBuilder{arena: e.arena, fn: fn, input: e.id, hasInput: true}
}

func (e Expr) binary(op BinaryOp, other Expr) Expr {
	e.ensureSameArena(other)
	id := e.arena.AddBinary(op, e.id, other.id)
	return Expr{arena: e.arena, id: id}
}

func (e Expr) unary(op UnaryOp) Expr {
	id := e.arena.AddUnary(op, e.id)
	return Expr{arena: e.arena, id: id}
}

func (e Expr) agg(op AggOp) Expr {
	id := e.arena.AddAgg(op, e.id)
	return Expr{arena: e.arena, id: id}
}

func (e Expr) ensureSameArena(other Expr) {
	if e.arena != other.arena {
		panic("expressions must share the same arena")
	}
}

func columnName(a *Arena, id NodeID) (string, bool) {
	node, ok := a.Get(id)
	if !ok {
		return "", false
	}
	if node.Kind == KindColumn {
		col := node.Payload.(Column)
		return a.String(col.NameID)
	}
	if node.Kind == KindAlias {
		alias := node.Payload.(Alias)
		return a.String(alias.NameID)
	}
	return "", false
}

func windowFuncFromAgg(op AggOp, column string) (window.WindowFunc, bool) {
	switch op {
	case AggSum:
		return window.Sum(column), true
	case AggMin:
		return window.Min(column), true
	case AggMax:
		return window.Max(column), true
	case AggCount:
		return window.Count(column), true
	default:
		return window.WindowFunc{}, false
	}
}

// Adapter types to bridge lazy window expressions to the window.Function interface.

type lagFuncAdapter struct {
	column       string
	offset       int
	defaultValue interface{}
	spec         *window.Spec
}

func (f *lagFuncAdapter) Compute(partition window.Partition) (series.Series, error) {
	return nil, fmt.Errorf("lag not directly computed here")
}
func (f *lagFuncAdapter) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}
func (f *lagFuncAdapter) Name() string {
	return fmt.Sprintf("lag(%d)", f.offset)
}
func (f *lagFuncAdapter) Validate(spec *window.Spec) error { return nil }
func (f *lagFuncAdapter) SetSpec(spec *window.Spec)        { f.spec = spec }

type leadFuncAdapter struct {
	column       string
	offset       int
	defaultValue interface{}
	spec         *window.Spec
}

func (f *leadFuncAdapter) Compute(partition window.Partition) (series.Series, error) {
	return nil, fmt.Errorf("lead not directly computed here")
}
func (f *leadFuncAdapter) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}
func (f *leadFuncAdapter) Name() string {
	return fmt.Sprintf("lead(%d)", f.offset)
}
func (f *leadFuncAdapter) Validate(spec *window.Spec) error { return nil }
func (f *leadFuncAdapter) SetSpec(spec *window.Spec)        { f.spec = spec }

type firstValueFuncAdapter struct {
	column string
	spec   *window.Spec
}

func (f *firstValueFuncAdapter) Compute(partition window.Partition) (series.Series, error) {
	return nil, fmt.Errorf("first_value not directly computed here")
}
func (f *firstValueFuncAdapter) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}
func (f *firstValueFuncAdapter) Name() string                    { return "first_value" }
func (f *firstValueFuncAdapter) Validate(spec *window.Spec) error { return nil }
func (f *firstValueFuncAdapter) SetSpec(spec *window.Spec)        { f.spec = spec }

type lastValueFuncAdapter struct {
	column string
	spec   *window.Spec
}

func (f *lastValueFuncAdapter) Compute(partition window.Partition) (series.Series, error) {
	return nil, fmt.Errorf("last_value not directly computed here")
}
func (f *lastValueFuncAdapter) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}
func (f *lastValueFuncAdapter) Name() string                   { return "last_value" }
func (f *lastValueFuncAdapter) Validate(spec *window.Spec) error { return nil }
func (f *lastValueFuncAdapter) SetSpec(spec *window.Spec)        { f.spec = spec }
