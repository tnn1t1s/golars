package lazy

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
)

// Expr represents a lazy expression backed by an arena node.
type Expr struct {
	arena *Arena
	id    NodeID
}

func (e Expr) Arena() *Arena { panic("not implemented") }
func (e Expr) ID() NodeID    { panic("not implemented") }

// WindowBuilder builds a window expression when a spec is applied.
type WindowBuilder struct {
	arena    *Arena
	fn       window.Function
	input    NodeID
	hasInput bool
}

// Over applies the window specification and returns a window expression.
func (w WindowBuilder) Over(spec *window.Spec) Expr {
	panic("not implemented")

}

func (e Expr) Add(other Expr) Expr { panic("not implemented") }
func (e Expr) Sub(other Expr) Expr { panic("not implemented") }
func (e Expr) Mul(other Expr) Expr { panic("not implemented") }
func (e Expr) Div(other Expr) Expr { panic("not implemented") }
func (e Expr) Eq(other Expr) Expr  { panic("not implemented") }
func (e Expr) Neq(other Expr) Expr { panic("not implemented") }
func (e Expr) Lt(other Expr) Expr  { panic("not implemented") }
func (e Expr) Lte(other Expr) Expr { panic("not implemented") }
func (e Expr) Gt(other Expr) Expr  { panic("not implemented") }
func (e Expr) Gte(other Expr) Expr { panic("not implemented") }
func (e Expr) And(other Expr) Expr { panic("not implemented") }
func (e Expr) Or(other Expr) Expr  { panic("not implemented") }
func (e Expr) Not() Expr           { panic("not implemented") }
func (e Expr) IsNull() Expr        { panic("not implemented") }
func (e Expr) IsNotNull() Expr     { panic("not implemented") }
func (e Expr) Sum() Expr           { panic("not implemented") }
func (e Expr) Mean() Expr          { panic("not implemented") }
func (e Expr) Min() Expr           { panic("not implemented") }
func (e Expr) Max() Expr           { panic("not implemented") }
func (e Expr) Count() Expr         { panic("not implemented") }
func (e Expr) Std() Expr           { panic("not implemented") }
func (e Expr) Var() Expr           { panic("not implemented") }
func (e Expr) First() Expr         { panic("not implemented") }
func (e Expr) Last() Expr          { panic("not implemented") }
func (e Expr) Median() Expr        { panic("not implemented") }
func (e Expr) Alias(name string) Expr {
	panic("not implemented")

}
func (e Expr) Cast(dt datatypes.DataType) Expr {
	panic("not implemented")

}

// Over converts an aggregate expression into a window expression.
func (e Expr) Over(spec *window.Spec) Expr {
	panic("not implemented")

}

// Lag builds a window LAG expression for the column.
func (e Expr) Lag(offset int, defaultValue ...interface{}) WindowBuilder {
	panic("not implemented")

}

// Lead builds a window LEAD expression for the column.
func (e Expr) Lead(offset int, defaultValue ...interface{}) WindowBuilder {
	panic("not implemented")

}

// FirstValue builds a FIRST_VALUE window expression for the column.
func (e Expr) FirstValue() WindowBuilder {
	panic("not implemented")

}

// LastValue builds a LAST_VALUE window expression for the column.
func (e Expr) LastValue() WindowBuilder {
	panic("not implemented")

}

func (e Expr) binary(op BinaryOp, other Expr) Expr {
	panic("not implemented")

}

func (e Expr) unary(op UnaryOp) Expr {
	panic("not implemented")

}

func (e Expr) agg(op AggOp) Expr {
	panic("not implemented")

}

func (e Expr) ensureSameArena(other Expr) {
	panic("not implemented")

}

func columnName(a *Arena, id NodeID) (string, bool) {
	panic("not implemented")

}

func windowFuncFromAgg(op AggOp, column string) (window.WindowFunc, bool) {
	panic("not implemented")

}
