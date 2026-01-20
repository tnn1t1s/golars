package window

import (
	"github.com/tnn1t1s/golars/expr"
)

// WindowFunc wraps a Function to provide the Over method
type WindowFunc struct {
	Function
}

// Over creates a window expression by applying a window specification
func (wf WindowFunc) Over(spec *Spec) expr.Expr {
	return NewExpr(wf.Function, spec)
}

// WrapFunction wraps a Function to provide the Over method
func WrapFunction(f Function) WindowFunc {
	return WindowFunc{f}
}
