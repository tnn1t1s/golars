package window

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// Expr represents a window expression that combines a function with a window specification
type Expr struct {
	function Function
	spec     *Spec
	alias    string
	input    expr.Expr // The input expression (for aggregates)
}

// NewExpr creates a new window expression
func NewExpr(function Function, spec *Spec) *Expr {
	panic(
		// Pass the spec to the function if it needs access to ordering info
		"not implemented")

}

// NewAggregateExpr creates a new window expression for an aggregate function
func NewAggregateExpr(function Function, input expr.Expr, spec *Spec) *Expr {
	panic(
		// Pass the spec to the function if it needs access to ordering info
		"not implemented")

}

// Over applies a window specification to create a window expression
func Over(function Function, spec *Spec) *Expr {
	panic("not implemented")

}

// String returns a string representation of the window expression
func (e *Expr) String() string {
	panic("not implemented")

	// Add PARTITION BY clause

	// Add ORDER BY clause

	// Add frame clause

}

// frameString converts a frame specification to string
func (e *Expr) frameString(frame *FrameSpec) string {
	panic("not implemented")

}

// boundString converts a frame bound to string
func (e *Expr) boundString(bound FrameBound) string {
	panic("not implemented")

}

// DataType returns the expected output data type
func (e *Expr) DataType() datatypes.DataType {
	panic("not implemented")

	// For functions that don't take input (like row_number), use a default type

}

// Alias gives the expression a name
func (e *Expr) Alias(name string) expr.Expr {
	panic("not implemented")

}

// IsColumn returns false as window expressions are not column references
func (e *Expr) IsColumn() bool {
	panic("not implemented")

}

// Name returns the name of the expression (alias if set, otherwise function name)
func (e *Expr) Name() string {
	panic("not implemented")

}

// GetFunction returns the window function
func (e *Expr) GetFunction() Function {
	panic("not implemented")

}

// GetSpec returns the window specification
func (e *Expr) GetSpec() *Spec {
	panic("not implemented")

}

// GetInput returns the input expression (for aggregate window functions)
func (e *Expr) GetInput() expr.Expr {
	panic("not implemented")

}

// Validate checks if the window expression is valid
func (e *Expr) Validate() error {
	panic(
		// Validate the window specification for this function
		"not implemented")

}
