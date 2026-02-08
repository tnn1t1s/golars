package window

import (
	"fmt"

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
	// Pass the spec to the function if it needs access to ordering info
	function.SetSpec(spec)
	return &Expr{
		function: function,
		spec:     spec,
	}
}

// NewAggregateExpr creates a new window expression for an aggregate function
func NewAggregateExpr(function Function, input expr.Expr, spec *Spec) *Expr {
	// Pass the spec to the function if it needs access to ordering info
	function.SetSpec(spec)
	return &Expr{
		function: function,
		spec:     spec,
		input:    input,
	}
}

// Over applies a window specification to create a window expression
func Over(function Function, spec *Spec) *Expr {
	return NewExpr(function, spec)
}

// String returns a string representation of the window expression
func (e *Expr) String() string {
	result := e.function.Name() + " OVER ("

	// Add PARTITION BY clause
	if e.spec.IsPartitioned() {
		result += "PARTITION BY "
		for i, col := range e.spec.GetPartitionBy() {
			if i > 0 {
				result += ", "
			}
			result += col
		}
	}

	// Add ORDER BY clause
	if e.spec.HasOrderBy() {
		if e.spec.IsPartitioned() {
			result += " "
		}
		result += "ORDER BY "
		for i, clause := range e.spec.GetOrderBy() {
			if i > 0 {
				result += ", "
			}
			result += clause.Column
			if clause.Ascending {
				result += " ASC"
			} else {
				result += " DESC"
			}
		}
	}

	// Add frame clause
	if e.spec.GetFrame() != nil {
		result += " " + e.frameString(e.spec.GetFrame())
	}

	result += ")"
	return result
}

// frameString converts a frame specification to string
func (e *Expr) frameString(frame *FrameSpec) string {
	var frameType string
	switch frame.Type {
	case RowsFrame:
		frameType = "ROWS"
	case RangeFrame:
		frameType = "RANGE"
	case GroupsFrame:
		frameType = "GROUPS"
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", frameType, e.boundString(frame.Start), e.boundString(frame.End))
}

// boundString converts a frame bound to string
func (e *Expr) boundString(bound FrameBound) string {
	switch bound.Type {
	case UnboundedPreceding:
		return "UNBOUNDED PRECEDING"
	case Preceding:
		return fmt.Sprintf("%v PRECEDING", bound.Offset)
	case CurrentRow:
		return "CURRENT ROW"
	case Following:
		return fmt.Sprintf("%v FOLLOWING", bound.Offset)
	case UnboundedFollowing:
		return "UNBOUNDED FOLLOWING"
	}
	return "UNKNOWN"
}

// DataType returns the expected output data type
func (e *Expr) DataType() datatypes.DataType {
	if e.input != nil {
		return e.function.DataType(e.input.DataType())
	}
	// For functions that don't take input (like row_number), use a default type
	return e.function.DataType(datatypes.Unknown{})
}

// Alias gives the expression a name
func (e *Expr) Alias(name string) expr.Expr {
	e.alias = name
	return e
}

// IsColumn returns false as window expressions are not column references
func (e *Expr) IsColumn() bool {
	return false
}

// Name returns the name of the expression (alias if set, otherwise function name)
func (e *Expr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.function.Name()
}

// GetFunction returns the window function
func (e *Expr) GetFunction() Function {
	return e.function
}

// GetSpec returns the window specification
func (e *Expr) GetSpec() *Spec {
	return e.spec
}

// GetInput returns the input expression (for aggregate window functions)
func (e *Expr) GetInput() expr.Expr {
	return e.input
}

// Validate checks if the window expression is valid
func (e *Expr) Validate() error {
	// Validate the window specification for this function
	return e.function.Validate(e.spec)
}
