package window

import (
	"fmt"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/expr"
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
	base := e.function.Name()
	if e.input != nil {
		base = fmt.Sprintf("%s(%s)", e.function.Name(), e.input.String())
	}
	
	over := "OVER ("
	
	// Add PARTITION BY clause
	if e.spec.IsPartitioned() {
		over += "PARTITION BY "
		for i, col := range e.spec.GetPartitionBy() {
			if i > 0 {
				over += ", "
			}
			over += col
		}
		over += " "
	}
	
	// Add ORDER BY clause
	if e.spec.HasOrderBy() {
		over += "ORDER BY "
		for i, ord := range e.spec.GetOrderBy() {
			if i > 0 {
				over += ", "
			}
			if ord.Column != "" {
				over += ord.Column
			} else {
				over += ord.Expr.String()
			}
			if !ord.Ascending {
				over += " DESC"
			}
		}
		over += " "
	}
	
	// Add frame clause
	if frame := e.spec.GetFrame(); frame != nil {
		frameStr := e.frameString(frame)
		if frameStr != "" {
			over += frameStr + " "
		}
	}
	
	over += ")"
	
	if e.alias != "" {
		return fmt.Sprintf("%s %s AS %s", base, over, e.alias)
	}
	
	return base + " " + over
}

// frameString converts a frame specification to string
func (e *Expr) frameString(frame *FrameSpec) string {
	if frame == nil {
		return ""
	}
	
	frameType := "ROWS"
	switch frame.Type {
	case RangeFrame:
		frameType = "RANGE"
	case GroupsFrame:
		frameType = "GROUPS"
	}
	
	start := e.boundString(frame.Start)
	end := e.boundString(frame.End)
	
	return fmt.Sprintf("%s BETWEEN %s AND %s", frameType, start, end)
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
	default:
		return "CURRENT ROW"
	}
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