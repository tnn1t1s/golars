package expr

// ExprBuilder provides fluent API for building expressions
type ExprBuilder struct {
	expr Expr
}

// NewBuilder creates a new expression builder
func NewBuilder(expr Expr) *ExprBuilder {
	return &ExprBuilder{expr: expr}
}

// Str returns a StringExpr for string operations
func (b *ExprBuilder) Str() *StringExpr {
	return &StringExpr{expr: b.expr}
}

// Dt returns datetime operations for the expression
// This is defined in the datetime package to avoid circular imports
func (b *ExprBuilder) Dt() interface{} {
	// This will be overridden by the datetime package
	return nil
}

// Arithmetic operations

// Add creates an addition expression
func (b *ExprBuilder) Add(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpAdd,
		},
	}
}

// Sub creates a subtraction expression
func (b *ExprBuilder) Sub(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpSubtract,
		},
	}
}

// Mul creates a multiplication expression
func (b *ExprBuilder) Mul(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpMultiply,
		},
	}
}

// Div creates a division expression
func (b *ExprBuilder) Div(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpDivide,
		},
	}
}

// Mod creates a modulo expression
func (b *ExprBuilder) Mod(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpModulo,
		},
	}
}

// Comparison operations

// Eq creates an equality expression
func (b *ExprBuilder) Eq(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpEqual,
		},
	}
}

// Ne creates a not-equal expression
func (b *ExprBuilder) Ne(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpNotEqual,
		},
	}
}

// Lt creates a less-than expression
func (b *ExprBuilder) Lt(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpLess,
		},
	}
}

// Le creates a less-than-or-equal expression
func (b *ExprBuilder) Le(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpLessEqual,
		},
	}
}

// Gt creates a greater-than expression
func (b *ExprBuilder) Gt(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpGreater,
		},
	}
}

// Ge creates a greater-than-or-equal expression
func (b *ExprBuilder) Ge(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpGreaterEqual,
		},
	}
}

// Logical operations

// And creates a logical AND expression
func (b *ExprBuilder) And(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpAnd,
		},
	}
}

// Or creates a logical OR expression
func (b *ExprBuilder) Or(other interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &BinaryExpr{
			left:  b.expr,
			right: toExpr(other),
			op:    OpOr,
		},
	}
}

// Not creates a logical NOT expression
func (b *ExprBuilder) Not() *ExprBuilder {
	return &ExprBuilder{
		expr: &UnaryExpr{
			expr: b.expr,
			op:   OpNot,
		},
	}
}

// Null operations

// IsNull creates an is-null check expression
func (b *ExprBuilder) IsNull() *ExprBuilder {
	return &ExprBuilder{
		expr: &UnaryExpr{
			expr: b.expr,
			op:   OpIsNull,
		},
	}
}

// IsNotNull creates an is-not-null check expression
func (b *ExprBuilder) IsNotNull() *ExprBuilder {
	return &ExprBuilder{
		expr: &UnaryExpr{
			expr: b.expr,
			op:   OpIsNotNull,
		},
	}
}

// Aggregation operations

// Sum creates a sum aggregation
func (b *ExprBuilder) Sum() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggSum,
		},
	}
}

// Mean creates a mean aggregation
func (b *ExprBuilder) Mean() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggMean,
		},
	}
}

// Min creates a min aggregation
func (b *ExprBuilder) Min() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggMin,
		},
	}
}

// Max creates a max aggregation
func (b *ExprBuilder) Max() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggMax,
		},
	}
}

// Count creates a count aggregation
func (b *ExprBuilder) Count() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggCount,
		},
	}
}

// Std creates a standard deviation aggregation
func (b *ExprBuilder) Std() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggStd,
		},
	}
}

// Var creates a variance aggregation
func (b *ExprBuilder) Var() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggVar,
		},
	}
}

// First creates a first value aggregation
func (b *ExprBuilder) First() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggFirst,
		},
	}
}

// Last creates a last value aggregation
func (b *ExprBuilder) Last() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggLast,
		},
	}
}

// Median creates a median aggregation
func (b *ExprBuilder) Median() *ExprBuilder {
	return &ExprBuilder{
		expr: &AggExpr{
			expr:  b.expr,
			aggOp: AggMedian,
		},
	}
}

// Other operations

// Alias gives the expression a name
func (b *ExprBuilder) Alias(name string) *ExprBuilder {
	return &ExprBuilder{
		expr: b.expr.Alias(name),
	}
}

// Build returns the built expression
func (b *ExprBuilder) Build() Expr {
	return b.expr
}

// Expr returns the underlying expression (same as Build)
func (b *ExprBuilder) Expr() Expr {
	return b.expr
}

// Helper function to convert various types to expressions
func toExpr(v interface{}) Expr {
	switch val := v.(type) {
	case Expr:
		return val
	case *ExprBuilder:
		return val.expr
	case string:
		// Assume it's a column name if it's a string
		return Col(val)
	default:
		// Otherwise treat as literal
		return Lit(val)
	}
}

// Convenience functions for creating expression builders

// ColBuilder creates a column expression builder
func ColBuilder(name string) *ExprBuilder {
	return NewBuilder(Col(name))
}

// LitBuilder creates a literal expression builder
func LitBuilder(value interface{}) *ExprBuilder {
	return NewBuilder(Lit(value))
}

// When creates a conditional expression builder
func When(condition interface{}) *WhenBuilder {
	return &WhenBuilder{
		when: toExpr(condition),
	}
}

// WhenBuilder helps build conditional expressions
type WhenBuilder struct {
	when Expr
	then Expr
}

// Then sets the then clause
func (w *WhenBuilder) Then(expr interface{}) *WhenThenBuilder {
	return &WhenThenBuilder{
		when: w.when,
		then: toExpr(expr),
	}
}

// WhenThenBuilder represents a when-then expression being built
type WhenThenBuilder struct {
	when      Expr
	then      Expr
	otherwise Expr
}

// Otherwise sets the otherwise clause
func (w *WhenThenBuilder) Otherwise(expr interface{}) *ExprBuilder {
	return &ExprBuilder{
		expr: &WhenThenExpr{
			when:      w.when,
			then:      w.then,
			otherwise: toExpr(expr),
		},
	}
}

// Build builds the expression without otherwise clause
func (w *WhenThenBuilder) Build() Expr {
	return &WhenThenExpr{
		when: w.when,
		then: w.then,
	}
}