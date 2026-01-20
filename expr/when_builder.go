package expr

// WhenBuilder helps build conditional expressions
type WhenBuilder struct {
	condition Expr
}

// When creates a new conditional expression
func When(condition Expr) *WhenBuilder {
	return &WhenBuilder{condition: condition}
}

// Then specifies the value when the condition is true
func (w *WhenBuilder) Then(value interface{}) *WhenThenBuilder {
	return &WhenThenBuilder{
		when: w.condition,
		then: toExpr(value),
	}
}

// WhenThenBuilder allows building when-then-otherwise expressions
type WhenThenBuilder struct {
	when Expr
	then Expr
}

// Otherwise specifies the default value when the condition is false
func (w *WhenThenBuilder) Otherwise(value interface{}) *WhenThenExpr {
	return &WhenThenExpr{
		when:      w.when,
		then:      w.then,
		otherwise: toExpr(value),
	}
}

// Build returns the when-then expression without otherwise
func (w *WhenThenBuilder) Build() *WhenThenExpr {
	return &WhenThenExpr{
		when: w.when,
		then: w.then,
	}
}
