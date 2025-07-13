package expr

// Comparison methods for BinaryExpr to allow chaining

// Gt creates a greater than comparison
func (b *BinaryExpr) Gt(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpGreater,
	}
}

// Lt creates a less than comparison
func (b *BinaryExpr) Lt(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpLess,
	}
}

// Gte creates a greater than or equal comparison
func (b *BinaryExpr) Gte(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpGreaterEqual,
	}
}

// Lte creates a less than or equal comparison
func (b *BinaryExpr) Lte(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpLessEqual,
	}
}

// Ge is an alias for Gte (greater than or equal)
func (b *BinaryExpr) Ge(other interface{}) *BinaryExpr {
	return b.Gte(other)
}

// Le is an alias for Lte (less than or equal)
func (b *BinaryExpr) Le(other interface{}) *BinaryExpr {
	return b.Lte(other)
}

// Eq creates an equality comparison
func (b *BinaryExpr) Eq(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpEqual,
	}
}

// Ne creates a not equal comparison
func (b *BinaryExpr) Ne(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpNotEqual,
	}
}

// Logical operations for BinaryExpr

// And creates a logical AND expression
func (b *BinaryExpr) And(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpAnd,
	}
}

// Or creates a logical OR expression
func (b *BinaryExpr) Or(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpOr,
	}
}

// Not creates a logical NOT expression
func (b *BinaryExpr) Not() *UnaryExpr {
	return &UnaryExpr{
		expr: b,
		op:   OpNot,
	}
}

// Arithmetic operations for BinaryExpr

// Add creates an addition expression
func (b *BinaryExpr) Add(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpAdd,
	}
}

// Sub creates a subtraction expression
func (b *BinaryExpr) Sub(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpSubtract,
	}
}

// Mul creates a multiplication expression
func (b *BinaryExpr) Mul(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpMultiply,
	}
}

// Div creates a division expression
func (b *BinaryExpr) Div(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpDivide,
	}
}

// Mod creates a modulo expression
func (b *BinaryExpr) Mod(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  b,
		right: toExpr(other),
		op:    OpModulo,
	}
}

// Other operations

// Cast converts the expression to a different type
func (b *BinaryExpr) Cast(dtype interface{}) *CastExpr {
	return &CastExpr{
		expr:     b,
		dataType: toDataType(dtype),
	}
}

// IsNull checks if the expression value is null
func (b *BinaryExpr) IsNull() *UnaryExpr {
	return &UnaryExpr{
		expr: b,
		op:   OpIsNull,
	}
}

// IsNotNull checks if the expression value is not null
func (b *BinaryExpr) IsNotNull() *UnaryExpr {
	return &UnaryExpr{
		expr: b,
		op:   OpIsNotNull,
	}
}