package expr

// Comparison methods for ColumnExpr

// Gt creates a greater than comparison
func (c *ColumnExpr) Gt(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpGreater,
	}
}

// Lt creates a less than comparison
func (c *ColumnExpr) Lt(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpLess,
	}
}

// Gte creates a greater than or equal comparison
func (c *ColumnExpr) Gte(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpGreaterEqual,
	}
}

// Lte creates a less than or equal comparison
func (c *ColumnExpr) Lte(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpLessEqual,
	}
}

// Eq creates an equality comparison
func (c *ColumnExpr) Eq(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpEqual,
	}
}

// EqMissing creates an equality comparison where null == null is true.
func (c *ColumnExpr) EqMissing(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpEqualMissing,
	}
}

// Ne creates a not equal comparison
func (c *ColumnExpr) Ne(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpNotEqual,
	}
}

// Ge is an alias for Gte (greater than or equal)
func (c *ColumnExpr) Ge(other interface{}) *BinaryExpr {
	return c.Gte(other)
}

// Le is an alias for Lte (less than or equal)
func (c *ColumnExpr) Le(other interface{}) *BinaryExpr {
	return c.Lte(other)
}

// Logical operations for ColumnExpr

// And creates a logical AND expression
func (c *ColumnExpr) And(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpAnd,
	}
}

// Or creates a logical OR expression
func (c *ColumnExpr) Or(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpOr,
	}
}

// Not creates a logical NOT expression
func (c *ColumnExpr) Not() *UnaryExpr {
	return &UnaryExpr{
		expr: c,
		op:   OpNot,
	}
}

// Arithmetic operations for ColumnExpr

// Add creates an addition expression
func (c *ColumnExpr) Add(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpAdd,
	}
}

// Sub creates a subtraction expression
func (c *ColumnExpr) Sub(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpSubtract,
	}
}

// Mul creates a multiplication expression
func (c *ColumnExpr) Mul(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpMultiply,
	}
}

// Div creates a division expression
func (c *ColumnExpr) Div(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpDivide,
	}
}

// Mod creates a modulo expression
func (c *ColumnExpr) Mod(other interface{}) *BinaryExpr {
	return &BinaryExpr{
		left:  c,
		right: toExpr(other),
		op:    OpModulo,
	}
}

// Null operations

// IsNull checks if the column value is null
func (c *ColumnExpr) IsNull() *UnaryExpr {
	return &UnaryExpr{
		expr: c,
		op:   OpIsNull,
	}
}

// IsNotNull checks if the column value is not null
func (c *ColumnExpr) IsNotNull() *UnaryExpr {
	return &UnaryExpr{
		expr: c,
		op:   OpIsNotNull,
	}
}

// Aggregation methods for ColumnExpr

// Sum creates a sum aggregation
func (c *ColumnExpr) Sum() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggSum,
	}
}

// Mean creates a mean aggregation
func (c *ColumnExpr) Mean() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggMean,
	}
}

// Min creates a min aggregation
func (c *ColumnExpr) Min() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggMin,
	}
}

// Max creates a max aggregation
func (c *ColumnExpr) Max() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggMax,
	}
}

// Count creates a count aggregation
func (c *ColumnExpr) Count() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggCount,
	}
}

// Std creates a standard deviation aggregation
func (c *ColumnExpr) Std() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggStd,
	}
}

// Var creates a variance aggregation
func (c *ColumnExpr) Var() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggVar,
	}
}

// Median creates a median aggregation
func (c *ColumnExpr) Median() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggMedian,
	}
}

// First returns the first value
func (c *ColumnExpr) First() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggFirst,
	}
}

// Last returns the last value
func (c *ColumnExpr) Last() *AggExpr {
	return &AggExpr{
		expr:  c,
		aggOp: AggLast,
	}
}

// TopK returns the top k largest values
func (c *ColumnExpr) TopK(k int) *TopKExpr {
	return &TopKExpr{
		expr:    c,
		k:       k,
		largest: true,
	}
}

// BottomK returns the k smallest values
func (c *ColumnExpr) BottomK(k int) *TopKExpr {
	return &TopKExpr{
		expr:    c,
		k:       k,
		largest: false,
	}
}

// Corr creates a correlation expression between this column and another
func (c *ColumnExpr) Corr(other *ColumnExpr) *CorrExpr {
	return &CorrExpr{
		col1: c,
		col2: other,
	}
}

// Other operations

// Cast converts the column to a different type
func (c *ColumnExpr) Cast(dtype interface{}) *CastExpr {
	return &CastExpr{
		expr:     c,
		dataType: toDataType(dtype),
	}
}

// Between checks if values are between two bounds
func (c *ColumnExpr) Between(lower, upper interface{}) *BetweenExpr {
	return &BetweenExpr{
		expr:  c,
		lower: toExpr(lower),
		upper: toExpr(upper),
	}
}

// IsIn checks if values are in a list
func (c *ColumnExpr) IsIn(values interface{}) *IsInExpr {
	return &IsInExpr{
		expr:   c,
		values: toExprList(values),
	}
}
