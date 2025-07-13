package expr

import (
	"testing"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/stretchr/testify/assert"
)

func TestColumnExpr(t *testing.T) {
	col := Col("test_col")
	
	assert.Equal(t, "col(test_col)", col.String())
	assert.True(t, col.IsColumn())
	assert.Equal(t, "test_col", col.Name())
	assert.Equal(t, datatypes.Unknown{}, col.DataType())
	
	// Test alias
	aliased := col.Alias("new_name")
	assert.Equal(t, "col(test_col).alias(new_name)", aliased.String())
	assert.Equal(t, "new_name", aliased.Name())
}

func TestLiteralExpr(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
		dataType datatypes.DataType
	}{
		{42, "lit(42)", datatypes.Int64{}},
		{int32(42), "lit(42)", datatypes.Int32{}},
		{3.14, "lit(3.14)", datatypes.Float64{}},
		{float32(3.14), "lit(3.14)", datatypes.Float32{}},
		{"hello", "lit(hello)", datatypes.String{}},
		{true, "lit(true)", datatypes.Boolean{}},
		{nil, "null", datatypes.Null{}},
	}
	
	for _, test := range tests {
		lit := Lit(test.value)
		assert.Equal(t, test.expected, lit.String())
		assert.False(t, lit.IsColumn())
		assert.Equal(t, test.dataType, lit.DataType())
	}
}

func TestBinaryExpr(t *testing.T) {
	col1 := Col("a")
	col2 := Col("b")
	
	tests := []struct {
		expr     Expr
		expected string
		isBoolean bool
	}{
		{&BinaryExpr{col1, col2, OpAdd}, "(col(a) + col(b))", false},
		{&BinaryExpr{col1, col2, OpSubtract}, "(col(a) - col(b))", false},
		{&BinaryExpr{col1, col2, OpMultiply}, "(col(a) * col(b))", false},
		{&BinaryExpr{col1, col2, OpDivide}, "(col(a) / col(b))", false},
		{&BinaryExpr{col1, col2, OpEqual}, "(col(a) == col(b))", true},
		{&BinaryExpr{col1, col2, OpNotEqual}, "(col(a) != col(b))", true},
		{&BinaryExpr{col1, col2, OpLess}, "(col(a) < col(b))", true},
		{&BinaryExpr{col1, col2, OpGreater}, "(col(a) > col(b))", true},
		{&BinaryExpr{col1, col2, OpAnd}, "(col(a) & col(b))", true},
		{&BinaryExpr{col1, col2, OpOr}, "(col(a) | col(b))", true},
	}
	
	for _, test := range tests {
		assert.Equal(t, test.expected, test.expr.String())
		if test.isBoolean {
			assert.Equal(t, datatypes.Boolean{}, test.expr.DataType())
		}
	}
}

func TestUnaryExpr(t *testing.T) {
	col := Col("a")
	
	tests := []struct {
		expr     Expr
		expected string
		dataType datatypes.DataType
	}{
		{&UnaryExpr{col, OpNot}, "!col(a)", datatypes.Boolean{}},
		{&UnaryExpr{col, OpNegate}, "-col(a)", datatypes.Unknown{}},
		{&UnaryExpr{col, OpIsNull}, "col(a).is_null()", datatypes.Boolean{}},
		{&UnaryExpr{col, OpIsNotNull}, "col(a).is_not_null()", datatypes.Boolean{}},
	}
	
	for _, test := range tests {
		assert.Equal(t, test.expected, test.expr.String())
		assert.Equal(t, test.dataType, test.expr.DataType())
	}
}

func TestAggExpr(t *testing.T) {
	col := Col("value")
	
	tests := []struct {
		expr     Expr
		expected string
		dataType datatypes.DataType
	}{
		{&AggExpr{col, AggSum}, "col(value).sum()", datatypes.Unknown{}},
		{&AggExpr{col, AggMean}, "col(value).mean()", datatypes.Float64{}},
		{&AggExpr{col, AggMin}, "col(value).min()", datatypes.Unknown{}},
		{&AggExpr{col, AggMax}, "col(value).max()", datatypes.Unknown{}},
		{&AggExpr{col, AggCount}, "col(value).count()", datatypes.UInt64{}},
		{&AggExpr{col, AggStd}, "col(value).std()", datatypes.Float64{}},
		{&AggExpr{col, AggVar}, "col(value).var()", datatypes.Float64{}},
	}
	
	for _, test := range tests {
		assert.Equal(t, test.expected, test.expr.String())
		assert.Equal(t, test.dataType, test.expr.DataType())
	}
}

func TestExprBuilder(t *testing.T) {
	// Test arithmetic operations
	expr := ColBuilder("a").Add("b").Mul(2).Build()
	assert.Equal(t, "((col(a) + col(b)) * lit(2))", expr.String())
	
	// Test comparison operations
	expr = ColBuilder("age").Gt(18).And(ColBuilder("age").Lt(65)).Build()
	assert.Equal(t, "((col(age) > lit(18)) & (col(age) < lit(65)))", expr.String())
	
	// Test null operations
	expr = ColBuilder("name").IsNotNull().Build()
	assert.Equal(t, "col(name).is_not_null()", expr.String())
	
	// Test aggregations
	expr = ColBuilder("salary").Mean().Alias("avg_salary").Build()
	assert.Equal(t, "col(salary).mean().alias(avg_salary)", expr.String())
	assert.Equal(t, "avg_salary", expr.Name())
}

func TestWhenThenExpr(t *testing.T) {
	// Test simple when-then
	expr := When(ColBuilder("age").Gt(18)).Then(Lit("adult")).Build()
	assert.Equal(t, "when((col(age) > lit(18))).then(lit(adult))", expr.String())
	
	// Test when-then-otherwise
	expr = When(ColBuilder("score").Ge(90)).
		Then("A").
		Otherwise("B").
		Build()
	assert.Equal(t, "when((col(score) >= lit(90))).then(col(A)).otherwise(col(B))", expr.String())
}

func TestComplexExpressions(t *testing.T) {
	// Test complex arithmetic
	expr := ColBuilder("a").Add(ColBuilder("b").Mul(2)).Div(ColBuilder("c").Sub(1)).Build()
	assert.Equal(t, "((col(a) + (col(b) * lit(2))) / (col(c) - lit(1)))", expr.String())
	
	// Test complex logical
	expr = ColBuilder("x").Gt(0).
		And(ColBuilder("y").Lt(100)).
		Or(ColBuilder("z").Eq(Lit("special"))).
		Build()
	assert.Equal(t, "(((col(x) > lit(0)) & (col(y) < lit(100))) | (col(z) == lit(special)))", expr.String())
	
	// Test nested conditionals
	expr = When(ColBuilder("grade").Ge(90)).Then("A").
		Otherwise(
			When(ColBuilder("grade").Ge(80)).Then("B").Otherwise("C"),
		).Build()
	expected := "when((col(grade) >= lit(90))).then(col(A)).otherwise(when((col(grade) >= lit(80))).then(col(B)).otherwise(col(C)))"
	assert.Equal(t, expected, expr.String())
}

func TestExprBuilderChaining(t *testing.T) {
	// Test method chaining
	builder := ColBuilder("price")
	expr := builder.Mul(1.1).Add(5).Gt(100).Build()
	assert.Equal(t, "(((col(price) * lit(1.1)) + lit(5)) > lit(100))", expr.String())
	assert.Equal(t, datatypes.Boolean{}, expr.DataType())
}

func TestToExpr(t *testing.T) {
	// Test various input types
	assert.Equal(t, "col(test)", toExpr("test").String())
	assert.Equal(t, "lit(42)", toExpr(42).String())
	assert.Equal(t, "lit(3.14)", toExpr(3.14).String())
	assert.Equal(t, "lit(true)", toExpr(true).String())
	
	// Test passing expression directly
	col := Col("x")
	assert.Equal(t, col, toExpr(col))
	
	// Test passing builder
	builder := ColBuilder("y")
	assert.Equal(t, builder.expr, toExpr(builder))
}