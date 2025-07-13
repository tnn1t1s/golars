package expr

import (
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
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

func TestColumnExprMethods(t *testing.T) {
	// Test arithmetic operations
	arithExpr := Col("a").Add("b").Mul(2)
	assert.Equal(t, "((col(a) + col(b)) * lit(2))", arithExpr.String())
	
	// Test comparison operations
	compExpr := Col("age").Gt(18).And(Col("age").Lt(65))
	assert.Equal(t, "((col(age) > lit(18)) & (col(age) < lit(65)))", compExpr.String())
	
	// Test null operations
	nullExpr := Col("name").IsNotNull()
	assert.Equal(t, "col(name).is_not_null()", nullExpr.String())
	
	// Test aggregations
	aggExpr := Col("salary").Mean().Alias("avg_salary")
	assert.Equal(t, "col(salary).mean().alias(avg_salary)", aggExpr.String())
	assert.Equal(t, "avg_salary", aggExpr.Name())
}

func TestWhenThenExpr(t *testing.T) {
	// Test simple when-then
	whenExpr1 := When(Col("age").Gt(18)).Then(Lit("adult")).Build()
	assert.Equal(t, "when((col(age) > lit(18))).then(lit(adult))", whenExpr1.String())
	
	// Test when-then-otherwise
	whenExpr2 := When(Col("score").Ge(90)).
		Then("A").
		Otherwise("B")
	assert.Equal(t, "when((col(score) >= lit(90))).then(col(A)).otherwise(col(B))", whenExpr2.String())
}

func TestComplexExpressions(t *testing.T) {
	// Test complex arithmetic
	arithExpr := Col("a").Add(Col("b").Mul(2)).Div(Col("c").Sub(1))
	assert.Equal(t, "((col(a) + (col(b) * lit(2))) / (col(c) - lit(1)))", arithExpr.String())
	
	// Test complex logical
	logicExpr := Col("x").Gt(0).
		And(Col("y").Lt(100)).
		Or(Col("z").Eq(Lit("special")))
	assert.Equal(t, "(((col(x) > lit(0)) & (col(y) < lit(100))) | (col(z) == lit(special)))", logicExpr.String())
	
	// Test nested conditionals
	condExpr := When(Col("grade").Ge(90)).Then("A").
		Otherwise(
			When(Col("grade").Ge(80)).Then("B").Otherwise("C"),
		)
	expected := "when((col(grade) >= lit(90))).then(col(A)).otherwise(when((col(grade) >= lit(80))).then(col(B)).otherwise(col(C)))"
	assert.Equal(t, expected, condExpr.String())
}

func TestColumnExprChaining(t *testing.T) {
	// Test method chaining
	expr := Col("price").Mul(1.1).Add(5).Gt(100)
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
	
	// Test passing column expr
	col2 := Col("y")
	assert.Equal(t, col2, toExpr(col2))
}