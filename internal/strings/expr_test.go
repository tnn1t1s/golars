package strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tnn1t1s/golars/expr"
)

func TestStringExpr_Methods(t *testing.T) {
	// Create a column expression
	col := expr.Col("name")

	// Create a StringExpr
	strExpr := Str(col)

	// Test Length
	lengthExpr := strExpr.Length()
	assert.Equal(t, "name.length", lengthExpr.Name())
	assert.Equal(t, "i32", lengthExpr.DataType().String())

	// Test ToUpper
	upperExpr := strExpr.ToUpper()
	assert.Equal(t, "name.to_upper", upperExpr.Name())
	assert.Equal(t, "str", upperExpr.DataType().String())

	// Test Contains
	containsExpr := strExpr.Contains("test", true)
	assert.Equal(t, "name.contains", containsExpr.Name())
	assert.Equal(t, "bool", containsExpr.DataType().String())

	// Test Replace
	replaceExpr := strExpr.Replace("old", "new", -1)
	assert.Equal(t, "name.replace", replaceExpr.Name())
	assert.Equal(t, "str", replaceExpr.DataType().String())
}
