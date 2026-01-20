package expr

import (
	"fmt"
	"strings"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// CastExpr represents a type cast operation
type CastExpr struct {
	expr     Expr
	dataType datatypes.DataType
}

// Cast wraps an expression in a cast operation.
func Cast(expr Expr, dataType datatypes.DataType) *CastExpr {
	return &CastExpr{expr: expr, dataType: dataType}
}

func (e *CastExpr) String() string {
	return fmt.Sprintf("%s.cast(%v)", e.expr.String(), e.dataType)
}

func (e *CastExpr) DataType() datatypes.DataType {
	return e.dataType
}

func (e *CastExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *CastExpr) IsColumn() bool {
	return false
}

func (e *CastExpr) Name() string {
	return e.String()
}

// Expr returns the expression being cast
func (e *CastExpr) Expr() Expr {
	return e.expr
}

// TargetType returns the target data type
func (e *CastExpr) TargetType() datatypes.DataType {
	return e.dataType
}

// BetweenExpr represents a BETWEEN operation
type BetweenExpr struct {
	expr  Expr
	lower Expr
	upper Expr
}

func (e *BetweenExpr) String() string {
	return fmt.Sprintf("%s.between(%s, %s)", e.expr.String(), e.lower.String(), e.upper.String())
}

func (e *BetweenExpr) DataType() datatypes.DataType {
	return datatypes.Boolean{}
}

func (e *BetweenExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *BetweenExpr) IsColumn() bool {
	return false
}

func (e *BetweenExpr) Name() string {
	return e.String()
}

// Expr returns the expression being tested
func (e *BetweenExpr) Expr() Expr {
	return e.expr
}

// Lower returns the lower bound
func (e *BetweenExpr) Lower() Expr {
	return e.lower
}

// Upper returns the upper bound
func (e *BetweenExpr) Upper() Expr {
	return e.upper
}

// IsInExpr represents an IN operation
type IsInExpr struct {
	expr   Expr
	values []Expr
}

func (e *IsInExpr) String() string {
	valueStrs := make([]string, len(e.values))
	for i, v := range e.values {
		valueStrs[i] = v.String()
	}
	return fmt.Sprintf("%s.is_in([%s])", e.expr.String(), strings.Join(valueStrs, ", "))
}

func (e *IsInExpr) DataType() datatypes.DataType {
	return datatypes.Boolean{}
}

func (e *IsInExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *IsInExpr) IsColumn() bool {
	return false
}

func (e *IsInExpr) Name() string {
	return e.String()
}

// Expr returns the expression being tested
func (e *IsInExpr) Expr() Expr {
	return e.expr
}

// Values returns the list of values to check against
func (e *IsInExpr) Values() []Expr {
	return e.values
}

// Helper functions

// toDataType converts various inputs to a DataType
func toDataType(dtype interface{}) datatypes.DataType {
	switch d := dtype.(type) {
	case datatypes.DataType:
		return d
	case string:
		// Parse string representations
		switch strings.ToLower(d) {
		case "bool", "boolean":
			return datatypes.Boolean{}
		case "int8", "i8":
			return datatypes.Int8{}
		case "int16", "i16":
			return datatypes.Int16{}
		case "int32", "i32":
			return datatypes.Int32{}
		case "int64", "i64":
			return datatypes.Int64{}
		case "uint8", "u8":
			return datatypes.UInt8{}
		case "uint16", "u16":
			return datatypes.UInt16{}
		case "uint32", "u32":
			return datatypes.UInt32{}
		case "uint64", "u64":
			return datatypes.UInt64{}
		case "float32", "f32":
			return datatypes.Float32{}
		case "float64", "f64":
			return datatypes.Float64{}
		case "string", "str":
			return datatypes.String{}
		case "binary", "bytes":
			return datatypes.Binary{}
		default:
			return datatypes.Unknown{}
		}
	default:
		return datatypes.Unknown{}
	}
}

// toExprList converts various inputs to a list of expressions
func toExprList(values interface{}) []Expr {
	switch v := values.(type) {
	case []Expr:
		return v
	case []interface{}:
		exprs := make([]Expr, len(v))
		for i, val := range v {
			exprs[i] = toExpr(val)
		}
		return exprs
	case []string:
		exprs := make([]Expr, len(v))
		for i, val := range v {
			exprs[i] = Lit(val)
		}
		return exprs
	case []int:
		exprs := make([]Expr, len(v))
		for i, val := range v {
			exprs[i] = Lit(val)
		}
		return exprs
	case []int64:
		exprs := make([]Expr, len(v))
		for i, val := range v {
			exprs[i] = Lit(val)
		}
		return exprs
	case []float64:
		exprs := make([]Expr, len(v))
		for i, val := range v {
			exprs[i] = Lit(val)
		}
		return exprs
	default:
		// Try to convert single value to expression list
		return []Expr{toExpr(values)}
	}
}
