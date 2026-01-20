package expr

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// Expr represents an expression that can be evaluated
type Expr interface {
	// String returns a string representation of the expression
	String() string

	// DataType returns the expected output data type (may return Unknown if not yet determined)
	DataType() datatypes.DataType

	// Alias gives the expression a name
	Alias(name string) Expr

	// IsColumn returns true if this is a column reference
	IsColumn() bool

	// Name returns the name of the expression (for columns and aliases)
	Name() string
}

// Base expression types

// ColumnExpr represents a reference to a column
type ColumnExpr struct {
	name string
}

// LiteralExpr represents a literal value
type LiteralExpr struct {
	value    interface{}
	dataType datatypes.DataType
}

// Value returns the literal value
func (e *LiteralExpr) Value() interface{} {
	return e.value
}

// AliasExpr wraps another expression with a name
type AliasExpr struct {
	expr  Expr
	alias string
}

// BinaryExpr represents a binary operation
type BinaryExpr struct {
	left  Expr
	right Expr
	op    BinaryOp
}

// Left returns the left expression
func (e *BinaryExpr) Left() Expr {
	return e.left
}

// Right returns the right expression
func (e *BinaryExpr) Right() Expr {
	return e.right
}

// Op returns the binary operation
func (e *BinaryExpr) Op() BinaryOp {
	return e.op
}

// UnaryExpr represents a unary operation
type UnaryExpr struct {
	expr Expr
	op   UnaryOp
}

// Expr returns the inner expression
func (e *UnaryExpr) Expr() Expr {
	return e.expr
}

// Op returns the unary operation
func (e *UnaryExpr) Op() UnaryOp {
	return e.op
}

// WhenThenExpr represents conditional logic
type WhenThenExpr struct {
	when      Expr
	then      Expr
	otherwise Expr
}

// TernaryExpr represents operations with three operands
type TernaryExpr struct {
	expr Expr
	arg1 Expr
	arg2 Expr
	op   TernaryOp
}

// TernaryOp represents ternary operations
type TernaryOp int

const (
	OpStrReplace TernaryOp = iota
)

// AggExpr represents an aggregation
type AggExpr struct {
	expr  Expr
	aggOp AggOp
}

// TopKExpr represents a top-k aggregation
type TopKExpr struct {
	expr    Expr
	k       int
	largest bool // true for top-k, false for bottom-k
}

// Input returns the input expression
func (e *AggExpr) Input() Expr {
	return e.expr
}

// AggType returns the aggregation operation type
func (e *AggExpr) AggType() AggOp {
	return e.aggOp
}

// BinaryOp represents binary operations
type BinaryOp int

const (
	OpAdd BinaryOp = iota
	OpSubtract
	OpMultiply
	OpDivide
	OpModulo
	OpEqual
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
	OpAnd
	OpOr
	// String operations
	OpStrContains
	OpStrStartsWith
	OpStrEndsWith
	OpStrEncode
	OpStrDecode
	OpStrFormat
	OpStrToDateTimeFormat
)

// UnaryOp represents unary operations
type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNegate
	OpIsNull
	OpIsNotNull
	// String operations
	OpStrLength
	OpStrToUpper
	OpStrToLower
	OpStrStrip
	OpStrToInteger
	OpStrToFloat
	OpStrToBoolean
	OpStrToDateTime
)

// AggOp represents aggregation operations
type AggOp int

const (
	AggSum AggOp = iota
	AggMean
	AggMin
	AggMax
	AggCount
	AggStd
	AggVar
	AggFirst
	AggLast
	AggMedian
	AggTopK
)

// Constructor functions

// Col creates a column reference expression
func Col(name string) *ColumnExpr {
	return &ColumnExpr{name: name}
}

// Lit creates a literal expression
func Lit(value interface{}) Expr {
	var dt datatypes.DataType

	switch v := value.(type) {
	case bool:
		dt = datatypes.Boolean{}
	case int:
		dt = datatypes.Int64{}
		value = int64(v)
	case int8:
		dt = datatypes.Int8{}
	case int16:
		dt = datatypes.Int16{}
	case int32:
		dt = datatypes.Int32{}
	case int64:
		dt = datatypes.Int64{}
	case uint8:
		dt = datatypes.UInt8{}
	case uint16:
		dt = datatypes.UInt16{}
	case uint32:
		dt = datatypes.UInt32{}
	case uint64:
		dt = datatypes.UInt64{}
	case float32:
		dt = datatypes.Float32{}
	case float64:
		dt = datatypes.Float64{}
	case string:
		dt = datatypes.String{}
	case []byte:
		dt = datatypes.Binary{}
	case nil:
		dt = datatypes.Null{}
	default:
		dt = datatypes.Unknown{}
	}

	return &LiteralExpr{value: value, dataType: dt}
}

// Implementation of Expr interface for ColumnExpr

func (e *ColumnExpr) String() string {
	return fmt.Sprintf("col(%s)", e.name)
}

func (e *ColumnExpr) DataType() datatypes.DataType {
	return datatypes.Unknown{} // Will be resolved during planning
}

func (e *ColumnExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *ColumnExpr) IsColumn() bool {
	return true
}

func (e *ColumnExpr) Name() string {
	return e.name
}

// Implementation of Expr interface for LiteralExpr

func (e *LiteralExpr) String() string {
	if e.value == nil {
		return "null"
	}
	return fmt.Sprintf("lit(%v)", e.value)
}

func (e *LiteralExpr) DataType() datatypes.DataType {
	return e.dataType
}

func (e *LiteralExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *LiteralExpr) IsColumn() bool {
	return false
}

func (e *LiteralExpr) Name() string {
	return ""
}

// Implementation of Expr interface for AliasExpr

func (e *AliasExpr) String() string {
	return fmt.Sprintf("%s.alias(%s)", e.expr.String(), e.alias)
}

func (e *AliasExpr) DataType() datatypes.DataType {
	return e.expr.DataType()
}

func (e *AliasExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e.expr, alias: name}
}

func (e *AliasExpr) IsColumn() bool {
	return false
}

func (e *AliasExpr) Name() string {
	return e.alias
}

// Implementation of Expr interface for BinaryExpr

func (e *BinaryExpr) String() string {
	op := ""
	switch e.op {
	case OpAdd:
		op = "+"
	case OpSubtract:
		op = "-"
	case OpMultiply:
		op = "*"
	case OpDivide:
		op = "/"
	case OpModulo:
		op = "%"
	case OpEqual:
		op = "=="
	case OpNotEqual:
		op = "!="
	case OpLess:
		op = "<"
	case OpLessEqual:
		op = "<="
	case OpGreater:
		op = ">"
	case OpGreaterEqual:
		op = ">="
	case OpAnd:
		op = "&"
	case OpOr:
		op = "|"
	case OpStrContains:
		return fmt.Sprintf("%s.str_contains(%s)", e.left.String(), e.right.String())
	case OpStrStartsWith:
		return fmt.Sprintf("%s.str_starts_with(%s)", e.left.String(), e.right.String())
	case OpStrEndsWith:
		return fmt.Sprintf("%s.str_ends_with(%s)", e.left.String(), e.right.String())
	case OpStrEncode:
		return fmt.Sprintf("%s.str_encode(%s)", e.left.String(), e.right.String())
	case OpStrDecode:
		return fmt.Sprintf("%s.str_decode(%s)", e.left.String(), e.right.String())
	case OpStrFormat:
		return fmt.Sprintf("%s.str_format(%s)", e.left.String(), e.right.String())
	case OpStrToDateTimeFormat:
		return fmt.Sprintf("%s.str_to_datetime(%s)", e.left.String(), e.right.String())
	}
	return fmt.Sprintf("(%s %s %s)", e.left.String(), op, e.right.String())
}

func (e *BinaryExpr) DataType() datatypes.DataType {
	// Comparison operators return boolean
	switch e.op {
	case OpEqual, OpNotEqual, OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return datatypes.Boolean{}
	case OpAnd, OpOr:
		return datatypes.Boolean{}
	case OpStrContains, OpStrStartsWith, OpStrEndsWith:
		return datatypes.Boolean{}
	default:
		// For arithmetic operations, return the data type of the left operand
		// This is simplified; real implementation would do type promotion
		return e.left.DataType()
	}
}

func (e *BinaryExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *BinaryExpr) IsColumn() bool {
	return false
}

func (e *BinaryExpr) Name() string {
	return ""
}

// Implementation of Expr interface for UnaryExpr

func (e *UnaryExpr) String() string {
	switch e.op {
	case OpNot:
		return fmt.Sprintf("!%s", e.expr.String())
	case OpNegate:
		return fmt.Sprintf("-%s", e.expr.String())
	case OpIsNull:
		return fmt.Sprintf("%s.is_null()", e.expr.String())
	case OpIsNotNull:
		return fmt.Sprintf("%s.is_not_null()", e.expr.String())
	case OpStrLength:
		return fmt.Sprintf("%s.str_length()", e.expr.String())
	case OpStrToUpper:
		return fmt.Sprintf("%s.str_to_upper()", e.expr.String())
	case OpStrToLower:
		return fmt.Sprintf("%s.str_to_lower()", e.expr.String())
	case OpStrStrip:
		return fmt.Sprintf("%s.str_strip()", e.expr.String())
	case OpStrToInteger:
		return fmt.Sprintf("%s.str_to_integer()", e.expr.String())
	case OpStrToFloat:
		return fmt.Sprintf("%s.str_to_float()", e.expr.String())
	case OpStrToBoolean:
		return fmt.Sprintf("%s.str_to_boolean()", e.expr.String())
	case OpStrToDateTime:
		return fmt.Sprintf("%s.str_to_datetime()", e.expr.String())
	default:
		return fmt.Sprintf("unary(%s)", e.expr.String())
	}
}

func (e *UnaryExpr) DataType() datatypes.DataType {
	switch e.op {
	case OpNot, OpIsNull, OpIsNotNull:
		return datatypes.Boolean{}
	case OpNegate:
		return e.expr.DataType()
	case OpStrLength:
		return datatypes.Int32{}
	case OpStrToUpper, OpStrToLower, OpStrStrip:
		return datatypes.String{}
	case OpStrToInteger:
		return datatypes.Int64{} // Default to Int64 for safety
	case OpStrToFloat:
		return datatypes.Float64{}
	case OpStrToBoolean:
		return datatypes.Boolean{}
	case OpStrToDateTime:
		return datatypes.Datetime{Unit: datatypes.Nanoseconds}
	default:
		return datatypes.Unknown{}
	}
}

func (e *UnaryExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *UnaryExpr) IsColumn() bool {
	return false
}

func (e *UnaryExpr) Name() string {
	return ""
}

// Implementation of Expr interface for AggExpr

func (e *AggExpr) String() string {
	aggName := ""
	switch e.aggOp {
	case AggSum:
		aggName = "sum"
	case AggMean:
		aggName = "mean"
	case AggMin:
		aggName = "min"
	case AggMax:
		aggName = "max"
	case AggCount:
		aggName = "count"
	case AggStd:
		aggName = "std"
	case AggVar:
		aggName = "var"
	case AggFirst:
		aggName = "first"
	case AggLast:
		aggName = "last"
	case AggMedian:
		aggName = "median"
	}
	return fmt.Sprintf("%s.%s()", e.expr.String(), aggName)
}

func (e *AggExpr) DataType() datatypes.DataType {
	switch e.aggOp {
	case AggCount:
		return datatypes.UInt64{}
	case AggMean, AggStd, AggVar, AggMedian:
		return datatypes.Float64{}
	default:
		return e.expr.DataType()
	}
}

func (e *AggExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *AggExpr) IsColumn() bool {
	return false
}

func (e *AggExpr) Name() string {
	return ""
}

// Implementation of Expr interface for WhenThenExpr

func (e *WhenThenExpr) String() string {
	if e.otherwise != nil {
		return fmt.Sprintf("when(%s).then(%s).otherwise(%s)",
			e.when.String(), e.then.String(), e.otherwise.String())
	}
	return fmt.Sprintf("when(%s).then(%s)", e.when.String(), e.then.String())
}

func (e *WhenThenExpr) DataType() datatypes.DataType {
	return e.then.DataType()
}

func (e *WhenThenExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *WhenThenExpr) IsColumn() bool {
	return false
}

func (e *WhenThenExpr) Name() string {
	return ""
}

// Implementation of Expr interface for TernaryExpr

func (e *TernaryExpr) String() string {
	switch e.op {
	case OpStrReplace:
		return fmt.Sprintf("%s.replace(%s, %s)", e.expr.String(), e.arg1.String(), e.arg2.String())
	default:
		return fmt.Sprintf("ternary(%s, %s, %s)", e.expr.String(), e.arg1.String(), e.arg2.String())
	}
}

func (e *TernaryExpr) DataType() datatypes.DataType {
	switch e.op {
	case OpStrReplace:
		return datatypes.String{}
	default:
		return datatypes.Unknown{}
	}
}

func (e *TernaryExpr) Alias(name string) Expr {
	return &AliasExpr{expr: e, alias: name}
}

func (e *TernaryExpr) IsColumn() bool {
	return false
}

func (e *TernaryExpr) Name() string {
	return ""
}
