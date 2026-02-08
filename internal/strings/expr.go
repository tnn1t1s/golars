package strings

import (
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// StringExpr provides string operations on expressions
type StringExpr struct {
	input expr.Expr
}

// Str creates a new StringExpr from an expression
func Str(e expr.Expr) *StringExpr {
	panic("not implemented")

}

// Length returns the length of each string
func (se *StringExpr) Length() expr.Expr {
	panic("not implemented")

}

// RuneLength returns the number of UTF-8 runes in each string
func (se *StringExpr) RuneLength() expr.Expr {
	panic("not implemented")

}

// ToUpper converts all characters to uppercase
func (se *StringExpr) ToUpper() expr.Expr {
	panic("not implemented")

}

// ToLower converts all characters to lowercase
func (se *StringExpr) ToLower() expr.Expr {
	panic("not implemented")

}

// Contains checks if each string contains the pattern
func (se *StringExpr) Contains(pattern string, literal bool) expr.Expr {
	panic("not implemented")

}

// StartsWith checks if each string starts with the pattern
func (se *StringExpr) StartsWith(pattern string) expr.Expr {
	panic("not implemented")

}

// EndsWith checks if each string ends with the pattern
func (se *StringExpr) EndsWith(pattern string) expr.Expr {
	panic("not implemented")

}

// Replace replaces occurrences of a pattern with a replacement string
func (se *StringExpr) Replace(pattern, replacement string, n int) expr.Expr {
	panic("not implemented")

}

// Slice extracts a substring from each string
func (se *StringExpr) Slice(start, length int) expr.Expr {
	panic("not implemented")

}

// Strip removes leading and trailing whitespace
func (se *StringExpr) Strip() expr.Expr {
	panic("not implemented")

}

// stringOpExpr represents a string operation expression
type stringOpExpr struct {
	input      expr.Expr
	op         string
	apply      func(series.Series) series.Series
	outputType datatypes.DataType
}

func (e *stringOpExpr) String() string {
	panic("not implemented")

}

func (e *stringOpExpr) DataType() datatypes.DataType {
	panic("not implemented")

}

func (e *stringOpExpr) Alias(name string) expr.Expr {
	panic("not implemented")

}

func (e *stringOpExpr) IsColumn() bool {
	panic("not implemented")

}

func (e *stringOpExpr) Name() string {
	panic("not implemented")

}

// aliasExpr wraps another expression with a name
type aliasExpr struct {
	expr  expr.Expr
	alias string
}

func (e *aliasExpr) String() string {
	panic("not implemented")

}

func (e *aliasExpr) DataType() datatypes.DataType {
	panic("not implemented")

}

func (e *aliasExpr) Alias(name string) expr.Expr {
	panic("not implemented")

}

func (e *aliasExpr) IsColumn() bool {
	panic("not implemented")

}

func (e *aliasExpr) Name() string {
	panic("not implemented")

}
