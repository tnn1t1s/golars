package strings

import (
	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

// StringExpr provides string operations on expressions
type StringExpr struct {
	input expr.Expr
}

// Str creates a new StringExpr from an expression
func Str(e expr.Expr) *StringExpr {
	return &StringExpr{input: e}
}

// Length returns the length of each string
func (se *StringExpr) Length() expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "length",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.Length()
		},
		outputType: datatypes.Int32{},
	}
}

// RuneLength returns the number of UTF-8 runes in each string
func (se *StringExpr) RuneLength() expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "rune_length",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.RuneLength()
		},
		outputType: datatypes.Int32{},
	}
}

// ToUpper converts all characters to uppercase
func (se *StringExpr) ToUpper() expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "to_upper",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.ToUpper()
		},
		outputType: datatypes.String{},
	}
}

// ToLower converts all characters to lowercase
func (se *StringExpr) ToLower() expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "to_lower",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.ToLower()
		},
		outputType: datatypes.String{},
	}
}

// Contains checks if each string contains the pattern
func (se *StringExpr) Contains(pattern string, literal bool) expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "contains",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.Contains(pattern, literal)
		},
		outputType: datatypes.Boolean{},
	}
}

// StartsWith checks if each string starts with the pattern
func (se *StringExpr) StartsWith(pattern string) expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "starts_with",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.StartsWith(pattern)
		},
		outputType: datatypes.Boolean{},
	}
}

// EndsWith checks if each string ends with the pattern
func (se *StringExpr) EndsWith(pattern string) expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "ends_with",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.EndsWith(pattern)
		},
		outputType: datatypes.Boolean{},
	}
}

// Replace replaces occurrences of a pattern with a replacement string
func (se *StringExpr) Replace(pattern, replacement string, n int) expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "replace",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.Replace(pattern, replacement, n)
		},
		outputType: datatypes.String{},
	}
}

// Slice extracts a substring from each string
func (se *StringExpr) Slice(start, length int) expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "slice",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.Slice(start, length)
		},
		outputType: datatypes.String{},
	}
}

// Strip removes leading and trailing whitespace
func (se *StringExpr) Strip() expr.Expr {
	return &stringOpExpr{
		input: se.input,
		op:    "strip",
		apply: func(s series.Series) series.Series {
			ops := NewStringOps(s)
			return ops.Strip()
		},
		outputType: datatypes.String{},
	}
}

// stringOpExpr represents a string operation expression
type stringOpExpr struct {
	input      expr.Expr
	op         string
	apply      func(series.Series) series.Series
	outputType datatypes.DataType
}

func (e *stringOpExpr) String() string {
	return e.input.String() + "." + e.op + "()"
}

func (e *stringOpExpr) DataType() datatypes.DataType {
	return e.outputType
}

func (e *stringOpExpr) Alias(name string) expr.Expr {
	return &aliasExpr{expr: e, alias: name}
}

func (e *stringOpExpr) IsColumn() bool {
	return false
}

func (e *stringOpExpr) Name() string {
	return e.input.Name() + "." + e.op
}

// aliasExpr wraps another expression with a name
type aliasExpr struct {
	expr  expr.Expr
	alias string
}

func (e *aliasExpr) String() string {
	return e.expr.String() + ".alias(" + e.alias + ")"
}

func (e *aliasExpr) DataType() datatypes.DataType {
	return e.expr.DataType()
}

func (e *aliasExpr) Alias(name string) expr.Expr {
	return &aliasExpr{expr: e.expr, alias: name}
}

func (e *aliasExpr) IsColumn() bool {
	return false
}

func (e *aliasExpr) Name() string {
	return e.alias
}