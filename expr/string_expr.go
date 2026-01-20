package expr

// StringExpr provides string operations on expressions
type StringExpr struct {
	expr Expr
}

// Length returns the length of each string
func (se *StringExpr) Length() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrLength,
	}
}

// ToUpper converts all characters to uppercase
func (se *StringExpr) ToUpper() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrToUpper,
	}
}

// ToLower converts all characters to lowercase
func (se *StringExpr) ToLower() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrToLower,
	}
}

// Contains checks if each string contains the pattern
func (se *StringExpr) Contains(pattern string) Expr {
	return &BinaryExpr{
		left:  se.expr,
		right: Lit(pattern),
		op:    OpStrContains,
	}
}

// StartsWith checks if each string starts with the pattern
func (se *StringExpr) StartsWith(pattern string) Expr {
	return &BinaryExpr{
		left:  se.expr,
		right: Lit(pattern),
		op:    OpStrStartsWith,
	}
}

// EndsWith checks if each string ends with the pattern
func (se *StringExpr) EndsWith(pattern string) Expr {
	return &BinaryExpr{
		left:  se.expr,
		right: Lit(pattern),
		op:    OpStrEndsWith,
	}
}

// Replace replaces occurrences of pattern with replacement
func (se *StringExpr) Replace(pattern, replacement string) Expr {
	return &TernaryExpr{
		expr: se.expr,
		arg1: Lit(pattern),
		arg2: Lit(replacement),
		op:   OpStrReplace,
	}
}

// Strip removes leading and trailing whitespace
func (se *StringExpr) Strip() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrStrip,
	}
}

// ToInteger converts strings to integers
func (se *StringExpr) ToInteger() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrToInteger,
	}
}

// ToFloat converts strings to floating point numbers
func (se *StringExpr) ToFloat() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrToFloat,
	}
}

// ToBoolean converts strings to boolean values
func (se *StringExpr) ToBoolean() Expr {
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrToBoolean,
	}
}

// ToDateTime converts strings to datetime values
func (se *StringExpr) ToDateTime(format ...string) Expr {
	if len(format) > 0 {
		return &BinaryExpr{
			left:  se.expr,
			right: Lit(format[0]),
			op:    OpStrToDateTimeFormat,
		}
	}
	return &UnaryExpr{
		expr: se.expr,
		op:   OpStrToDateTime,
	}
}

// Encode encodes strings with specified encoding
func (se *StringExpr) Encode(encoding string) Expr {
	return &BinaryExpr{
		left:  se.expr,
		right: Lit(encoding),
		op:    OpStrEncode,
	}
}

// Decode decodes strings from specified encoding
func (se *StringExpr) Decode(encoding string) Expr {
	return &BinaryExpr{
		left:  se.expr,
		right: Lit(encoding),
		op:    OpStrDecode,
	}
}

// Format applies printf-style formatting
func (se *StringExpr) Format(formatStr string, args ...Expr) Expr {
	// For now, just support single format string
	return &BinaryExpr{
		left:  se.expr,
		right: Lit(formatStr),
		op:    OpStrFormat,
	}
}
