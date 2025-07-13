package strings

import (
	"strings"

	"github.com/tnn1t1s/golars/series"
)

// Trim removes leading and trailing characters (default: whitespace)
func (so *StringOps) Trim(chars ...string) series.Series {
	cutset := " \t\n\r"
	if len(chars) > 0 {
		cutset = strings.Join(chars, "")
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.Trim(str, cutset)
	}, "trim")
}

// LTrim removes leading characters (default: whitespace)
func (so *StringOps) LTrim(chars ...string) series.Series {
	cutset := " \t\n\r"
	if len(chars) > 0 {
		cutset = strings.Join(chars, "")
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.TrimLeft(str, cutset)
	}, "ltrim")
}

// RTrim removes trailing characters (default: whitespace)
func (so *StringOps) RTrim(chars ...string) series.Series {
	cutset := " \t\n\r"
	if len(chars) > 0 {
		cutset = strings.Join(chars, "")
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.TrimRight(str, cutset)
	}, "rtrim")
}

// Strip removes leading and trailing whitespace
func (so *StringOps) Strip() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.TrimSpace(str)
	}, "strip")
}

// Pad pads the string to a specified width with a fill character
func (so *StringOps) Pad(width int, side string, fillchar string) series.Series {
	if len(fillchar) == 0 {
		fillchar = " "
	}
	// Use first rune of fillchar
	fillRune := []rune(fillchar)[0]
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		currentLen := len([]rune(str))
		if currentLen >= width {
			return str
		}
		
		padLen := width - currentLen
		padding := strings.Repeat(string(fillRune), padLen)
		
		switch side {
		case "left":
			return padding + str
		case "right":
			return str + padding
		case "center", "both":
			leftPad := padLen / 2
			rightPad := padLen - leftPad
			return strings.Repeat(string(fillRune), leftPad) + str + strings.Repeat(string(fillRune), rightPad)
		default:
			return str
		}
	}, "pad")
}

// LPad pads the string on the left to a specified width
func (so *StringOps) LPad(width int, fillchar string) series.Series {
	return so.Pad(width, "left", fillchar)
}

// RPad pads the string on the right to a specified width
func (so *StringOps) RPad(width int, fillchar string) series.Series {
	return so.Pad(width, "right", fillchar)
}

// CenterPad centers the string to a specified width
func (so *StringOps) CenterPad(width int, fillchar string) series.Series {
	return so.Pad(width, "center", fillchar)
}

// ZFill pads with zeros on the left to a specified width
func (so *StringOps) ZFill(width int) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		// Handle negative numbers
		if strings.HasPrefix(str, "-") || strings.HasPrefix(str, "+") {
			if len(str) >= width {
				return str
			}
			sign := str[0:1]
			number := str[1:]
			padLen := width - len(str)
			return sign + strings.Repeat("0", padLen) + number
		}
		
		// Regular padding with zeros
		if len(str) >= width {
			return str
		}
		padLen := width - len(str)
		return strings.Repeat("0", padLen) + str
	}, "zfill")
}

// TrimPrefix removes the specified prefix if present
func (so *StringOps) TrimPrefix(prefix string) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.TrimPrefix(str, prefix)
	}, "trim_prefix")
}

// TrimSuffix removes the specified suffix if present
func (so *StringOps) TrimSuffix(suffix string) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.TrimSuffix(str, suffix)
	}, "trim_suffix")
}