package strings

import (
	"strings"
	"unicode/utf8"

	"github.com/tnn1t1s/golars/series"
)

// Trim removes leading and trailing characters (default: whitespace)
func (so *StringOps) Trim(chars ...string) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		if len(chars) > 0 && chars[0] != "" {
			return strings.Trim(s, chars[0])
		}
		return strings.TrimSpace(s)
	}, "trim")
}

// LTrim removes leading characters (default: whitespace)
func (so *StringOps) LTrim(chars ...string) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		if len(chars) > 0 && chars[0] != "" {
			return strings.TrimLeft(s, chars[0])
		}
		return strings.TrimLeftFunc(s, func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\n' || r == '\r'
		})
	}, "ltrim")
}

// RTrim removes trailing characters (default: whitespace)
func (so *StringOps) RTrim(chars ...string) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		if len(chars) > 0 && chars[0] != "" {
			return strings.TrimRight(s, chars[0])
		}
		return strings.TrimRightFunc(s, func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\n' || r == '\r'
		})
	}, "rtrim")
}

// Strip removes leading and trailing whitespace
func (so *StringOps) Strip() series.Series {
	return applyUnaryStringOp(so.s, strings.TrimSpace, "strip")
}

// Pad pads the string to a specified width with a fill character
func (so *StringOps) Pad(width int, side string, fillchar string) series.Series {
	fc := ' '
	if fillchar != "" {
		fc, _ = utf8.DecodeRuneInString(fillchar)
	}
	return applyUnaryStringOp(so.s, func(s string) string {
		runeLen := utf8.RuneCountInString(s)
		if runeLen >= width {
			return s
		}
		pad := width - runeLen
		fill := strings.Repeat(string(fc), pad)
		switch side {
		case "left":
			return fill + s
		case "right":
			return s + fill
		case "both":
			left := pad / 2
			right := pad - left
			return strings.Repeat(string(fc), left) + s + strings.Repeat(string(fc), right)
		default:
			return s
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
	return so.Pad(width, "both", fillchar)
}

// ZFill pads with zeros on the left to a specified width
func (so *StringOps) ZFill(width int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		runeLen := utf8.RuneCountInString(s)
		if runeLen >= width {
			return s
		}
		// Handle sign characters
		if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
			return string(s[0]) + strings.Repeat("0", width-runeLen) + s[1:]
		}
		return strings.Repeat("0", width-runeLen) + s
	}, "zfill")
}

// TrimPrefix removes the specified prefix if present
func (so *StringOps) TrimPrefix(prefix string) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		return strings.TrimPrefix(s, prefix)
	}, "trim_prefix")
}

// TrimSuffix removes the specified suffix if present
func (so *StringOps) TrimSuffix(suffix string) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		return strings.TrimSuffix(s, suffix)
	}, "trim_suffix")
}
