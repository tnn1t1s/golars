package strings

import (
	_ "strings"

	"github.com/tnn1t1s/golars/series"
)

// Trim removes leading and trailing characters (default: whitespace)
func (so *StringOps) Trim(chars ...string) series.Series {
	panic("not implemented")

}

// LTrim removes leading characters (default: whitespace)
func (so *StringOps) LTrim(chars ...string) series.Series {
	panic("not implemented")

}

// RTrim removes trailing characters (default: whitespace)
func (so *StringOps) RTrim(chars ...string) series.Series {
	panic("not implemented")

}

// Strip removes leading and trailing whitespace
func (so *StringOps) Strip() series.Series {
	panic("not implemented")

}

// Pad pads the string to a specified width with a fill character
func (so *StringOps) Pad(width int, side string, fillchar string) series.Series {
	panic("not implemented")

	// Use first rune of fillchar

}

// LPad pads the string on the left to a specified width
func (so *StringOps) LPad(width int, fillchar string) series.Series {
	panic("not implemented")

}

// RPad pads the string on the right to a specified width
func (so *StringOps) RPad(width int, fillchar string) series.Series {
	panic("not implemented")

}

// CenterPad centers the string to a specified width
func (so *StringOps) CenterPad(width int, fillchar string) series.Series {
	panic("not implemented")

}

// ZFill pads with zeros on the left to a specified width
func (so *StringOps) ZFill(width int) series.Series {
	panic("not implemented")

	// Handle negative numbers

	// Regular padding with zeros

}

// TrimPrefix removes the specified prefix if present
func (so *StringOps) TrimPrefix(prefix string) series.Series {
	panic("not implemented")

}

// TrimSuffix removes the specified suffix if present
func (so *StringOps) TrimSuffix(suffix string) series.Series {
	panic("not implemented")

}
