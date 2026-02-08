package strings

import (
	"strings"
	"unicode"

	"github.com/tnn1t1s/golars/series"
)

// ToUpper converts all characters to uppercase
func (so *StringOps) ToUpper() series.Series {
	return applyUnaryStringOp(so.s, strings.ToUpper, "to_upper")
}

// ToLower converts all characters to lowercase
func (so *StringOps) ToLower() series.Series {
	return applyUnaryStringOp(so.s, strings.ToLower, "to_lower")
}

// ToTitle converts to title case (each word starts with uppercase)
func (so *StringOps) ToTitle() series.Series {
	return applyUnaryStringOp(so.s, strings.ToTitle, "to_title")
}

// Capitalize capitalizes the first character, lowercases the rest
func (so *StringOps) Capitalize() series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		if s == "" {
			return s
		}
		runes := []rune(s)
		runes[0] = unicode.ToUpper(runes[0])
		for i := 1; i < len(runes); i++ {
			runes[i] = unicode.ToLower(runes[i])
		}
		return string(runes)
	}, "capitalize")
}

// SwapCase swaps the case of each character
func (so *StringOps) SwapCase() series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		runes := []rune(s)
		for i, r := range runes {
			if unicode.IsUpper(r) {
				runes[i] = unicode.ToLower(r)
			} else if unicode.IsLower(r) {
				runes[i] = unicode.ToUpper(r)
			}
		}
		return string(runes)
	}, "swap_case")
}

// IsUpper checks if all characters are uppercase
func (so *StringOps) IsUpper() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		hasLetter := false
		for _, r := range s {
			if unicode.IsLetter(r) {
				hasLetter = true
				if !unicode.IsUpper(r) {
					return false
				}
			}
		}
		return hasLetter
	}, "is_upper")
}

// IsLower checks if all characters are lowercase
func (so *StringOps) IsLower() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		hasLetter := false
		for _, r := range s {
			if unicode.IsLetter(r) {
				hasLetter = true
				if !unicode.IsLower(r) {
					return false
				}
			}
		}
		return hasLetter
	}, "is_lower")
}

// IsTitle checks if the string is in title case
func (so *StringOps) IsTitle() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		return s == strings.ToTitle(s)
	}, "is_title")
}

// IsAlpha checks if all characters are alphabetic
func (so *StringOps) IsAlpha() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if !unicode.IsLetter(r) {
				return false
			}
		}
		return true
	}, "is_alpha")
}

// IsNumeric checks if all characters are numeric
func (so *StringOps) IsNumeric() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if !unicode.IsDigit(r) {
				return false
			}
		}
		return true
	}, "is_numeric")
}

// IsAlphaNumeric checks if all characters are alphanumeric
func (so *StringOps) IsAlphaNumeric() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				return false
			}
		}
		return true
	}, "is_alphanumeric")
}

// IsSpace checks if all characters are whitespace
func (so *StringOps) IsSpace() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if !unicode.IsSpace(r) {
				return false
			}
		}
		return true
	}, "is_space")
}
