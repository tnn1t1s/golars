package strings

import (
	"strings"
	"unicode"

	"github.com/davidpalaitis/golars/series"
)

// ToUpper converts all characters to uppercase
func (so *StringOps) ToUpper() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.ToUpper(str)
	}, "to_upper")
}

// ToLower converts all characters to lowercase
func (so *StringOps) ToLower() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.ToLower(str)
	}, "to_lower")
}

// ToTitle converts to title case (each word starts with uppercase)
func (so *StringOps) ToTitle() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.Title(str)
	}, "to_title")
}

// Capitalize capitalizes the first character, lowercases the rest
func (so *StringOps) Capitalize() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		if len(str) == 0 {
			return str
		}
		
		// Convert to runes to handle Unicode properly
		runes := []rune(str)
		
		// Capitalize first rune, lowercase the rest
		for i, r := range runes {
			if i == 0 {
				runes[i] = unicode.ToUpper(r)
			} else {
				runes[i] = unicode.ToLower(r)
			}
		}
		
		return string(runes)
	}, "capitalize")
}

// SwapCase swaps the case of each character
func (so *StringOps) SwapCase() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		runes := []rune(str)
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
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		for _, r := range str {
			if unicode.IsLetter(r) && !unicode.IsUpper(r) {
				return false
			}
		}
		// Must have at least one letter
		for _, r := range str {
			if unicode.IsLetter(r) {
				return true
			}
		}
		return false
	}, "is_upper")
}

// IsLower checks if all characters are lowercase
func (so *StringOps) IsLower() series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		for _, r := range str {
			if unicode.IsLetter(r) && !unicode.IsLower(r) {
				return false
			}
		}
		// Must have at least one letter
		for _, r := range str {
			if unicode.IsLetter(r) {
				return true
			}
		}
		return false
	}, "is_lower")
}

// IsTitle checks if the string is in title case
func (so *StringOps) IsTitle() series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		
		// Check if string matches its title case version
		return str == strings.Title(str)
	}, "is_title")
}

// IsAlpha checks if all characters are alphabetic
func (so *StringOps) IsAlpha() series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		for _, r := range str {
			if !unicode.IsLetter(r) {
				return false
			}
		}
		return true
	}, "is_alpha")
}

// IsNumeric checks if all characters are numeric
func (so *StringOps) IsNumeric() series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		for _, r := range str {
			if !unicode.IsDigit(r) {
				return false
			}
		}
		return true
	}, "is_numeric")
}

// IsAlphaNumeric checks if all characters are alphanumeric
func (so *StringOps) IsAlphaNumeric() series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		for _, r := range str {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				return false
			}
		}
		return true
	}, "is_alphanumeric")
}

// IsSpace checks if all characters are whitespace
func (so *StringOps) IsSpace() series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if len(str) == 0 {
			return false
		}
		for _, r := range str {
			if !unicode.IsSpace(r) {
				return false
			}
		}
		return true
	}, "is_space")
}