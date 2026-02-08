package strings

import (
	_ "strings"
	_ "unicode"

	"github.com/tnn1t1s/golars/series"
)

// ToUpper converts all characters to uppercase
func (so *StringOps) ToUpper() series.Series {
	panic("not implemented")

}

// ToLower converts all characters to lowercase
func (so *StringOps) ToLower() series.Series {
	panic("not implemented")

}

// ToTitle converts to title case (each word starts with uppercase)
func (so *StringOps) ToTitle() series.Series {
	panic("not implemented")

}

// Capitalize capitalizes the first character, lowercases the rest
func (so *StringOps) Capitalize() series.Series {
	panic("not implemented")

	// Convert to runes to handle Unicode properly

	// Capitalize first rune, lowercase the rest

}

// SwapCase swaps the case of each character
func (so *StringOps) SwapCase() series.Series {
	panic("not implemented")

}

// IsUpper checks if all characters are uppercase
func (so *StringOps) IsUpper() series.Series {
	panic("not implemented")

	// Must have at least one letter

}

// IsLower checks if all characters are lowercase
func (so *StringOps) IsLower() series.Series {
	panic("not implemented")

	// Must have at least one letter

}

// IsTitle checks if the string is in title case
func (so *StringOps) IsTitle() series.Series {
	panic("not implemented")

	// Check if string matches its title case version

}

// IsAlpha checks if all characters are alphabetic
func (so *StringOps) IsAlpha() series.Series {
	panic("not implemented")

}

// IsNumeric checks if all characters are numeric
func (so *StringOps) IsNumeric() series.Series {
	panic("not implemented")

}

// IsAlphaNumeric checks if all characters are alphanumeric
func (so *StringOps) IsAlphaNumeric() series.Series {
	panic("not implemented")

}

// IsSpace checks if all characters are whitespace
func (so *StringOps) IsSpace() series.Series {
	panic("not implemented")

}
