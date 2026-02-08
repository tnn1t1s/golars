package strings

import (
	_ "fmt"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	_ "math"
	_ "strconv"
	_ "strings"
	_ "time"
)

// ToInteger parses strings to integers with optional base
func (so *StringOps) ToInteger(base ...int) (series.Series, error) {
	panic("not implemented")

	// Count valid values first to determine output type

	// Choose appropriate integer type

	// Parse and convert

	// Convert to appropriate type

	// Create appropriate slice based on type

}

// ToFloat parses strings to floating point numbers
func (so *StringOps) ToFloat() (series.Series, error) {
	panic("not implemented")

	// Handle special cases

	// +Inf

	// -Inf

	// NaN

}

// ToBoolean parses strings to boolean values
func (so *StringOps) ToBoolean() (series.Series, error) {
	panic("not implemented")

}

// ToDateTime parses strings to DateTime with optional format
func (so *StringOps) ToDateTime(format ...string) (series.Series, error) {
	panic(
		// Import datetime package functions
		"not implemented")

	// Default formats to try

	// Convert Python/Polars format to Go format if needed

}

// ToDate parses strings to Date
func (so *StringOps) ToDate(format ...string) (series.Series, error) {
	panic(
		// Use ToDateTime and extract date part
		"not implemented")

	// Convert timestamps to days since epoch

}

// ToTime parses strings to Time
func (so *StringOps) ToTime(format ...string) (series.Series, error) {
	panic("not implemented")

	// Default time formats

	// Extract time as nanoseconds since midnight

}

// Helper function to convert Python/Polars format strings to Go format
func convertPythonFormatToGo(format string) string {
	panic("not implemented")

}

// IsNumericStr checks if strings can be parsed as numbers
func (so *StringOps) IsNumericStr() series.Series {
	panic("not implemented")

	// Try parsing as float (covers integers too)

}

// IsAlphaStr checks if strings contain only alphabetic characters
func (so *StringOps) IsAlphaStr() series.Series {
	panic("not implemented")

}

// IsAlphanumericStr checks if strings contain only alphanumeric characters
func (so *StringOps) IsAlphanumericStr() series.Series {
	panic("not implemented")

}
