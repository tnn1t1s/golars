package strings

import (
	_ "github.com/apache/arrow-go/v18/arrow"
	_ "github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	_ "strings"
	_ "unicode/utf8"
)

// StringOps provides string manipulation operations on a Series
type StringOps struct {
	s series.Series
}

// NewStringOps creates a new StringOps instance
func NewStringOps(s series.Series) *StringOps {
	panic("not implemented")

}

// Length returns the length of each string in the series
func (so *StringOps) Length() series.Series {
	panic("not implemented")

}

// RuneLength returns the number of UTF-8 runes in each string
func (so *StringOps) RuneLength() series.Series {
	panic("not implemented")

}

// Concat concatenates two string series element-wise
func (so *StringOps) Concat(other series.Series) series.Series {
	panic("not implemented")

}

// Repeat repeats each string n times
func (so *StringOps) Repeat(n int) series.Series {
	panic("not implemented")

}

// Slice extracts a substring from each string
func (so *StringOps) Slice(start, length int) series.Series {
	panic("not implemented")

	// Convert to runes for proper Unicode handling

	// Create local copy of start to avoid modifying the parameter

	// Handle negative start

	// Calculate end position

}

// Left returns the first n characters of each string
func (so *StringOps) Left(n int) series.Series {
	panic("not implemented")

}

// Right returns the last n characters of each string
func (so *StringOps) Right(n int) series.Series {
	panic("not implemented")

}

// Replace replaces occurrences of a pattern with a replacement string
// n < 0 means replace all occurrences
func (so *StringOps) Replace(pattern, replacement string, n int) series.Series {
	panic("not implemented")

}

// Reverse reverses each string
func (so *StringOps) Reverse() series.Series {
	panic("not implemented")

}

// Helper function to apply unary string operations
func applyUnaryOp(s series.Series, op func(string) interface{}, name string) series.Series {
	panic("not implemented")

	// Determine output type based on first non-null result

	// Determine output type

	// Build appropriate array based on output type

	// String output

}

// Helper function to apply binary string operations
func applyBinaryOp(s1, s2 series.Series, op func(string, string) interface{}, name string) series.Series {
	panic("not implemented")

}

// Helper function to apply unary operations that may return errors
func applyUnaryOpWithError(s series.Series, op func(string) (interface{}, error), name string) (series.Series, error) {
	panic("not implemented")

	// First pass: determine output type

	// Default to string if no valid sample

	// Build result based on output type

	// Return as binary data (not implemented in series yet, use string)

	// String output

}

// applyUnaryBoolOp applies a unary operation that returns a boolean to each string in the series
func applyUnaryBoolOp(s series.Series, op func(string) bool, name string) series.Series {
	panic("not implemented")

}

// applyUnaryInt64Op applies a unary operation that returns an int64 to each string in the series
func applyUnaryInt64Op(s series.Series, op func(string) int64, name string) series.Series {
	panic("not implemented")

}
