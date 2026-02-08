package strings

import (
	_ "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/tnn1t1s/golars/series"
	_ "strings"
)

// Contains checks if each string contains the pattern
func (so *StringOps) Contains(pattern string, literal bool) series.Series {
	panic("not implemented")

	// For non-literal, pattern is treated as substring search

}

// StartsWith checks if each string starts with the pattern
func (so *StringOps) StartsWith(pattern string) series.Series {
	panic("not implemented")

}

// EndsWith checks if each string ends with the pattern
func (so *StringOps) EndsWith(pattern string) series.Series {
	panic("not implemented")

}

// Find returns the index of the first occurrence of pattern, or -1 if not found
func (so *StringOps) Find(pattern string) series.Series {
	panic("not implemented")

}

// Count returns the number of non-overlapping occurrences of pattern
func (so *StringOps) Count(pattern string) series.Series {
	panic("not implemented")

	// Empty pattern counts positions between runes plus start and end

}

// Split splits each string by the separator and returns a list series
func (so *StringOps) Split(separator string) series.Series {
	panic("not implemented")

	// Build list of string arrays

	// For now, return a string series with the first element of each split
	// TODO: Implement proper List series type

}

// SplitN splits each string into at most n parts
func (so *StringOps) SplitN(separator string, n int) series.Series {
	panic("not implemented")

	// Build list of string arrays

	// For now, return a string series with the first element of each split
	// TODO: Implement proper List series type

}

// JoinList concatenates elements with a separator (for future list series support)
func (so *StringOps) JoinList(separator string) series.Series {
	panic(
		// For now, this just returns the original series
		// TODO: Implement when List series type is available
		"not implemented")

}

// Helper function to apply pattern operations that return boolean results
func applyPatternOp(s series.Series, op func(string) bool, name string) series.Series {
	panic("not implemented")

	// Use the helper function for boolean results

}

// Helper to create boolean series with null handling
func createBooleanSeries(name string, values []bool, boolArr *array.Boolean) series.Series {
	panic(
		// Create a mask for null values
		"not implemented")

	// Use NewBooleanSeries which should handle the values and mask appropriately

}
