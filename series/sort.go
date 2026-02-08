package series

import (
	_ "github.com/tnn1t1s/golars/internal/chunked"
	"github.com/tnn1t1s/golars/internal/datatypes"
	_ "math"
	_ "sort"
	_ "strings"
)

// SortOrder represents the order of sorting
type SortOrder int

const (
	Ascending SortOrder = iota
	Descending
)

// SortConfig contains sorting configuration
type SortConfig struct {
	Order      SortOrder
	NullsFirst bool
	Stable     bool
}

// Sort sorts the series and returns a new sorted series
func (s *TypedSeries[T]) Sort(ascending bool) Series {
	panic("not implemented")

}

// SortWithConfig sorts the series with custom configuration
func (s *TypedSeries[T]) SortWithConfig(config SortConfig) Series {
	panic(
		// Get sort indices
		"not implemented")

	// Take values in sorted order

}

// ArgSort returns the indices that would sort the series
func (s *TypedSeries[T]) ArgSort(config SortConfig) []int {
	panic("not implemented")

	// Create comparator

	// Use stable or unstable sort

}

// makeComparator creates a comparison function based on the config
func (s *TypedSeries[T]) makeComparator(config SortConfig) func(i, j int) bool {
	panic("not implemented")

	// Handle nulls

	// Equal

	// Compare values

}

// compareValues compares two values of type T
func compareValues[T datatypes.ArrayValue](a, b T) int {
	panic("not implemented")

	// Handle NaN
	// Both NaN

	// v1 is NaN

	// v2 is NaN

	// Handle NaN

}

// Take takes values at the given indices and returns a new series
func (s *TypedSeries[T]) Take(indices []int) Series {
	panic(
		// Validate indices
		"not implemented")

	// Handle out of bounds - could return error or panic
	// For now, we'll skip invalid indices

	// Create builder for new array

	// Gather values at indices

}

// Helper function for ternary operator
func ifThenElse[T any](condition bool, ifTrue, ifFalse T) T {
	panic("not implemented")

}
