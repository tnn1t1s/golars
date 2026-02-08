package golars

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// SeriesFrom creates a new series with automatic type inference
// The name parameter can be:
// - A string (series name) followed by values
// - Omitted (anonymous series with just values)
//
// The values parameter can be:
// - A slice of any type
// - []interface{} with mixed types (will infer common type)
func SeriesFrom(args ...interface{}) (Series, error) {
	panic("not implemented")

	// Parse arguments

	// Series(values) - anonymous series

	// Series(name, values) or Series(values, dtype)

	// Series(name, values)

	// Series(values, dtype)

	// Series(name, values, dtype)

	// Convert values to []interface{} if needed

	// If conversion fails, try to handle specific types directly

	// If dtype is specified, use it; otherwise infer

	// Check if we have nulls

	// Use type inference

}

// createSeriesFromTypedSlice handles strongly-typed slices directly
func createSeriesFromTypedSlice(name string, values interface{}, dtype datatypes.DataType) (Series, error) {
	panic("not implemented")

	// Convert []int to appropriate integer type

	// Default for int

}

// createIntSeriesFromInt converts []int to the appropriate integer series type
func createIntSeriesFromInt(name string, values []int, dtype datatypes.DataType) (Series, error) {
	panic("not implemented")

}

// createSeriesFromConvertedValues creates a series from pre-converted values
func createSeriesFromConvertedValues(name string, typedValues interface{}, validity []bool, dtype datatypes.DataType, hasNulls bool) (Series, error) {
	panic("not implemented")

}

// isCompatibleType checks if two data types are compatible
func isCompatibleType(a, b datatypes.DataType) bool {
	panic(
		// For now, require exact type match
		// Could be extended to allow compatible conversions
		"not implemented")

}
