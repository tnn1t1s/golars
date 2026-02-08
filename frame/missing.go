package frame

import (
	_ "fmt"
	_ "sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// FillNullOptions configures null filling operations
type FillNullOptions struct {
	Value   interface{} // Value to fill nulls with
	Method  string      // Method: "forward", "backward", "value"
	Limit   int         // Maximum number of consecutive nulls to fill
	Columns []string    // Specific columns to fill (empty means all)
}

// FillNull fills null values in the DataFrame
func (df *DataFrame) FillNull(options FillNullOptions) (*DataFrame, error) {
	panic(
		// Default to value method if not specified
		"not implemented")

	// Get columns to process

	// Create result columns

	// Process each column

	// Check if this column should be filled

	// Fill nulls based on method

	// Keep column as is

}

// ForwardFill fills null values with the previous non-null value
func (df *DataFrame) ForwardFill(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// BackwardFill fills null values with the next non-null value
func (df *DataFrame) BackwardFill(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// Helper function to forward fill a series
func forwardFillSeries(s series.Series, limit int) series.Series {
	panic("not implemented")

	// Collect values

}

// Helper function to backward fill a series
func backwardFillSeries(s series.Series, limit int) series.Series {
	panic("not implemented")

	// Collect values

	// Process from end to start

}

// Helper function to fill with a specific value
func valueFillSeries(s series.Series, fillValue interface{}) series.Series {
	panic("not implemented")

	// Collect values

}

// Helper to create series from interface values
func createSeriesFromInterface(name string, values []interface{}, validity []bool, dataType datatypes.DataType) series.Series {
	panic(
		// If validity is nil, assume all values are valid
		"not implemented")

	// If any values are invalid, use NewSeriesWithValidity

	// If any values are invalid, use NewSeriesWithValidity

	// If any values are invalid, use NewSeriesWithValidity

	// If any values are invalid, use NewSeriesWithValidity

	// Convert to string

}

// DropNull removes rows with null values
func (df *DataFrame) DropNull(subset ...string) (*DataFrame, error) {
	panic(
		// Determine which columns to check
		"not implemented")

	// Find column indices

	// Find rows to keep

	// Create new columns with only kept rows

}

// DropDuplicates removes duplicate rows
type DropDuplicatesOptions struct {
	Subset []string // Columns to consider for duplicates (empty means all)
	Keep   string   // Which duplicate to keep: "first", "last", "none"
}

// DropDuplicates removes duplicate rows from the DataFrame
func (df *DataFrame) DropDuplicates(options DropDuplicatesOptions) (*DataFrame, error) {
	panic(
		// Default to keeping first
		"not implemented")

	// Validate keep option

	// Determine which columns to check

	// Find column indices

	// Track seen rows and which to keep
	// Maps row key to indices where it appears

	// Build key from subset columns

	// Determine which rows to keep based on keep option

	// Only keep if not duplicated

	// Sort keepRows to maintain order

	// Create new columns with only kept rows

}

// Imports needed at the top
// import "sort"
