package frame

import (
	_ "fmt"
	_ "github.com/tnn1t1s/golars/series"
)

// Stack reshapes a DataFrame from wide to long format by stacking specified columns.
// It's similar to melt but preserves multi-level structure.
func (df *DataFrame) Stack(columns ...string) (*DataFrame, error) {
	panic("not implemented")

	// Get column indices

	// Validate columns exist

	// Get non-stack column indices

	// Create result

	// Repeat ID columns

	// Use the helper function from melt.go

	// Create level column (column names)

	// Determine common type from stack columns

	// Create value column data

}

// Unstack reshapes a DataFrame from long to wide format.
// It's the inverse of Stack operation.
func (df *DataFrame) Unstack(levelColumn string, fillValue interface{}) (*DataFrame, error) {
	panic(
		// This is essentially a pivot operation
		// Find the value column (last non-level column)
		"not implemented")

	// Get index columns (all except level and value)

	// Use pivot to unstack

}

// Transpose swaps rows and columns of a DataFrame.
// Column names become the index and index becomes column names.
func (df *DataFrame) Transpose() (*DataFrame, error) {
	panic("not implemented")

	// The original column names become the first column

	// Each row becomes a new column

	// Convert all values to string for simplicity

	// Use string type for now (could be improved with type detection)

}
