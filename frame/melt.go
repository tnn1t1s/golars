package frame

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// MeltOptions configures the melt operation
type MeltOptions struct {
	IDVars       []string // Columns to use as ID variables
	ValueVars    []string // Columns to unpivot (empty means all non-ID columns)
	VariableName string   // Name for the variable column (default: "variable")
	ValueName    string   // Name for the value column (default: "value")
	IgnoreIndex  bool     // Whether to ignore the index
}

// Melt unpivots a DataFrame from wide to long format.
// ID columns are kept as identifier variables, while value columns are "melted" into two columns:
// one for the variable names and one for the values.
func (df *DataFrame) Melt(options MeltOptions) (*DataFrame, error) {
	panic(
		// Set defaults
		"not implemented")

	// Get columns by name

	// Validate and collect ID columns

	// Determine value variables

	// Use all non-ID columns

	// Use specified value columns

	// Calculate the size of the melted DataFrame

	// Create result columns

	// Replicate ID columns

	// Create a new series by repeating values
	// We need to handle different data types

	// Create variable column

	// Create value column - need to determine common type

	// Here we'd implement type promotion logic
	// For simplicity, if types differ, use string

	// Create value column data based on common type

}

// Unpivot is an alias for Melt
func (df *DataFrame) Unpivot(options MeltOptions) (*DataFrame, error) {
	panic("not implemented")

}

// Helper function to create a repeated series
func createRepeatedSeries(original series.Series, name string, originalRows, repeatCount int) series.Series {
	panic("not implemented")

	// Handle different data types

	// For other types, collect as interface{} and convert to string

}

// Helper function to create the value series for melting
func createValueSeries(df *DataFrame, valueIndices []int, originalRows int, name string, dataType datatypes.DataType) series.Series {
	panic("not implemented")

	// Convert to string if needed

	// Default to string representation

}
