package frame

import (
	_ "fmt"
	_ "math"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// CumulativeOptions configures cumulative operations
type CumulativeOptions struct {
	Axis      int      // 0 for index (rows), 1 for columns
	SkipNulls bool     // Whether to skip null values
	Columns   []string // Specific columns to apply operation to
}

// CumSum calculates cumulative sum for numeric columns
func (df *DataFrame) CumSum(options CumulativeOptions) (*DataFrame, error) {
	panic("not implemented")

}

// CumProd calculates cumulative product for numeric columns
func (df *DataFrame) CumProd(options CumulativeOptions) (*DataFrame, error) {
	panic("not implemented")

}

// CumMax calculates cumulative maximum for numeric columns
func (df *DataFrame) CumMax(options CumulativeOptions) (*DataFrame, error) {
	panic("not implemented")

}

// CumMin calculates cumulative minimum for numeric columns
func (df *DataFrame) CumMin(options CumulativeOptions) (*DataFrame, error) {
	panic("not implemented")

}

// CumCount calculates cumulative count of non-null values
func (df *DataFrame) CumCount(options CumulativeOptions) (*DataFrame, error) {
	panic("not implemented")

}

// Generic function to apply cumulative operations
func (df *DataFrame) applyCumulative(options CumulativeOptions, fn func(series.Series, bool) series.Series) (*DataFrame, error) {
	panic("not implemented")

	// Get columns to process

	// Default to all numeric columns for most operations

	// Create result columns

	// Check if this column should be processed

	// Apply cumulative function

	// Keep column as is

}

// Helper function for cumulative sum
func cumSumSeries(s series.Series, skipNulls bool) series.Series {
	panic("not implemented")

	// Keep current sum

	// Propagate null

	// Convert back to original type if needed

}

// Helper function for cumulative product
func cumProdSeries(s series.Series, skipNulls bool) series.Series {
	panic("not implemented")

	// Keep current product

	// Propagate null

}

// Helper function for cumulative maximum
func cumMaxSeries(s series.Series, skipNulls bool) series.Series {
	panic("not implemented")

	// Keep current max

	// Propagate null

}

// Helper function for cumulative minimum
func cumMinSeries(s series.Series, skipNulls bool) series.Series {
	panic("not implemented")

	// Keep current min

	// Propagate null

}

// Helper function for cumulative count
func cumCountSeries(s series.Series, skipNulls bool) series.Series {
	panic("not implemented")

	// Count is always valid

}

// Helper to convert float64 array back to original series type
func convertToOriginalSeriesType(name string, values []float64, validity []bool, dataType datatypes.DataType) series.Series {
	panic("not implemented")

	// Default to float64

}
