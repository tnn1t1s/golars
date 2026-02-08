package frame

import (
	_ "fmt"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/apache/arrow-go/v18/arrow/array"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/tnn1t1s/golars/series"
)

// SortOptions contains options for sorting a DataFrame
type SortOptions struct {
	Columns    []string
	Orders     []series.SortOrder
	NullsFirst bool
	Stable     bool
}

// Sort sorts the DataFrame by the specified columns in ascending order
func (df *DataFrame) Sort(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// SortDesc sorts the DataFrame by the specified columns in descending order
func (df *DataFrame) SortDesc(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// SortBy sorts the DataFrame with custom options
func (df *DataFrame) SortBy(options SortOptions) (*DataFrame, error) {
	panic("not implemented")

	// Validate columns and get series

	// Ensure orders array matches columns

	// Extend with ascending order

}

func (df *DataFrame) arrowSortIndices(sortSeries []series.Series, options SortOptions) ([]int, error) {
	panic("not implemented")

}

func toArrowSortOrder(order series.SortOrder) arrowcompute.SortOrder {
	panic("not implemented")

}

func arrowSortIndexArrayToInts(arr arrow.Array) ([]int, error) {
	panic("not implemented")

}

// compareSeriesValues compares two values from a series
func compareSeriesValues(s series.Series, i, j int, order series.SortOrder, nullsFirst bool) int {
	panic(
		// Handle nulls
		"not implemented")

	// Equal

	// Get values

	// Compare based on type

	// Handle NaN
	// Both NaN

	// v1 is NaN

	// v2 is NaN

	// Handle NaN
	// Both NaN

	// v1 is NaN

	// v2 is NaN

	// Apply order

}

// Take creates a new DataFrame with rows at the specified indices
func (df *DataFrame) Take(indices []int) (*DataFrame, error) {
	panic("not implemented")

	// Validate indices

	// Create new columns with gathered values

}
