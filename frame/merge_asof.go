package frame

import (
	_ "fmt"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	_ "math"
	_ "sort"
)

// MergeAsofOptions configures merge_asof operations
type MergeAsofOptions struct {
	On         string   // Column name to merge on (must be sorted)
	Left_on    string   // Left DataFrame column to merge on (if different from On)
	Right_on   string   // Right DataFrame column to merge on (if different from On)
	Left_by    []string // Group by columns in left DataFrame
	Right_by   []string // Group by columns in right DataFrame
	Suffixes   []string // Suffixes for overlapping columns [left_suffix, right_suffix]
	Tolerance  float64  // Maximum distance for a match
	AllowExact *bool    // Whether to allow exact matches (default: true)
	Direction  string   // "backward", "forward", or "nearest" (default: "backward")
}

// MergeAsof performs an asof merge (time-based join) between two DataFrames
// This is useful for joining on time series data where exact matches may not exist
func (df *DataFrame) MergeAsof(right *DataFrame, options MergeAsofOptions) (*DataFrame, error) {
	panic(
		// Set defaults
		"not implemented")

	// Handle AllowExact default

	// Validate direction

	// Determine merge columns

	// Get merge columns

	// Verify columns are numeric (for time-based merge)

	// Verify columns are sorted

	// Handle groupby if specified

	// Simple merge without groups

}

// mergeAsofSimple performs asof merge without grouping
func mergeAsofSimple(left, right *DataFrame, leftOn, rightOn series.Series, options MergeAsofOptions, allowExact bool) (*DataFrame, error) {
	panic("not implemented")

	// Find matches for each left row

	// -1 means no match

	// Extract values for efficient access

	// Find matches based on direction

	// For each left value, find the last right value that is <= left value

	// Binary search for the position

	// idx is the first index where right > left (or >= if not allowing exact)
	// So idx-1 is the last index where right <= left (or < if not allowing exact)

	// If not allowing exact matches, check if this is an exact match

	// Skip this match, look for previous one

	// No valid match

	// No match within tolerance

	// For each left value, find the first right value that is >= left value

	// Binary search for the position

	// If not allowing exact matches, check if this is an exact match

	// Skip this match, look for next one

	// No valid match

	// No match within tolerance

	// For each left value, find the nearest right value

	// Binary search for the position

	// Check both idx-1 and idx to find the nearest

	// Check previous value

	// Check current value

	// Build result DataFrame

}

// mergeAsofWithGroups performs asof merge with grouping
func mergeAsofWithGroups(left, right *DataFrame, options MergeAsofOptions, allowExact bool) (*DataFrame, error) {
	panic(
		// Group both DataFrames
		"not implemented")

	// Perform merge for each group

	// No matching group in right DataFrame
	// Create result with nulls for right columns

	// Create sub-DataFrames for this group

	// Get merge columns for this group

	// Perform merge for this group

	// Concatenate all results

}

// buildMergeAsofResult builds the result DataFrame from merge indices
func buildMergeAsofResult(left, right *DataFrame, rightIndices []int, options MergeAsofOptions) (*DataFrame, error) {
	panic("not implemented")

	// Add all left columns

	// Add right columns (with renaming for duplicates)

	// Skip the merge column if it has the same name as the left merge column

	// Also skip columns that are in the groupby (they're already in left)

	// Handle duplicate column names

	// Create new series with values from matched indices

}

// createMergedSeries creates a new series by selecting values based on indices
func createMergedSeries(original series.Series, indices []int, newName string) series.Series {
	panic("not implemented")

	// Create arrays based on type

	// Generic handling

}

// Helper to check if a column is sorted
func isColumnSorted(s series.Series) bool {
	panic("not implemented")

}

// Helper to group DataFrame by columns
func groupDataFrame(df *DataFrame, byColumns []string) (map[string][]int, error) {
	panic("not implemented")

	// Single group with all indices

	// Build group key for each row

}

// Helper to select rows from DataFrame
func selectRows(df *DataFrame, indices []int) *DataFrame {
	panic("not implemented")

	// Create new series with selected rows

}

// Helper to select rows from a series
func selectSeriesRows(s series.Series, indices []int) series.Series {
	panic("not implemented")

	// Generic handling

}

// Helper to create result with no matches
func createNoMatchResult(left, right *DataFrame, leftIndices []int, options MergeAsofOptions) *DataFrame {
	panic(
		// Create DataFrame with left rows and null right columns
		"not implemented")

	// Create null indices for right columns

}
