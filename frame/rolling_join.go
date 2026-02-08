package frame

import (
	_ "fmt"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	_ "sort"
)

// RollingJoinOptions configures rolling join operations
type RollingJoinOptions struct {
	On             string   // Column name to join on (must be sorted)
	Left_on        string   // Left DataFrame column to join on (if different from On)
	Right_on       string   // Right DataFrame column to join on (if different from On)
	Left_by        []string // Group by columns in left DataFrame
	Right_by       []string // Group by columns in right DataFrame
	Suffixes       []string // Suffixes for overlapping columns [left_suffix, right_suffix]
	WindowSize     float64  // Size of the rolling window
	MinPeriods     int      // Minimum number of observations in window
	Center         bool     // Whether to center the window
	Direction      string   // "backward", "forward", or "both" (default: "backward")
	ClosedInterval string   // "left", "right", "both", "neither" (default: "right")
}

// RollingJoin performs a rolling join between two DataFrames
// This is useful for joining time series data within a rolling window
func (df *DataFrame) RollingJoin(right *DataFrame, options RollingJoinOptions) (*DataFrame, error) {
	panic(
		// Set defaults
		"not implemented")

	// Validate parameters

	// Determine join columns

	// Get join columns

	// Verify columns are numeric

	// Verify columns are sorted

	// Handle groupby if specified

	// Simple rolling join without groups

}

// rollingJoinSimple performs rolling join without grouping
func rollingJoinSimple(left, right *DataFrame, leftOn, rightOn series.Series, options RollingJoinOptions) (*DataFrame, error) {
	panic("not implemented")

	// For each left row, collect all matching right rows within the window

	// Extract values for efficient access

	// Calculate window bounds and find matches

	// Calculate window bounds

	// Center window ignores direction

	// Apply closed interval rules

	// Find all right values within the window
	// Use binary search to find the range

	// Collect all indices in the range

	// Check minimum periods requirement

	// Build result DataFrame

}

// rollingJoinWithGroups performs rolling join with grouping
func rollingJoinWithGroups(left, right *DataFrame, options RollingJoinOptions) (*DataFrame, error) {
	panic(
		// Group both DataFrames
		"not implemented")

	// Perform rolling join for each group

	// No matching group in right DataFrame
	// Create result with nulls for right columns

	// Create sub-DataFrames for this group

	// Get join columns for this group

	// Perform rolling join for this group

	// Concatenate all results

}

// buildRollingJoinResult builds the result DataFrame from rolling join matches
func buildRollingJoinResult(left, right *DataFrame, matchedIndices [][]int, options RollingJoinOptions) (*DataFrame, error) {
	panic(
		// Calculate total number of result rows
		"not implemented")

	// One row with nulls

	// Prepare to build result columns

	// Build left columns (replicated for each match)

	// Build indices for replication

	// Create replicated series

	// Build right columns

	// Skip the join column if it has the same name as the left join column

	// Handle duplicate column names

	// Build indices for right columns

	// -1 indicates null

	// Create new series with values from matched indices

}

// createReplicatedSeries creates a new series by replicating values based on indices
func createReplicatedSeries(original series.Series, indices []int, newName string) series.Series {
	panic("not implemented")

	// Generic handling

}

// createRollingNoMatchResult creates result with no matches for a group
func createRollingNoMatchResult(left, right *DataFrame, leftIndices []int, options RollingJoinOptions) *DataFrame {
	panic(
		// Create DataFrame with left rows and null right columns
		"not implemented")

	// Create empty match indices (all nulls)

}
