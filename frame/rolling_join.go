package frame

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/tnn1t1s/golars/series"
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
func (df *DataFrame) RollingJoin(right *DataFrame, options RollingJoinOptions) (*DataFrame, error) {
	// Set defaults
	if options.Direction == "" {
		options.Direction = "backward"
	}
	if options.ClosedInterval == "" {
		options.ClosedInterval = "right"
	}
	if len(options.Suffixes) == 0 {
		options.Suffixes = []string{"", "_right"}
	}
	if options.WindowSize <= 0 {
		return nil, fmt.Errorf("window size must be positive")
	}

	// Validate parameters
	switch options.Direction {
	case "backward", "forward", "both":
	default:
		return nil, fmt.Errorf("direction must be one of: backward, forward, both; got %q", options.Direction)
	}
	switch options.ClosedInterval {
	case "left", "right", "both", "neither":
	default:
		return nil, fmt.Errorf("closed_interval must be one of: left, right, both, neither; got %q", options.ClosedInterval)
	}

	// Determine join columns
	leftOnCol := options.On
	rightOnCol := options.On
	if options.Left_on != "" {
		leftOnCol = options.Left_on
	}
	if options.Right_on != "" {
		rightOnCol = options.Right_on
	}
	if leftOnCol == "" || rightOnCol == "" {
		return nil, fmt.Errorf("join column must be specified via On or Left_on/Right_on")
	}

	// Get join columns
	leftOn, err := df.Column(leftOnCol)
	if err != nil {
		return nil, fmt.Errorf("left join column: %w", err)
	}
	rightOn, err := right.Column(rightOnCol)
	if err != nil {
		return nil, fmt.Errorf("right join column: %w", err)
	}

	// Verify columns are sorted
	if !isColumnSorted(leftOn) {
		return nil, fmt.Errorf("left join column %q must be sorted", leftOnCol)
	}
	if !isColumnSorted(rightOn) {
		return nil, fmt.Errorf("right join column %q must be sorted", rightOnCol)
	}

	// Handle groupby if specified
	if len(options.Left_by) > 0 || len(options.Right_by) > 0 {
		return rollingJoinWithGroups(df, right, options)
	}

	// Simple rolling join without groups
	return rollingJoinSimple(df, right, leftOn, rightOn, options)
}

// rollingJoinSimple performs rolling join without grouping
func rollingJoinSimple(left, right *DataFrame, leftOn, rightOn series.Series, options RollingJoinOptions) (*DataFrame, error) {
	leftLen := left.height
	rightLen := right.height

	// Extract values for efficient access
	leftVals := make([]float64, leftLen)
	for i := 0; i < leftLen; i++ {
		if leftOn.IsNull(i) {
			leftVals[i] = math.NaN()
		} else {
			leftVals[i] = toFloat64Value(leftOn.Get(i))
		}
	}

	rightVals := make([]float64, rightLen)
	for i := 0; i < rightLen; i++ {
		if rightOn.IsNull(i) {
			rightVals[i] = math.NaN()
		} else {
			rightVals[i] = toFloat64Value(rightOn.Get(i))
		}
	}

	// For each left row, collect all matching right rows within the window
	matchedIndices := make([][]int, leftLen)

	for i := 0; i < leftLen; i++ {
		if math.IsNaN(leftVals[i]) {
			continue
		}
		lv := leftVals[i]

		// Calculate window bounds
		var lower, upper float64
		if options.Center {
			half := options.WindowSize / 2.0
			lower = lv - half
			upper = lv + half
		} else {
			switch options.Direction {
			case "backward":
				lower = lv - options.WindowSize
				upper = lv
			case "forward":
				lower = lv
				upper = lv + options.WindowSize
			case "both":
				lower = lv - options.WindowSize
				upper = lv + options.WindowSize
			}
		}

		// Find all right values within the window using binary search
		startIdx := sort.Search(rightLen, func(j int) bool {
			return rightVals[j] >= lower
		})

		var matches []int
		for j := startIdx; j < rightLen; j++ {
			rv := rightVals[j]
			if rv > upper {
				break
			}

			// Apply closed interval rules
			inWindow := false
			switch options.ClosedInterval {
			case "both":
				inWindow = rv >= lower && rv <= upper
			case "left":
				inWindow = rv >= lower && rv < upper
			case "right":
				inWindow = rv > lower && rv <= upper
			case "neither":
				inWindow = rv > lower && rv < upper
			}

			if inWindow {
				matches = append(matches, j)
			}
		}

		// Check minimum periods requirement
		if options.MinPeriods > 0 && len(matches) < options.MinPeriods {
			matches = nil
		}

		matchedIndices[i] = matches
	}

	// Build result DataFrame
	return buildRollingJoinResult(left, right, matchedIndices, options)
}

// rollingJoinWithGroups performs rolling join with grouping
func rollingJoinWithGroups(left, right *DataFrame, options RollingJoinOptions) (*DataFrame, error) {
	leftBy := options.Left_by
	rightBy := options.Right_by
	if len(rightBy) == 0 {
		rightBy = leftBy
	}

	// Group both DataFrames
	leftGroups, err := groupDataFrame(left, leftBy)
	if err != nil {
		return nil, fmt.Errorf("grouping left DataFrame: %w", err)
	}
	rightGroups, err := groupDataFrame(right, rightBy)
	if err != nil {
		return nil, fmt.Errorf("grouping right DataFrame: %w", err)
	}

	// Determine join columns
	leftOnCol := options.On
	rightOnCol := options.On
	if options.Left_on != "" {
		leftOnCol = options.Left_on
	}
	if options.Right_on != "" {
		rightOnCol = options.Right_on
	}

	var results []*DataFrame

	// Perform rolling join for each group
	for groupKey, leftIdx := range leftGroups {
		rightIdx, ok := rightGroups[groupKey]
		if !ok {
			// No matching group in right DataFrame
			result := createRollingNoMatchResult(left, right, leftIdx, options)
			results = append(results, result)
			continue
		}

		// Create sub-DataFrames for this group
		leftSub := selectRows(left, leftIdx)
		rightSub := selectRows(right, rightIdx)

		// Get join columns for this group
		leftOn, err := leftSub.Column(leftOnCol)
		if err != nil {
			return nil, err
		}
		rightOn, err := rightSub.Column(rightOnCol)
		if err != nil {
			return nil, err
		}

		// Perform rolling join for this group
		result, err := rollingJoinSimple(leftSub, rightSub, leftOn, rightOn, options)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if len(results) == 0 {
		return NewDataFrame()
	}

	// Concatenate all results
	return Concat(results, ConcatOptions{})
}

// buildRollingJoinResult builds the result DataFrame from rolling join matches
func buildRollingJoinResult(left, right *DataFrame, matchedIndices [][]int, options RollingJoinOptions) (*DataFrame, error) {
	// Calculate total number of result rows
	totalRows := 0
	for _, matches := range matchedIndices {
		if len(matches) == 0 {
			totalRows++ // One row with nulls
		} else {
			totalRows += len(matches)
		}
	}

	leftOnCol := options.On
	rightOnCol := options.On
	if options.Left_on != "" {
		leftOnCol = options.Left_on
	}
	if options.Right_on != "" {
		rightOnCol = options.Right_on
	}
	_ = leftOnCol // used below for skip check

	rightSuffix := "_right"
	if len(options.Suffixes) >= 2 {
		rightSuffix = options.Suffixes[1]
	}

	// Build indices for replication
	leftReplicatedIdx := make([]int, 0, totalRows)
	rightReplicatedIdx := make([]int, 0, totalRows)
	for i, matches := range matchedIndices {
		if len(matches) == 0 {
			leftReplicatedIdx = append(leftReplicatedIdx, i)
			rightReplicatedIdx = append(rightReplicatedIdx, -1) // null
		} else {
			for _, ri := range matches {
				leftReplicatedIdx = append(leftReplicatedIdx, i)
				rightReplicatedIdx = append(rightReplicatedIdx, ri)
			}
		}
	}

	var resultCols []series.Series

	// Build left columns (replicated for each match)
	for _, col := range left.columns {
		resultCols = append(resultCols, createReplicatedSeries(col, leftReplicatedIdx, col.Name()))
	}

	// Build skip set for right columns
	skipSet := make(map[string]bool)
	if rightOnCol == leftOnCol {
		skipSet[rightOnCol] = true
	}

	// Build right columns
	for _, col := range right.columns {
		if skipSet[col.Name()] {
			continue
		}

		name := col.Name()
		// Handle duplicate column names
		if left.HasColumn(name) {
			name = name + rightSuffix
		}

		// Create new series with values from matched indices
		resultCols = append(resultCols, createReplicatedSeries(col, rightReplicatedIdx, name))
	}

	return NewDataFrame(resultCols...)
}

// createReplicatedSeries creates a new series by replicating values based on indices
func createReplicatedSeries(original series.Series, indices []int, newName string) series.Series {
	n := len(indices)
	vals := make([]interface{}, n)
	validity := make([]bool, n)

	for i, idx := range indices {
		if idx < 0 || idx >= original.Len() {
			validity[i] = false
		} else if original.IsNull(idx) {
			validity[i] = false
		} else {
			vals[i] = original.Get(idx)
			validity[i] = true
		}
	}

	return createSeriesFromInterface(newName, vals, validity, original.DataType())
}

// createRollingNoMatchResult creates result with no matches for a group
func createRollingNoMatchResult(left, right *DataFrame, leftIndices []int, options RollingJoinOptions) *DataFrame {
	leftSub := selectRows(left, leftIndices)

	// Create empty match indices (all nulls)
	emptyMatches := make([][]int, len(leftIndices))
	result, _ := buildRollingJoinResult(leftSub, right, emptyMatches, options)
	return result
}

// rollingGroupDataFrame groups a DataFrame by columns (used internally for rolling join)
func rollingGroupDataFrame(df *DataFrame, byColumns []string) (map[string][]int, error) {
	groups := make(map[string][]int)

	if len(byColumns) == 0 {
		indices := make([]int, df.height)
		for i := range indices {
			indices[i] = i
		}
		groups[""] = indices
		return groups, nil
	}

	for _, name := range byColumns {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("group column %q not found", name)
		}
	}

	for row := 0; row < df.height; row++ {
		var parts []string
		for _, name := range byColumns {
			col, _ := df.Column(name)
			parts = append(parts, col.GetAsString(row))
		}
		key := strings.Join(parts, "\x00")
		groups[key] = append(groups[key], row)
	}

	return groups, nil
}
