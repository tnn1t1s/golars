package frame

import (
	"fmt"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
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
// This is useful for joining time series data within a rolling window
func (df *DataFrame) RollingJoin(right *DataFrame, options RollingJoinOptions) (*DataFrame, error) {
	// Set defaults
	if options.Direction == "" {
		options.Direction = "backward"
	}
	if options.ClosedInterval == "" {
		options.ClosedInterval = "right"
	}
	if len(options.Suffixes) == 0 {
		options.Suffixes = []string{"_x", "_y"}
	}
	if options.MinPeriods == 0 {
		options.MinPeriods = 1
	}

	// Validate parameters
	if options.WindowSize <= 0 {
		return nil, fmt.Errorf("window size must be positive, got %f", options.WindowSize)
	}
	if options.MinPeriods < 1 {
		return nil, fmt.Errorf("min_periods must be at least 1, got %d", options.MinPeriods)
	}
	if options.Direction != "backward" && options.Direction != "forward" && options.Direction != "both" {
		return nil, fmt.Errorf("direction must be 'backward', 'forward', or 'both', got '%s'", options.Direction)
	}
	if options.ClosedInterval != "left" && options.ClosedInterval != "right" &&
		options.ClosedInterval != "both" && options.ClosedInterval != "neither" {
		return nil, fmt.Errorf("closed_interval must be 'left', 'right', 'both', or 'neither', got '%s'", options.ClosedInterval)
	}

	// Determine join columns
	leftOn := options.On
	if options.Left_on != "" {
		leftOn = options.Left_on
	}
	rightOn := options.On
	if options.Right_on != "" {
		rightOn = options.Right_on
	}

	if leftOn == "" || rightOn == "" {
		return nil, fmt.Errorf("join column(s) must be specified")
	}

	// Get join columns
	leftCol, err := df.Column(leftOn)
	if err != nil {
		return nil, fmt.Errorf("left join column '%s' not found", leftOn)
	}
	rightCol, err := right.Column(rightOn)
	if err != nil {
		return nil, fmt.Errorf("right join column '%s' not found", rightOn)
	}

	// Verify columns are numeric
	if !isNumericType(leftCol.DataType()) || !isNumericType(rightCol.DataType()) {
		return nil, fmt.Errorf("join columns must be numeric for rolling join")
	}

	// Verify columns are sorted
	if !isColumnSorted(leftCol) {
		return nil, fmt.Errorf("left join column '%s' must be sorted", leftOn)
	}
	if !isColumnSorted(rightCol) {
		return nil, fmt.Errorf("right join column '%s' must be sorted", rightOn)
	}

	// Handle groupby if specified
	if len(options.Left_by) > 0 || len(options.Right_by) > 0 {
		return rollingJoinWithGroups(df, right, options)
	}

	// Simple rolling join without groups
	return rollingJoinSimple(df, right, leftCol, rightCol, options)
}

// rollingJoinSimple performs rolling join without grouping
func rollingJoinSimple(left, right *DataFrame, leftOn, rightOn series.Series, options RollingJoinOptions) (*DataFrame, error) {
	leftLen := left.Height()
	rightLen := right.Height()

	// For each left row, collect all matching right rows within the window
	matchedIndices := make([][]int, leftLen)

	// Extract values for efficient access
	leftValues := make([]float64, leftLen)
	for i := 0; i < leftLen; i++ {
		leftValues[i] = toFloat64Value(leftOn.Get(i))
	}

	rightValues := make([]float64, rightLen)
	for i := 0; i < rightLen; i++ {
		rightValues[i] = toFloat64Value(rightOn.Get(i))
	}

	// Calculate window bounds and find matches
	for i := 0; i < leftLen; i++ {
		if leftOn.IsNull(i) {
			matchedIndices[i] = []int{}
			continue
		}

		leftVal := leftValues[i]
		matches := []int{}

		// Calculate window bounds
		var windowStart, windowEnd float64

		if options.Center {
			// Center window ignores direction
			offset := options.WindowSize / 2
			windowStart = leftVal - offset
			windowEnd = leftVal + offset
		} else {
			switch options.Direction {
			case "backward":
				windowEnd = leftVal
				windowStart = leftVal - options.WindowSize

			case "forward":
				windowStart = leftVal
				windowEnd = leftVal + options.WindowSize

			case "both":
				windowStart = leftVal - options.WindowSize
				windowEnd = leftVal + options.WindowSize
			}
		}

		// Apply closed interval rules
		startInclusive := options.ClosedInterval == "left" || options.ClosedInterval == "both"
		endInclusive := options.ClosedInterval == "right" || options.ClosedInterval == "both"

		// Find all right values within the window
		// Use binary search to find the range
		startIdx := sort.Search(rightLen, func(j int) bool {
			if startInclusive {
				return rightValues[j] >= windowStart
			}
			return rightValues[j] > windowStart
		})

		endIdx := sort.Search(rightLen, func(j int) bool {
			if endInclusive {
				return rightValues[j] > windowEnd
			}
			return rightValues[j] >= windowEnd
		})

		// Collect all indices in the range
		for j := startIdx; j < endIdx; j++ {
			matches = append(matches, j)
		}

		// Check minimum periods requirement
		if len(matches) >= options.MinPeriods {
			matchedIndices[i] = matches
		} else {
			matchedIndices[i] = []int{}
		}
	}

	// Build result DataFrame
	return buildRollingJoinResult(left, right, matchedIndices, options)
}

// rollingJoinWithGroups performs rolling join with grouping
func rollingJoinWithGroups(left, right *DataFrame, options RollingJoinOptions) (*DataFrame, error) {
	// Group both DataFrames
	leftGroups, err := groupDataFrame(left, options.Left_by)
	if err != nil {
		return nil, fmt.Errorf("failed to group left DataFrame: %w", err)
	}

	rightGroups, err := groupDataFrame(right, options.Right_by)
	if err != nil {
		return nil, fmt.Errorf("failed to group right DataFrame: %w", err)
	}

	// Perform rolling join for each group
	resultFrames := make([]*DataFrame, 0)

	for groupKey, leftIndices := range leftGroups {
		rightIndices, exists := rightGroups[groupKey]
		if !exists {
			// No matching group in right DataFrame
			// Create result with nulls for right columns
			noMatchResult := createRollingNoMatchResult(left, right, leftIndices, options)
			resultFrames = append(resultFrames, noMatchResult)
			continue
		}

		// Create sub-DataFrames for this group
		leftSub := selectRows(left, leftIndices)
		rightSub := selectRows(right, rightIndices)

		// Get join columns for this group
		leftOn := options.On
		if options.Left_on != "" {
			leftOn = options.Left_on
		}
		rightOn := options.On
		if options.Right_on != "" {
			rightOn = options.Right_on
		}

		leftCol, _ := leftSub.Column(leftOn)
		rightCol, _ := rightSub.Column(rightOn)

		// Perform rolling join for this group
		groupResult, err := rollingJoinSimple(leftSub, rightSub, leftCol, rightCol, options)
		if err != nil {
			return nil, fmt.Errorf("failed to join group %s: %w", groupKey, err)
		}

		resultFrames = append(resultFrames, groupResult)
	}

	// Concatenate all results
	if len(resultFrames) == 0 {
		return nil, fmt.Errorf("no groups to join")
	}

	return Concat(resultFrames, ConcatOptions{Axis: 0})
}

// buildRollingJoinResult builds the result DataFrame from rolling join matches
func buildRollingJoinResult(left, right *DataFrame, matchedIndices [][]int, options RollingJoinOptions) (*DataFrame, error) {
	// Calculate total number of result rows
	totalRows := 0
	for _, matches := range matchedIndices {
		if len(matches) > 0 {
			totalRows += len(matches)
		} else {
			totalRows += 1 // One row with nulls
		}
	}

	// Prepare to build result columns
	resultColumns := make([]series.Series, 0)
	leftColNames := make(map[string]bool)

	// Build left columns (replicated for each match)
	for _, col := range left.columns {
		leftColNames[col.Name()] = true

		// Build indices for replication
		replicationIndices := make([]int, 0, totalRows)
		for i, matches := range matchedIndices {
			if len(matches) > 0 {
				for range matches {
					replicationIndices = append(replicationIndices, i)
				}
			} else {
				replicationIndices = append(replicationIndices, i)
			}
		}

		// Create replicated series
		newSeries := createReplicatedSeries(col, replicationIndices, col.Name())
		resultColumns = append(resultColumns, newSeries)
	}

	// Build right columns
	for _, col := range right.columns {
		colName := col.Name()

		// Skip the join column if it has the same name as the left join column
		leftOn := options.On
		if options.Left_on != "" {
			leftOn = options.Left_on
		}
		rightOn := options.On
		if options.Right_on != "" {
			rightOn = options.Right_on
		}

		if colName == rightOn && rightOn == leftOn {
			continue
		}

		// Handle duplicate column names
		if leftColNames[colName] {
			colName = colName + options.Suffixes[1]
		}

		// Build indices for right columns
		rightIndices := make([]int, 0, totalRows)
		for _, matches := range matchedIndices {
			if len(matches) > 0 {
				rightIndices = append(rightIndices, matches...)
			} else {
				rightIndices = append(rightIndices, -1) // -1 indicates null
			}
		}

		// Create new series with values from matched indices
		newSeries := createMergedSeries(col, rightIndices, colName)
		resultColumns = append(resultColumns, newSeries)
	}

	return NewDataFrame(resultColumns...)
}

// createReplicatedSeries creates a new series by replicating values based on indices
func createReplicatedSeries(original series.Series, indices []int, newName string) series.Series {
	length := len(indices)
	dataType := original.DataType()

	switch dataType.(type) {
	case datatypes.Int64:
		values := make([]int64, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !original.IsNull(idx) {
				values[i] = original.Get(idx).(int64)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity(newName, values, validity, dataType)

	case datatypes.Float64:
		values := make([]float64, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !original.IsNull(idx) {
				values[i] = original.Get(idx).(float64)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity(newName, values, validity, dataType)

	case datatypes.String:
		values := make([]string, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !original.IsNull(idx) {
				values[i] = original.Get(idx).(string)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity(newName, values, validity, dataType)

	default:
		// Generic handling
		values := make([]interface{}, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !original.IsNull(idx) {
				values[i] = original.Get(idx)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return createSeriesFromInterface(newName, values, validity, dataType)
	}
}

// createRollingNoMatchResult creates result with no matches for a group
func createRollingNoMatchResult(left, right *DataFrame, leftIndices []int, options RollingJoinOptions) *DataFrame {
	// Create DataFrame with left rows and null right columns
	leftSub := selectRows(left, leftIndices)

	// Create empty match indices (all nulls)
	emptyMatches := make([][]int, len(leftIndices))
	for i := range emptyMatches {
		emptyMatches[i] = []int{}
	}

	result, _ := buildRollingJoinResult(leftSub, right, emptyMatches, options)
	return result
}
