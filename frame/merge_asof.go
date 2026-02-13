package frame

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/tnn1t1s/golars/series"
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
func (df *DataFrame) MergeAsof(right *DataFrame, options MergeAsofOptions) (*DataFrame, error) {
	// Set defaults
	if options.Direction == "" {
		options.Direction = "backward"
	}
	if len(options.Suffixes) == 0 {
		options.Suffixes = []string{"", "_right"}
	}

	// Handle AllowExact default
	allowExact := true
	if options.AllowExact != nil {
		allowExact = *options.AllowExact
	}

	// Validate direction
	switch options.Direction {
	case "backward", "forward", "nearest":
	default:
		return nil, fmt.Errorf("invalid direction: %q (must be backward, forward, or nearest)", options.Direction)
	}

	// Determine merge columns
	leftOnCol := options.On
	rightOnCol := options.On
	if options.Left_on != "" {
		leftOnCol = options.Left_on
	}
	if options.Right_on != "" {
		rightOnCol = options.Right_on
	}
	if leftOnCol == "" || rightOnCol == "" {
		return nil, fmt.Errorf("merge column must be specified via On or Left_on/Right_on")
	}

	// Get merge columns
	leftOn, err := df.Column(leftOnCol)
	if err != nil {
		return nil, fmt.Errorf("left merge column: %w", err)
	}
	rightOn, err := right.Column(rightOnCol)
	if err != nil {
		return nil, fmt.Errorf("right merge column: %w", err)
	}

	// Verify columns are sorted
	if !isColumnSorted(leftOn) {
		return nil, fmt.Errorf("left merge column %q must be sorted", leftOnCol)
	}
	if !isColumnSorted(rightOn) {
		return nil, fmt.Errorf("right merge column %q must be sorted", rightOnCol)
	}

	// Handle groupby if specified
	if len(options.Left_by) > 0 || len(options.Right_by) > 0 {
		return mergeAsofWithGroups(df, right, options, allowExact)
	}

	// Simple merge without groups
	return mergeAsofSimple(df, right, leftOn, rightOn, options, allowExact)
}

// mergeAsofSimple performs asof merge without grouping
func mergeAsofSimple(left, right *DataFrame, leftOn, rightOn series.Series, options MergeAsofOptions, allowExact bool) (*DataFrame, error) {
	leftLen := left.height
	rightLen := right.height

	// Find matches for each left row
	rightIndices := make([]int, leftLen) // -1 means no match

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

	// Find matches based on direction
	switch options.Direction {
	case "backward":
		for i := 0; i < leftLen; i++ {
			rightIndices[i] = -1
			if math.IsNaN(leftVals[i]) {
				continue
			}
			lv := leftVals[i]

			// Binary search for the last right value <= left value
			idx := sort.Search(rightLen, func(j int) bool {
				return rightVals[j] > lv
			})
			// idx is the first index where right > left
			// So idx-1 is the last index where right <= left
			idx--

			if !allowExact {
				// Skip exact matches
				for idx >= 0 && rightVals[idx] == lv {
					idx--
				}
			}

			if idx < 0 {
				continue
			}

			// Check tolerance
			if options.Tolerance > 0 && math.Abs(lv-rightVals[idx]) > options.Tolerance {
				continue
			}

			rightIndices[i] = idx
		}

	case "forward":
		for i := 0; i < leftLen; i++ {
			rightIndices[i] = -1
			if math.IsNaN(leftVals[i]) {
				continue
			}
			lv := leftVals[i]

			// Binary search for the first right value >= left value
			idx := sort.Search(rightLen, func(j int) bool {
				return rightVals[j] >= lv
			})

			if !allowExact {
				// Skip exact matches
				for idx < rightLen && rightVals[idx] == lv {
					idx++
				}
			}

			if idx >= rightLen {
				continue
			}

			// Check tolerance
			if options.Tolerance > 0 && math.Abs(rightVals[idx]-lv) > options.Tolerance {
				continue
			}

			rightIndices[i] = idx
		}

	case "nearest":
		for i := 0; i < leftLen; i++ {
			rightIndices[i] = -1
			if math.IsNaN(leftVals[i]) {
				continue
			}
			lv := leftVals[i]

			// Binary search for the position
			idx := sort.Search(rightLen, func(j int) bool {
				return rightVals[j] >= lv
			})

			bestIdx := -1
			bestDist := math.Inf(1)

			// Check previous value (backward)
			if idx > 0 {
				dist := math.Abs(lv - rightVals[idx-1])
				if !allowExact && rightVals[idx-1] == lv {
					// skip
				} else if dist < bestDist {
					bestDist = dist
					bestIdx = idx - 1
				}
			}

			// Check current value (forward)
			if idx < rightLen {
				dist := math.Abs(rightVals[idx] - lv)
				if !allowExact && rightVals[idx] == lv {
					// skip
				} else if dist < bestDist {
					bestDist = dist
					bestIdx = idx
				}
			}

			if bestIdx < 0 {
				continue
			}

			// Check tolerance
			if options.Tolerance > 0 && bestDist > options.Tolerance {
				continue
			}

			rightIndices[i] = bestIdx
		}
	}

	// Build result DataFrame
	return buildMergeAsofResult(left, right, rightIndices, options)
}

// mergeAsofWithGroups performs asof merge with grouping
func mergeAsofWithGroups(left, right *DataFrame, options MergeAsofOptions, allowExact bool) (*DataFrame, error) {
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

	// Determine merge columns
	leftOnCol := options.On
	rightOnCol := options.On
	if options.Left_on != "" {
		leftOnCol = options.Left_on
	}
	if options.Right_on != "" {
		rightOnCol = options.Right_on
	}

	var results []*DataFrame

	// Perform merge for each group
	for groupKey, leftIdx := range leftGroups {
		rightIdx, ok := rightGroups[groupKey]
		if !ok {
			// No matching group in right DataFrame
			result := createNoMatchResult(left, right, leftIdx, options)
			results = append(results, result)
			continue
		}

		// Create sub-DataFrames for this group
		leftSub := selectRows(left, leftIdx)
		rightSub := selectRows(right, rightIdx)

		// Get merge columns for this group
		leftOn, err := leftSub.Column(leftOnCol)
		if err != nil {
			return nil, err
		}
		rightOn, err := rightSub.Column(rightOnCol)
		if err != nil {
			return nil, err
		}

		// Perform merge for this group
		result, err := mergeAsofSimple(leftSub, rightSub, leftOn, rightOn, options, allowExact)
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

// buildMergeAsofResult builds the result DataFrame from merge indices
func buildMergeAsofResult(left, right *DataFrame, rightIndices []int, options MergeAsofOptions) (*DataFrame, error) {
	var resultCols []series.Series

	leftOnCol := options.On
	rightOnCol := options.On
	if options.Left_on != "" {
		leftOnCol = options.Left_on
	}
	if options.Right_on != "" {
		rightOnCol = options.Right_on
	}

	leftSuffix := ""
	rightSuffix := "_right"
	if len(options.Suffixes) >= 2 {
		leftSuffix = options.Suffixes[0]
		rightSuffix = options.Suffixes[1]
	}

	// Add all left columns
	for _, col := range left.columns {
		name := col.Name()
		if leftSuffix != "" && right.HasColumn(name) && name != leftOnCol {
			name = name + leftSuffix
		}
		if name != col.Name() {
			resultCols = append(resultCols, col.Rename(name))
		} else {
			resultCols = append(resultCols, col)
		}
	}

	// Build skip set for right columns
	skipSet := make(map[string]bool)
	// Skip the merge column if it has the same name as the left merge column
	if rightOnCol == leftOnCol {
		skipSet[rightOnCol] = true
	}
	// Also skip columns that are in the groupby (they're already in left)
	for _, byCol := range options.Left_by {
		skipSet[byCol] = true
	}
	for _, byCol := range options.Right_by {
		skipSet[byCol] = true
	}

	// Add right columns (with renaming for duplicates)
	for _, col := range right.columns {
		if skipSet[col.Name()] {
			continue
		}

		name := col.Name()
		// Handle duplicate column names
		if left.HasColumn(name) {
			name = name + rightSuffix
		}

		resultCols = append(resultCols, createMergedSeries(col, rightIndices, name))
	}

	return NewDataFrame(resultCols...)
}

// createMergedSeries creates a new series by selecting values based on indices
func createMergedSeries(original series.Series, indices []int, newName string) series.Series {
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

// Helper to check if a column is sorted
func isColumnSorted(s series.Series) bool {
	for i := 1; i < s.Len(); i++ {
		if s.IsNull(i) || s.IsNull(i-1) {
			continue
		}
		curr := toFloat64Value(s.Get(i))
		prev := toFloat64Value(s.Get(i - 1))
		if curr < prev {
			return false
		}
	}
	return true
}

// Helper to group DataFrame by columns
func groupDataFrame(df *DataFrame, byColumns []string) (map[string][]int, error) {
	groups := make(map[string][]int)

	if len(byColumns) == 0 {
		// Single group with all indices
		indices := make([]int, df.height)
		for i := range indices {
			indices[i] = i
		}
		groups[""] = indices
		return groups, nil
	}

	// Validate columns exist
	for _, name := range byColumns {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("group column %q not found", name)
		}
	}

	// Build group key for each row
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

// Helper to select rows from DataFrame
func selectRows(df *DataFrame, indices []int) *DataFrame {
	var resultCols []series.Series
	for _, col := range df.columns {
		resultCols = append(resultCols, selectSeriesRows(col, indices))
	}
	result, _ := NewDataFrame(resultCols...)
	return result
}

// Helper to select rows from a series
func selectSeriesRows(s series.Series, indices []int) series.Series {
	if taken, ok := series.TakeFast(s, indices); ok {
		return taken
	}
	return s.Take(indices)
}

// Helper to create result with no matches
func createNoMatchResult(left, right *DataFrame, leftIndices []int, options MergeAsofOptions) *DataFrame {
	leftSub := selectRows(left, leftIndices)

	// Create null indices for right columns
	nullIndices := make([]int, len(leftIndices))
	for i := range nullIndices {
		nullIndices[i] = -1
	}

	result, _ := buildMergeAsofResult(leftSub, right, nullIndices, options)
	return result
}
