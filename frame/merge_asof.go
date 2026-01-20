package frame

import (
	"fmt"
	"math"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
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
// This is useful for joining on time series data where exact matches may not exist
func (df *DataFrame) MergeAsof(right *DataFrame, options MergeAsofOptions) (*DataFrame, error) {
	// Set defaults
	if options.Direction == "" {
		options.Direction = "backward"
	}
	if len(options.Suffixes) == 0 {
		options.Suffixes = []string{"_x", "_y"}
	}
	// Handle AllowExact default
	allowExact := true
	if options.AllowExact != nil {
		allowExact = *options.AllowExact
	}

	// Validate direction
	if options.Direction != "backward" && options.Direction != "forward" && options.Direction != "nearest" {
		return nil, fmt.Errorf("direction must be 'backward', 'forward', or 'nearest', got '%s'", options.Direction)
	}

	// Determine merge columns
	leftOn := options.On
	if options.Left_on != "" {
		leftOn = options.Left_on
	}
	rightOn := options.On
	if options.Right_on != "" {
		rightOn = options.Right_on
	}

	if leftOn == "" || rightOn == "" {
		return nil, fmt.Errorf("merge column(s) must be specified")
	}

	// Get merge columns
	leftCol, err := df.Column(leftOn)
	if err != nil {
		return nil, fmt.Errorf("left merge column '%s' not found", leftOn)
	}
	rightCol, err := right.Column(rightOn)
	if err != nil {
		return nil, fmt.Errorf("right merge column '%s' not found", rightOn)
	}

	// Verify columns are numeric (for time-based merge)
	if !isNumericType(leftCol.DataType()) || !isNumericType(rightCol.DataType()) {
		return nil, fmt.Errorf("merge columns must be numeric for asof merge")
	}

	// Verify columns are sorted
	if !isColumnSorted(leftCol) {
		return nil, fmt.Errorf("left merge column '%s' must be sorted", leftOn)
	}
	if !isColumnSorted(rightCol) {
		return nil, fmt.Errorf("right merge column '%s' must be sorted", rightOn)
	}

	// Handle groupby if specified
	if len(options.Left_by) > 0 || len(options.Right_by) > 0 {
		return mergeAsofWithGroups(df, right, options, allowExact)
	}

	// Simple merge without groups
	return mergeAsofSimple(df, right, leftCol, rightCol, options, allowExact)
}

// mergeAsofSimple performs asof merge without grouping
func mergeAsofSimple(left, right *DataFrame, leftOn, rightOn series.Series, options MergeAsofOptions, allowExact bool) (*DataFrame, error) {
	leftLen := left.Height()
	rightLen := right.Height()

	// Find matches for each left row
	rightIndices := make([]int, leftLen)
	for i := 0; i < leftLen; i++ {
		rightIndices[i] = -1 // -1 means no match
	}

	// Extract values for efficient access
	leftValues := make([]float64, leftLen)
	for i := 0; i < leftLen; i++ {
		leftValues[i] = toFloat64Value(leftOn.Get(i))
	}

	rightValues := make([]float64, rightLen)
	for i := 0; i < rightLen; i++ {
		rightValues[i] = toFloat64Value(rightOn.Get(i))
	}

	// Find matches based on direction
	switch options.Direction {
	case "backward":
		// For each left value, find the last right value that is <= left value
		for i := 0; i < leftLen; i++ {
			if leftOn.IsNull(i) {
				continue
			}
			leftVal := leftValues[i]

			// Binary search for the position
			idx := sort.Search(rightLen, func(j int) bool {
				if allowExact {
					return rightValues[j] > leftVal
				}
				return rightValues[j] >= leftVal
			})

			// idx is the first index where right > left (or >= if not allowing exact)
			// So idx-1 is the last index where right <= left (or < if not allowing exact)
			if idx > 0 {
				matchIdx := idx - 1
				// If not allowing exact matches, check if this is an exact match
				if !allowExact && rightValues[matchIdx] == leftVal {
					// Skip this match, look for previous one
					if matchIdx > 0 {
						matchIdx--
					} else {
						continue // No valid match
					}
				}
				if options.Tolerance > 0 {
					distance := leftVal - rightValues[matchIdx]
					if distance > options.Tolerance {
						continue // No match within tolerance
					}
				}
				rightIndices[i] = matchIdx
			}
		}

	case "forward":
		// For each left value, find the first right value that is >= left value
		for i := 0; i < leftLen; i++ {
			if leftOn.IsNull(i) {
				continue
			}
			leftVal := leftValues[i]

			// Binary search for the position
			idx := sort.Search(rightLen, func(j int) bool {
				if allowExact {
					return rightValues[j] >= leftVal
				}
				return rightValues[j] > leftVal
			})

			if idx < rightLen {
				// If not allowing exact matches, check if this is an exact match
				if !allowExact && rightValues[idx] == leftVal {
					// Skip this match, look for next one
					if idx < rightLen-1 {
						idx++
					} else {
						continue // No valid match
					}
				}
				if options.Tolerance > 0 {
					distance := rightValues[idx] - leftVal
					if distance > options.Tolerance {
						continue // No match within tolerance
					}
				}
				rightIndices[i] = idx
			}
		}

	case "nearest":
		// For each left value, find the nearest right value
		for i := 0; i < leftLen; i++ {
			if leftOn.IsNull(i) {
				continue
			}
			leftVal := leftValues[i]

			// Binary search for the position
			idx := sort.Search(rightLen, func(j int) bool {
				return rightValues[j] >= leftVal
			})

			// Check both idx-1 and idx to find the nearest
			bestIdx := -1
			bestDist := math.Inf(1)

			// Check previous value
			if idx > 0 {
				dist := math.Abs(leftVal - rightValues[idx-1])
				if dist < bestDist && (allowExact || rightValues[idx-1] != leftVal) {
					bestDist = dist
					bestIdx = idx - 1
				}
			}

			// Check current value
			if idx < rightLen {
				dist := math.Abs(leftVal - rightValues[idx])
				if dist < bestDist && (allowExact || rightValues[idx] != leftVal) {
					bestDist = dist
					bestIdx = idx
				}
			}

			if bestIdx >= 0 && (options.Tolerance <= 0 || bestDist <= options.Tolerance) {
				rightIndices[i] = bestIdx
			}
		}
	}

	// Build result DataFrame
	return buildMergeAsofResult(left, right, rightIndices, options)
}

// mergeAsofWithGroups performs asof merge with grouping
func mergeAsofWithGroups(left, right *DataFrame, options MergeAsofOptions, allowExact bool) (*DataFrame, error) {
	// Group both DataFrames
	leftGroups, err := groupDataFrame(left, options.Left_by)
	if err != nil {
		return nil, fmt.Errorf("failed to group left DataFrame: %w", err)
	}

	rightGroups, err := groupDataFrame(right, options.Right_by)
	if err != nil {
		return nil, fmt.Errorf("failed to group right DataFrame: %w", err)
	}

	// Perform merge for each group
	resultFrames := make([]*DataFrame, 0)

	for groupKey, leftIndices := range leftGroups {
		rightIndices, exists := rightGroups[groupKey]
		if !exists {
			// No matching group in right DataFrame
			// Create result with nulls for right columns
			noMatchResult := createNoMatchResult(left, right, leftIndices, options)
			resultFrames = append(resultFrames, noMatchResult)
			continue
		}

		// Create sub-DataFrames for this group
		leftSub := selectRows(left, leftIndices)
		rightSub := selectRows(right, rightIndices)

		// Get merge columns for this group
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

		// Perform merge for this group
		groupResult, err := mergeAsofSimple(leftSub, rightSub, leftCol, rightCol, options, allowExact)
		if err != nil {
			return nil, fmt.Errorf("failed to merge group %s: %w", groupKey, err)
		}

		resultFrames = append(resultFrames, groupResult)
	}

	// Concatenate all results
	if len(resultFrames) == 0 {
		return nil, fmt.Errorf("no groups to merge")
	}

	return Concat(resultFrames, ConcatOptions{Axis: 0})
}

// buildMergeAsofResult builds the result DataFrame from merge indices
func buildMergeAsofResult(left, right *DataFrame, rightIndices []int, options MergeAsofOptions) (*DataFrame, error) {
	resultColumns := make([]series.Series, 0)

	// Add all left columns
	for _, col := range left.columns {
		resultColumns = append(resultColumns, col)
	}

	// Add right columns (with renaming for duplicates)
	leftColNames := make(map[string]bool)
	for _, col := range left.columns {
		leftColNames[col.Name()] = true
	}

	for _, col := range right.columns {
		colName := col.Name()

		// Skip the merge column if it has the same name as the left merge column
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

		// Also skip columns that are in the groupby (they're already in left)
		skipCol := false
		for i, rightByCol := range options.Right_by {
			if i < len(options.Left_by) && colName == rightByCol && rightByCol == options.Left_by[i] {
				skipCol = true
				break
			}
		}
		if skipCol {
			continue
		}

		// Handle duplicate column names
		if leftColNames[colName] {
			colName = colName + options.Suffixes[1]
		}

		// Create new series with values from matched indices
		newSeries := createMergedSeries(col, rightIndices, colName)
		resultColumns = append(resultColumns, newSeries)
	}

	return NewDataFrame(resultColumns...)
}

// createMergedSeries creates a new series by selecting values based on indices
func createMergedSeries(original series.Series, indices []int, newName string) series.Series {
	length := len(indices)
	dataType := original.DataType()

	// Create arrays based on type
	switch dataType.(type) {
	case datatypes.Int64:
		values := make([]int64, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if idx >= 0 && !original.IsNull(idx) {
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
			if idx >= 0 && !original.IsNull(idx) {
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
			if idx >= 0 && !original.IsNull(idx) {
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
			if idx >= 0 && !original.IsNull(idx) {
				values[i] = original.Get(idx)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return createSeriesFromInterface(newName, values, validity, dataType)
	}
}

// Helper to check if a column is sorted
func isColumnSorted(s series.Series) bool {
	for i := 1; i < s.Len(); i++ {
		if s.IsNull(i-1) || s.IsNull(i) {
			continue
		}

		prev := toFloat64Value(s.Get(i - 1))
		curr := toFloat64Value(s.Get(i))

		if prev > curr {
			return false
		}
	}
	return true
}

// Helper to group DataFrame by columns
func groupDataFrame(df *DataFrame, byColumns []string) (map[string][]int, error) {
	if len(byColumns) == 0 {
		// Single group with all indices
		allIndices := make([]int, df.Height())
		for i := range allIndices {
			allIndices[i] = i
		}
		return map[string][]int{"": allIndices}, nil
	}

	// Build group key for each row
	groups := make(map[string][]int)

	for i := 0; i < df.Height(); i++ {
		key := ""
		for j, colName := range byColumns {
			col, err := df.Column(colName)
			if err != nil {
				return nil, err
			}

			if j > 0 {
				key += "|"
			}

			if col.IsNull(i) {
				key += "<null>"
			} else {
				key += fmt.Sprintf("%v", col.Get(i))
			}
		}

		groups[key] = append(groups[key], i)
	}

	return groups, nil
}

// Helper to select rows from DataFrame
func selectRows(df *DataFrame, indices []int) *DataFrame {
	resultColumns := make([]series.Series, len(df.columns))

	for i, col := range df.columns {
		// Create new series with selected rows
		newSeries := selectSeriesRows(col, indices)
		resultColumns[i] = newSeries
	}

	result, _ := NewDataFrame(resultColumns...)
	return result
}

// Helper to select rows from a series
func selectSeriesRows(s series.Series, indices []int) series.Series {
	length := len(indices)
	dataType := s.DataType()

	switch dataType.(type) {
	case datatypes.Int64:
		values := make([]int64, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !s.IsNull(idx) {
				values[i] = s.Get(idx).(int64)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity(s.Name(), values, validity, dataType)

	case datatypes.Float64:
		values := make([]float64, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !s.IsNull(idx) {
				values[i] = s.Get(idx).(float64)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity(s.Name(), values, validity, dataType)

	case datatypes.String:
		values := make([]string, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !s.IsNull(idx) {
				values[i] = s.Get(idx).(string)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity(s.Name(), values, validity, dataType)

	default:
		// Generic handling
		values := make([]interface{}, length)
		validity := make([]bool, length)

		for i, idx := range indices {
			if !s.IsNull(idx) {
				values[i] = s.Get(idx)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}

		return createSeriesFromInterface(s.Name(), values, validity, dataType)
	}
}

// Helper to create result with no matches
func createNoMatchResult(left, right *DataFrame, leftIndices []int, options MergeAsofOptions) *DataFrame {
	// Create DataFrame with left rows and null right columns
	leftSub := selectRows(left, leftIndices)

	// Create null indices for right columns
	nullIndices := make([]int, len(leftIndices))
	for i := range nullIndices {
		nullIndices[i] = -1
	}

	result, _ := buildMergeAsofResult(leftSub, right, nullIndices, options)
	return result
}
