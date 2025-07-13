package frame

import (
	"fmt"

	"github.com/davidpalaitis/golars/compute"
	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// JoinType specifies the type of join operation
type JoinType string

const (
	InnerJoin JoinType = "inner"
	LeftJoin  JoinType = "left"
	RightJoin JoinType = "right"
	OuterJoin JoinType = "outer"
	CrossJoin JoinType = "cross"
	AntiJoin  JoinType = "anti"
	SemiJoin  JoinType = "semi"
)

// JoinConfig contains configuration for join operations
type JoinConfig struct {
	How      JoinType
	LeftOn   []string
	RightOn  []string
	Suffix   string // Default: "_right"
}

// Join performs a join operation on a single column
func (df *DataFrame) Join(other *DataFrame, on string, how JoinType) (*DataFrame, error) {
	return df.JoinOn(other, []string{on}, []string{on}, how)
}

// JoinOn performs a join operation on specified columns
func (df *DataFrame) JoinOn(other *DataFrame, leftOn []string, rightOn []string, how JoinType) (*DataFrame, error) {
	config := JoinConfig{
		How:     how,
		LeftOn:  leftOn,
		RightOn: rightOn,
		Suffix:  "_right",
	}
	return df.JoinWithConfig(other, config)
}

// JoinWithConfig performs a join operation with full configuration
func (df *DataFrame) JoinWithConfig(other *DataFrame, config JoinConfig) (*DataFrame, error) {
	df.mu.RLock()
	other.mu.RLock()
	defer df.mu.RUnlock()
	defer other.mu.RUnlock()

	// Validate join columns
	if err := validateJoinColumns(df, other, config); err != nil {
		return nil, err
	}

	// Dispatch to specific join implementation
	switch config.How {
	case InnerJoin:
		return innerJoin(df, other, config)
	case LeftJoin:
		return leftJoin(df, other, config)
	case RightJoin:
		// Right join is left join with swapped sides
		swappedConfig := config
		swappedConfig.LeftOn, swappedConfig.RightOn = config.RightOn, config.LeftOn
		return leftJoin(other, df, swappedConfig)
	case OuterJoin:
		return outerJoin(df, other, config)
	case CrossJoin:
		return crossJoin(df, other, config)
	case AntiJoin:
		return antiJoin(df, other, config)
	case SemiJoin:
		return semiJoin(df, other, config)
	default:
		return nil, fmt.Errorf("unknown join type: %s", config.How)
	}
}

// validateJoinColumns ensures join columns exist and have compatible types
func validateJoinColumns(left, right *DataFrame, config JoinConfig) error {
	if len(config.LeftOn) != len(config.RightOn) {
		return fmt.Errorf("number of left and right join columns must match")
	}

	if len(config.LeftOn) == 0 && config.How != CrossJoin {
		return fmt.Errorf("join columns required for %s join", config.How)
	}

	// Validate left columns exist
	for _, col := range config.LeftOn {
		if _, err := left.Column(col); err != nil {
			return fmt.Errorf("left join column %s not found", col)
		}
	}

	// Validate right columns exist
	for _, col := range config.RightOn {
		if _, err := right.Column(col); err != nil {
			return fmt.Errorf("right join column %s not found", col)
		}
	}

	// Validate compatible types
	for i := range config.LeftOn {
		leftCol, _ := left.Column(config.LeftOn[i])
		rightCol, _ := right.Column(config.RightOn[i])

		if !leftCol.DataType().Equals(rightCol.DataType()) {
			return fmt.Errorf("incompatible types for join columns %s and %s", 
				config.LeftOn[i], config.RightOn[i])
		}
	}

	return nil
}

// getJoinColumns extracts the specified columns for joining
func getJoinColumns(df *DataFrame, columns []string) ([]series.Series, error) {
	joinCols := make([]series.Series, len(columns))
	for i, col := range columns {
		s, err := df.Column(col)
		if err != nil {
			return nil, err
		}
		joinCols[i] = s
	}
	return joinCols, nil
}

// innerJoin performs an inner join operation
func innerJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	// Get join columns
	leftKeys, err := getJoinColumns(left, config.LeftOn)
	if err != nil {
		return nil, err
	}

	rightKeys, err := getJoinColumns(right, config.RightOn)
	if err != nil {
		return nil, err
	}

	// Build hash table on smaller side for efficiency
	var ht *compute.HashTable
	var probeKeys []series.Series
	var probeDF *DataFrame
	var buildOnLeft bool

	if left.Height() <= right.Height() {
		ht, err = compute.BuildHashTable(leftKeys)
		probeKeys = rightKeys
		probeDF = right
		buildOnLeft = true
	} else {
		ht, err = compute.BuildHashTable(rightKeys)
		probeKeys = leftKeys
		probeDF = left
		buildOnLeft = false
	}

	if err != nil {
		return nil, err
	}

	// Probe and collect matches
	leftIndices := make([]int, 0)
	rightIndices := make([]int, 0)

	for i := 0; i < probeDF.Height(); i++ {
		matches := ht.Probe(probeKeys, i)
		for _, match := range matches {
			if buildOnLeft {
				leftIndices = append(leftIndices, match)
				rightIndices = append(rightIndices, i)
			} else {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, match)
			}
		}
	}

	// Build result DataFrame
	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// leftJoin performs a left join operation
func leftJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftKeys, err := getJoinColumns(left, config.LeftOn)
	if err != nil {
		return nil, err
	}

	rightKeys, err := getJoinColumns(right, config.RightOn)
	if err != nil {
		return nil, err
	}

	// Build hash table on right side
	ht, err := compute.BuildHashTable(rightKeys)
	if err != nil {
		return nil, err
	}

	leftIndices := make([]int, 0)
	rightIndices := make([]int, 0)

	// Probe left side
	for i := 0; i < left.Height(); i++ {
		matches := ht.Probe(leftKeys, i)

		if len(matches) == 0 {
			// No match - include with null right side
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1) // Sentinel for null
		} else {
			// Include all matches
			for _, match := range matches {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, match)
			}
		}
	}

	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// outerJoin performs a full outer join operation
func outerJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	// First perform left join
	leftResult, err := leftJoin(left, right, config)
	if err != nil {
		return nil, err
	}

	// Then find unmatched right rows
	rightKeys, err := getJoinColumns(right, config.RightOn)
	if err != nil {
		return nil, err
	}

	leftKeys, err := getJoinColumns(left, config.LeftOn)
	if err != nil {
		return nil, err
	}

	// Build hash table on left side
	ht, err := compute.BuildHashTable(leftKeys)
	if err != nil {
		return nil, err
	}

	// Find unmatched right rows
	unmatchedRightIndices := make([]int, 0)
	for i := 0; i < right.Height(); i++ {
		matches := ht.Probe(rightKeys, i)
		if len(matches) == 0 {
			unmatchedRightIndices = append(unmatchedRightIndices, i)
		}
	}

	// If no unmatched right rows, left join result is the outer join
	if len(unmatchedRightIndices) == 0 {
		return leftResult, nil
	}

	// Build indices for unmatched right rows
	leftIndices := make([]int, len(unmatchedRightIndices))
	for i := range leftIndices {
		leftIndices[i] = -1 // Null left side
	}

	// Build result for unmatched right rows
	unmatchedResult, err := buildJoinResult(left, right, leftIndices, unmatchedRightIndices, config)
	if err != nil {
		return nil, err
	}

	// Concatenate left join result with unmatched right rows
	return concatenateDataFrames(leftResult, unmatchedResult)
}

// crossJoin performs a cartesian product of two DataFrames
func crossJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftIndices := make([]int, 0, left.Height()*right.Height())
	rightIndices := make([]int, 0, left.Height()*right.Height())

	for i := 0; i < left.Height(); i++ {
		for j := 0; j < right.Height(); j++ {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, j)
		}
	}

	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// antiJoin returns rows from left that have no match in right
func antiJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftKeys, err := getJoinColumns(left, config.LeftOn)
	if err != nil {
		return nil, err
	}

	rightKeys, err := getJoinColumns(right, config.RightOn)
	if err != nil {
		return nil, err
	}

	// Build hash table on right side
	ht, err := compute.BuildHashTable(rightKeys)
	if err != nil {
		return nil, err
	}

	// Find unmatched left rows
	unmatchedIndices := make([]int, 0)
	for i := 0; i < left.Height(); i++ {
		matches := ht.Probe(leftKeys, i)
		if len(matches) == 0 {
			unmatchedIndices = append(unmatchedIndices, i)
		}
	}

	// Return only left columns for unmatched rows
	return left.Take(unmatchedIndices)
}

// semiJoin returns rows from left that have a match in right
func semiJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftKeys, err := getJoinColumns(left, config.LeftOn)
	if err != nil {
		return nil, err
	}

	rightKeys, err := getJoinColumns(right, config.RightOn)
	if err != nil {
		return nil, err
	}

	// Build hash table on right side
	ht, err := compute.BuildHashTable(rightKeys)
	if err != nil {
		return nil, err
	}

	// Find matched left rows (deduplicated)
	matchedIndices := make([]int, 0)
	seen := make(map[int]bool)

	for i := 0; i < left.Height(); i++ {
		matches := ht.Probe(leftKeys, i)
		if len(matches) > 0 && !seen[i] {
			matchedIndices = append(matchedIndices, i)
			seen[i] = true
		}
	}

	// Return only left columns for matched rows
	return left.Take(matchedIndices)
}

// buildJoinResult constructs the result DataFrame from join indices
func buildJoinResult(left, right *DataFrame, leftIndices, rightIndices []int, config JoinConfig) (*DataFrame, error) {
	if len(leftIndices) != len(rightIndices) {
		return nil, fmt.Errorf("internal error: index arrays must have same length")
	}

	resultColumns := make([]series.Series, 0)

	// Add left columns
	for _, col := range left.columns {
		newSeries, err := takeSeriesWithNulls(col, leftIndices)
		if err != nil {
			return nil, err
		}
		resultColumns = append(resultColumns, newSeries)
	}

	// Build set of right join columns to skip
	rightJoinCols := make(map[string]bool)
	for _, col := range config.RightOn {
		rightJoinCols[col] = true
	}

	// Add right columns (handle name conflicts)
	for _, col := range right.columns {
		// Skip join columns from right (already in left)
		if rightJoinCols[col.Name()] {
			continue
		}

		newSeries, err := takeSeriesWithNulls(col, rightIndices)
		if err != nil {
			return nil, err
		}

		// Handle column name conflicts
		colName := col.Name()
		if left.HasColumn(colName) {
			colName = colName + config.Suffix
		}
		
		// Rename the series
		newSeries = renameSeries(newSeries, colName)
		resultColumns = append(resultColumns, newSeries)
	}

	return NewDataFrame(resultColumns...)
}

// takeSeriesWithNulls takes values from a series using indices, with -1 meaning null
func takeSeriesWithNulls(s series.Series, indices []int) (series.Series, error) {
	// If all indices are valid (no -1), use regular Take
	hasNulls := false
	for _, idx := range indices {
		if idx < 0 {
			hasNulls = true
			break
		}
	}
	
	if !hasNulls {
		return s.Take(indices), nil
	}

	// Build values and validity arrays
	values := make([]interface{}, len(indices))
	validity := make([]bool, len(indices))
	
	for i, idx := range indices {
		if idx >= 0 {
			values[i] = s.Get(idx)
			validity[i] = s.IsValid(idx)
		} else {
			// idx < 0 means null
			values[i] = getZeroValue(s.DataType())
			validity[i] = false
		}
	}
	
	// Create new series with validity mask
	return createSeriesFromValues(s.Name(), values, validity, s.DataType()), nil
}

// getZeroValue returns the zero value for a data type
func getZeroValue(dtype datatypes.DataType) interface{} {
	switch dtype.(type) {
	case datatypes.Int8:
		return int8(0)
	case datatypes.Int16:
		return int16(0)
	case datatypes.Int32:
		return int32(0)
	case datatypes.Int64:
		return int64(0)
	case datatypes.UInt8:
		return uint8(0)
	case datatypes.UInt16:
		return uint16(0)
	case datatypes.UInt32:
		return uint32(0)
	case datatypes.UInt64:
		return uint64(0)
	case datatypes.Float32:
		return float32(0)
	case datatypes.Float64:
		return float64(0)
	case datatypes.String:
		return ""
	case datatypes.Boolean:
		return false
	case datatypes.Binary:
		return []byte{}
	default:
		return nil
	}
}

// createSeriesFromValues creates a series from interface values with validity
func createSeriesFromValues(name string, values []interface{}, validity []bool, dtype datatypes.DataType) series.Series {
	switch dtype := dtype.(type) {
	case datatypes.Int8:
		data := make([]int8, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(int8)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Int16:
		data := make([]int16, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(int16)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Int32:
		data := make([]int32, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(int32)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Int64:
		data := make([]int64, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(int64)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.UInt8:
		data := make([]uint8, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(uint8)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.UInt16:
		data := make([]uint16, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(uint16)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.UInt32:
		data := make([]uint32, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(uint32)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.UInt64:
		data := make([]uint64, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(uint64)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Float32:
		data := make([]float32, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(float32)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Float64:
		data := make([]float64, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(float64)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.String:
		data := make([]string, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(string)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Boolean:
		data := make([]bool, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.(bool)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	case datatypes.Binary:
		data := make([][]byte, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				data[i] = v.([]byte)
			}
		}
		return series.NewSeriesWithValidity(name, data, validity, dtype)
	default:
		// Fallback
		data := make([]int32, len(values))
		return series.NewSeriesWithValidity(name, data, validity, datatypes.Int32{})
	}
}

// renameSeries creates a new series with a different name
func renameSeries(s series.Series, newName string) series.Series {
	return s.Rename(newName)
}

// createNullSeries creates a series filled with nulls
func createNullSeries(name string, dtype datatypes.DataType, length int) series.Series {
	validity := make([]bool, length) // All false = all nulls
	
	switch dtype := dtype.(type) {
	case datatypes.Int8:
		return series.NewSeriesWithValidity(name, make([]int8, length), validity, dtype)
	case datatypes.Int16:
		return series.NewSeriesWithValidity(name, make([]int16, length), validity, dtype)
	case datatypes.Int32:
		return series.NewSeriesWithValidity(name, make([]int32, length), validity, dtype)
	case datatypes.Int64:
		return series.NewSeriesWithValidity(name, make([]int64, length), validity, dtype)
	case datatypes.UInt8:
		return series.NewSeriesWithValidity(name, make([]uint8, length), validity, dtype)
	case datatypes.UInt16:
		return series.NewSeriesWithValidity(name, make([]uint16, length), validity, dtype)
	case datatypes.UInt32:
		return series.NewSeriesWithValidity(name, make([]uint32, length), validity, dtype)
	case datatypes.UInt64:
		return series.NewSeriesWithValidity(name, make([]uint64, length), validity, dtype)
	case datatypes.Float32:
		return series.NewSeriesWithValidity(name, make([]float32, length), validity, dtype)
	case datatypes.Float64:
		return series.NewSeriesWithValidity(name, make([]float64, length), validity, dtype)
	case datatypes.String:
		return series.NewSeriesWithValidity(name, make([]string, length), validity, dtype)
	case datatypes.Boolean:
		return series.NewSeriesWithValidity(name, make([]bool, length), validity, dtype)
	case datatypes.Binary:
		return series.NewSeriesWithValidity(name, make([][]byte, length), validity, dtype)
	default:
		// Fallback to int32
		return series.NewSeriesWithValidity(name, make([]int32, length), validity, datatypes.Int32{})
	}
}

// concatenateDataFrames combines two DataFrames vertically
func concatenateDataFrames(df1, df2 *DataFrame) (*DataFrame, error) {
	if len(df1.columns) != len(df2.columns) {
		return nil, fmt.Errorf("dataframes must have same number of columns")
	}

	resultColumns := make([]series.Series, len(df1.columns))
	
	for i := range df1.columns {
		col1 := df1.columns[i]
		col2 := df2.columns[i]
		
		// Ensure columns have same name
		if col1.Name() != col2.Name() {
			return nil, fmt.Errorf("column names must match: %s != %s", col1.Name(), col2.Name())
		}
		
		// Ensure columns have same type
		if !col1.DataType().Equals(col2.DataType()) {
			return nil, fmt.Errorf("column types must match for %s", col1.Name())
		}
		
		// Concatenate the columns
		totalLen := col1.Len() + col2.Len()
		indices := make([]int, totalLen)
		
		// Add indices from first column
		for j := 0; j < col1.Len(); j++ {
			indices[j] = j
		}
		
		// Take from first column
		part1 := col1.Take(indices[:col1.Len()])
		
		// Add indices from second column
		for j := 0; j < col2.Len(); j++ {
			indices[col1.Len()+j] = j
		}
		
		// Take from second column
		part2 := col2.Take(indices[col1.Len():])
		
		// Combine values
		values := make([]interface{}, totalLen)
		validity := make([]bool, totalLen)
		
		for j := 0; j < col1.Len(); j++ {
			values[j] = part1.Get(j)
			validity[j] = part1.IsValid(j)
		}
		
		for j := 0; j < col2.Len(); j++ {
			values[col1.Len()+j] = part2.Get(j)
			validity[col1.Len()+j] = part2.IsValid(j)
		}
		
		// Create concatenated series
		resultColumns[i] = createSeriesFromValues(col1.Name(), values, validity, col1.DataType())
	}

	return NewDataFrame(resultColumns...)
}