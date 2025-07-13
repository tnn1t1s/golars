package frame

import (
	"fmt"
	"math"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Mode calculates the mode (most frequent value) of each column
func (df *DataFrame) Mode(axis int, numeric bool, dropNaN bool) (*DataFrame, error) {
	// For now, only support axis=0 (column-wise mode)
	if axis != 0 {
		return nil, fmt.Errorf("only axis=0 is currently supported")
	}

	resultColumns := make([]series.Series, 0)

	for _, col := range df.columns {
		// Skip non-numeric columns if requested
		if numeric && !isNumericType(col.DataType()) {
			continue
		}

		// Calculate mode for this column
		modeValue := calculateMode(col, dropNaN)
		
		// Create a single-value series with the mode
		var modeSeries series.Series
		switch col.DataType().(type) {
		case datatypes.Float64:
			modeSeries = series.NewFloat64Series(col.Name(), []float64{modeValue.(float64)})
		case datatypes.Float32:
			modeSeries = series.NewFloat32Series(col.Name(), []float32{modeValue.(float32)})
		case datatypes.Int64:
			modeSeries = series.NewInt64Series(col.Name(), []int64{modeValue.(int64)})
		case datatypes.Int32:
			modeSeries = series.NewInt32Series(col.Name(), []int32{modeValue.(int32)})
		case datatypes.String:
			modeSeries = series.NewStringSeries(col.Name(), []string{modeValue.(string)})
		default:
			// For other types, try to handle common cases
			switch col.DataType().(type) {
			case datatypes.Int16:
				modeSeries = series.NewInt16Series(col.Name(), []int16{modeValue.(int16)})
			case datatypes.Int8:
				modeSeries = series.NewInt8Series(col.Name(), []int8{modeValue.(int8)})
			case datatypes.UInt64:
				modeSeries = series.NewUInt64Series(col.Name(), []uint64{modeValue.(uint64)})
			case datatypes.UInt32:
				modeSeries = series.NewUInt32Series(col.Name(), []uint32{modeValue.(uint32)})
			case datatypes.UInt16:
				modeSeries = series.NewUInt16Series(col.Name(), []uint16{modeValue.(uint16)})
			case datatypes.UInt8:
				modeSeries = series.NewUInt8Series(col.Name(), []uint8{modeValue.(uint8)})
			case datatypes.Boolean:
				modeSeries = series.NewBooleanSeries(col.Name(), []bool{modeValue.(bool)})
			default:
				// As a last resort, try float64
				modeSeries = series.NewFloat64Series(col.Name(), []float64{toFloat64Value(modeValue)})
			}
		}
		
		resultColumns = append(resultColumns, modeSeries)
	}

	if len(resultColumns) == 0 {
		return nil, fmt.Errorf("no columns to calculate mode")
	}

	return NewDataFrame(resultColumns...)
}

// Skew calculates the skewness of numeric columns
func (df *DataFrame) Skew(axis int, skipNA bool) (*DataFrame, error) {
	if axis != 0 {
		return nil, fmt.Errorf("only axis=0 is currently supported")
	}

	skewValues := make([]float64, 0)
	columnNames := make([]string, 0)

	for _, col := range df.columns {
		if !isNumericType(col.DataType()) {
			continue
		}

		skew, err := calculateSkewness(col, skipNA)
		if err != nil {
			// If we can't calculate skewness, use NaN
			skew = math.NaN()
		}

		skewValues = append(skewValues, skew)
		columnNames = append(columnNames, col.Name())
	}

	if len(skewValues) == 0 {
		return nil, fmt.Errorf("no numeric columns found")
	}

	// Create result DataFrame with one row
	resultColumns := []series.Series{
		series.NewStringSeries("statistic", []string{"skew"}),
	}

	for i, name := range columnNames {
		resultColumns = append(resultColumns, 
			series.NewFloat64Series(name, []float64{skewValues[i]}))
	}

	return NewDataFrame(resultColumns...)
}

// Kurtosis calculates the kurtosis of numeric columns
func (df *DataFrame) Kurtosis(axis int, skipNA bool) (*DataFrame, error) {
	if axis != 0 {
		return nil, fmt.Errorf("only axis=0 is currently supported")
	}

	kurtosisValues := make([]float64, 0)
	columnNames := make([]string, 0)

	for _, col := range df.columns {
		if !isNumericType(col.DataType()) {
			continue
		}

		kurt, err := calculateKurtosis(col, skipNA)
		if err != nil {
			// If we can't calculate kurtosis, use NaN
			kurt = math.NaN()
		}

		kurtosisValues = append(kurtosisValues, kurt)
		columnNames = append(columnNames, col.Name())
	}

	if len(kurtosisValues) == 0 {
		return nil, fmt.Errorf("no numeric columns found")
	}

	// Create result DataFrame with one row
	resultColumns := []series.Series{
		series.NewStringSeries("statistic", []string{"kurtosis"}),
	}

	for i, name := range columnNames {
		resultColumns = append(resultColumns, 
			series.NewFloat64Series(name, []float64{kurtosisValues[i]}))
	}

	return NewDataFrame(resultColumns...)
}

// Helper function to calculate mode
func calculateMode(s series.Series, dropNaN bool) interface{} {
	// Count frequency of each value
	counts := make(map[interface{}]int)
	
	for i := 0; i < s.Len(); i++ {
		if dropNaN && s.IsNull(i) {
			continue
		}
		
		val := s.Get(i)
		counts[val]++
	}
	
	if len(counts) == 0 {
		// Return null if no valid values
		return nil
	}
	
	// Find the value with maximum count
	var mode interface{}
	maxCount := 0
	
	for val, count := range counts {
		if count > maxCount {
			maxCount = count
			mode = val
		}
	}
	
	return mode
}

// Helper function to calculate skewness
func calculateSkewness(s series.Series, skipNA bool) (float64, error) {
	// Collect non-null values
	values := make([]float64, 0)
	for i := 0; i < s.Len(); i++ {
		if skipNA && s.IsNull(i) {
			continue
		}
		values = append(values, toFloat64Value(s.Get(i)))
	}

	n := len(values)
	if n < 3 {
		return 0, fmt.Errorf("need at least 3 values for skewness")
	}

	// Calculate mean
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(n)

	// Calculate moments
	var m2, m3 float64
	for _, v := range values {
		diff := v - mean
		m2 += diff * diff
		m3 += diff * diff * diff
	}
	m2 /= float64(n)
	m3 /= float64(n)

	// Calculate skewness
	if m2 == 0 {
		return 0, nil
	}

	skewness := m3 / math.Pow(m2, 1.5)
	
	// Apply bias correction (sample skewness)
	if n > 2 {
		skewness *= math.Sqrt(float64(n*(n-1))) / float64(n-2)
	}

	return skewness, nil
}

// Helper function to calculate kurtosis
func calculateKurtosis(s series.Series, skipNA bool) (float64, error) {
	// Collect non-null values
	values := make([]float64, 0)
	for i := 0; i < s.Len(); i++ {
		if skipNA && s.IsNull(i) {
			continue
		}
		values = append(values, toFloat64Value(s.Get(i)))
	}

	n := len(values)
	if n < 4 {
		return 0, fmt.Errorf("need at least 4 values for kurtosis")
	}

	// Calculate mean
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(n)

	// Calculate moments
	var m2, m4 float64
	for _, v := range values {
		diff := v - mean
		diff2 := diff * diff
		m2 += diff2
		m4 += diff2 * diff2
	}
	m2 /= float64(n)
	m4 /= float64(n)

	// Calculate kurtosis
	if m2 == 0 {
		return 0, nil
	}

	kurtosis := m4/(m2*m2) - 3.0 // Excess kurtosis (subtract 3 for normal distribution)

	// Apply bias correction (sample kurtosis)
	if n > 3 {
		kurtosis = float64(n-1) / float64((n-2)*(n-3)) * 
			((float64(n+1)*kurtosis + 6) * float64(n-1) / float64(n))
	}

	return kurtosis, nil
}

// ValueCounts returns a DataFrame with unique values and their counts
func (df *DataFrame) ValueCounts(columns []string, normalize bool, sort bool, ascending bool, dropNaN bool) (*DataFrame, error) {
	// If no columns specified, use all columns
	if len(columns) == 0 {
		columns = df.Columns()
	}

	// For single column, return simple value counts
	if len(columns) == 1 {
		col, err := df.Column(columns[0])
		if err != nil {
			return nil, err
		}

		return valueCountsSingle(col, normalize, sort, ascending, dropNaN)
	}

	// For multiple columns, we need to group by all columns and count
	// This is effectively a groupby with count aggregation
	return nil, fmt.Errorf("multi-column value_counts not yet implemented")
}

// Helper function for single column value counts
func valueCountsSingle(s series.Series, normalize bool, sortCounts bool, ascending bool, dropNaN bool) (*DataFrame, error) {
	// Count frequencies
	counts := make(map[interface{}]int)
	total := 0

	for i := 0; i < s.Len(); i++ {
		if dropNaN && s.IsNull(i) {
			continue
		}
		val := s.Get(i)
		counts[val]++
		total++
	}

	// Extract unique values and their counts
	uniqueValues := make([]interface{}, 0, len(counts))
	countValues := make([]float64, 0, len(counts))

	for val, count := range counts {
		uniqueValues = append(uniqueValues, val)
		if normalize {
			countValues = append(countValues, float64(count)/float64(total))
		} else {
			countValues = append(countValues, float64(count))
		}
	}

	// Sort if requested
	if sortCounts {
		// Create indices for sorting
		indices := make([]int, len(countValues))
		for i := range indices {
			indices[i] = i
		}

		// Sort indices based on counts
		sort.Slice(indices, func(i, j int) bool {
			if ascending {
				return countValues[indices[i]] < countValues[indices[j]]
			}
			return countValues[indices[i]] > countValues[indices[j]]
		})

		// Reorder based on sorted indices
		sortedValues := make([]interface{}, len(uniqueValues))
		sortedCounts := make([]float64, len(countValues))
		for i, idx := range indices {
			sortedValues[i] = uniqueValues[idx]
			sortedCounts[i] = countValues[idx]
		}
		uniqueValues = sortedValues
		countValues = sortedCounts
	}

	// Create result DataFrame
	valueSeries := createSeriesFromInterface(s.Name(), uniqueValues, nil, s.DataType())
	countSeries := series.NewFloat64Series("count", countValues)

	return NewDataFrame(valueSeries, countSeries)
}

// NUnique returns the number of unique values in each column
func (df *DataFrame) NUnique(axis int, dropNaN bool) (*DataFrame, error) {
	if axis != 0 {
		return nil, fmt.Errorf("only axis=0 is currently supported")
	}

	columnNames := df.Columns()
	uniqueCounts := make([]int64, len(columnNames))

	for i, col := range df.columns {
		uniqueCounts[i] = countUnique(col, dropNaN)
	}

	// Create result DataFrame with one row
	resultColumns := []series.Series{
		series.NewStringSeries("statistic", []string{"n_unique"}),
	}

	for i, name := range columnNames {
		resultColumns = append(resultColumns, 
			series.NewInt64Series(name, []int64{uniqueCounts[i]}))
	}

	return NewDataFrame(resultColumns...)
}

// Helper function to count unique values
func countUnique(s series.Series, dropNaN bool) int64 {
	seen := make(map[interface{}]bool)
	
	for i := 0; i < s.Len(); i++ {
		if dropNaN && s.IsNull(i) {
			continue
		}
		val := s.Get(i)
		seen[val] = true
	}
	
	return int64(len(seen))
}

// RankOptions configures rank calculations
type RankOptions struct {
	Method    string   // Method: "average", "min", "max", "dense", "ordinal"
	Ascending bool     // Sort ascending (true) or descending (false)
	NaOption  string   // How to handle NaN: "keep", "top", "bottom"
	Pct       bool     // Whether to return percentile ranks
	Columns   []string // Specific columns to rank
}

// Rank assigns ranks to entries
func (df *DataFrame) Rank(options RankOptions) (*DataFrame, error) {
	// Set defaults
	if options.Method == "" {
		options.Method = "average"
	}
	if options.NaOption == "" {
		options.NaOption = "keep"
	}

	// Validate method
	validMethods := map[string]bool{
		"average": true, "min": true, "max": true, "dense": true, "ordinal": true,
	}
	if !validMethods[options.Method] {
		return nil, fmt.Errorf("invalid rank method: %s", options.Method)
	}

	// Get columns to rank
	columnsToRank := options.Columns
	if len(columnsToRank) == 0 {
		// Default to all numeric columns
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				columnsToRank = append(columnsToRank, col.Name())
			}
		}
	}

	// Create result columns
	resultColumns := make([]series.Series, len(df.columns))
	
	for i, col := range df.columns {
		// Check if this column should be ranked
		shouldRank := false
		for _, name := range columnsToRank {
			if col.Name() == name {
				shouldRank = true
				break
			}
		}

		if shouldRank {
			// Calculate ranks for this column
			rankedSeries := rankSeries(col, options)
			resultColumns[i] = rankedSeries
		} else {
			// Keep column as is
			resultColumns[i] = col
		}
	}

	return NewDataFrame(resultColumns...)
}

// Helper function to rank a single series
func rankSeries(s series.Series, options RankOptions) series.Series {
	length := s.Len()
	
	// Create index-value pairs for sorting
	type indexValue struct {
		index int
		value float64
		isNull bool
	}
	
	pairs := make([]indexValue, length)
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			pairs[i] = indexValue{index: i, value: 0, isNull: true}
		} else {
			pairs[i] = indexValue{index: i, value: toFloat64Value(s.Get(i)), isNull: false}
		}
	}
	
	// Sort pairs based on value and null handling
	sort.Slice(pairs, func(i, j int) bool {
		// Handle nulls based on NaOption
		if pairs[i].isNull && pairs[j].isNull {
			return false // Nulls are equal
		}
		if pairs[i].isNull {
			return options.NaOption == "top"
		}
		if pairs[j].isNull {
			return options.NaOption != "top"
		}
		
		// Both non-null, sort by value
		if options.Ascending {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].value > pairs[j].value
	})
	
	// Assign ranks based on method
	ranks := make([]float64, length)
	validity := make([]bool, length)
	
	switch options.Method {
	case "average":
		// Average rank for ties
		i := 0
		for i < len(pairs) {
			if pairs[i].isNull && options.NaOption == "keep" {
				validity[pairs[i].index] = false
				i++
				continue
			}
			
			// Find all elements with the same value
			j := i
			for j < len(pairs) && !pairs[j].isNull && 
				(j == i || pairs[j].value == pairs[i].value) {
				j++
			}
			
			// Calculate average rank
			avgRank := float64(i+j+1) / 2.0
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = avgRank
				validity[pairs[k].index] = true
			}
			
			i = j
		}
		
	case "min":
		// Minimum rank for ties
		i := 0
		for i < len(pairs) {
			if pairs[i].isNull && options.NaOption == "keep" {
				validity[pairs[i].index] = false
				i++
				continue
			}
			
			// Find all elements with the same value
			j := i
			for j < len(pairs) && !pairs[j].isNull && 
				(j == i || pairs[j].value == pairs[i].value) {
				j++
			}
			
			// Assign minimum rank
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = float64(i + 1)
				validity[pairs[k].index] = true
			}
			
			i = j
		}
		
	case "max":
		// Maximum rank for ties
		i := 0
		for i < len(pairs) {
			if pairs[i].isNull && options.NaOption == "keep" {
				validity[pairs[i].index] = false
				i++
				continue
			}
			
			// Find all elements with the same value
			j := i
			for j < len(pairs) && !pairs[j].isNull && 
				(j == i || pairs[j].value == pairs[i].value) {
				j++
			}
			
			// Assign maximum rank
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = float64(j)
				validity[pairs[k].index] = true
			}
			
			i = j
		}
		
	case "dense":
		// Dense ranking (no gaps)
		rank := 1.0
		lastValue := math.NaN()
		
		for _, pair := range pairs {
			if pair.isNull && options.NaOption == "keep" {
				validity[pair.index] = false
				continue
			}
			
			if !pair.isNull && pair.value != lastValue {
				if !math.IsNaN(lastValue) {
					rank++
				}
				lastValue = pair.value
			}
			
			ranks[pair.index] = rank
			validity[pair.index] = true
		}
		
	case "ordinal":
		// No ties, each value gets unique rank
		rank := 1.0
		for _, pair := range pairs {
			if pair.isNull && options.NaOption == "keep" {
				validity[pair.index] = false
				continue
			}
			
			ranks[pair.index] = rank
			validity[pair.index] = true
			rank++
		}
	}
	
	// Convert to percentile ranks if requested
	if options.Pct {
		validCount := 0
		for _, v := range validity {
			if v {
				validCount++
			}
		}
		
		if validCount > 0 {
			for i := range ranks {
				if validity[i] {
					ranks[i] = ranks[i] / float64(validCount)
				}
			}
		}
	}
	
	return series.NewSeriesWithValidity(s.Name(), ranks, validity, datatypes.Float64{})
}