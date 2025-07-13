package frame

import (
	"fmt"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// ConcatOptions configures concatenation operations
type ConcatOptions struct {
	Axis         int    // 0 for vertical (rows), 1 for horizontal (columns)
	Join         string // "inner" or "outer" for column alignment
	IgnoreIndex  bool   // Whether to ignore original indices
	Sort         bool   // Whether to sort columns in result
	VerifySchema bool   // Whether to verify matching schemas
}

// Concat concatenates multiple DataFrames
func Concat(frames []*DataFrame, options ConcatOptions) (*DataFrame, error) {
	if len(frames) == 0 {
		return nil, fmt.Errorf("no DataFrames provided for concatenation")
	}

	// Default options
	if options.Join == "" {
		options.Join = "outer"
	}

	// Validate join type
	if options.Join != "inner" && options.Join != "outer" {
		return nil, fmt.Errorf("join must be 'inner' or 'outer', got '%s'", options.Join)
	}

	// Single frame returns a copy
	if len(frames) == 1 {
		return frames[0], nil
	}

	if options.Axis == 0 {
		// Vertical concatenation (row-wise)
		return concatVertical(frames, options)
	} else if options.Axis == 1 {
		// Horizontal concatenation (column-wise)
		return concatHorizontal(frames, options)
	} else {
		return nil, fmt.Errorf("axis must be 0 or 1, got %d", options.Axis)
	}
}

// concatVertical concatenates DataFrames vertically (stacking rows)
func concatVertical(frames []*DataFrame, options ConcatOptions) (*DataFrame, error) {
	// Collect all unique column names
	columnSet := make(map[string]datatypes.DataType)
	columnOrder := make([]string, 0)
	
	for _, df := range frames {
		for _, col := range df.columns {
			if existingType, exists := columnSet[col.Name()]; exists {
				// Verify data types match
				if options.VerifySchema && !existingType.Equals(col.DataType()) {
					return nil, fmt.Errorf("data type mismatch for column '%s': %s vs %s",
						col.Name(), existingType.String(), col.DataType().String())
				}
			} else {
				columnSet[col.Name()] = col.DataType()
				columnOrder = append(columnOrder, col.Name())
			}
		}
	}

	// Filter columns based on join type
	finalColumns := make([]string, 0)
	if options.Join == "inner" {
		// Only keep columns present in all DataFrames
		for _, colName := range columnOrder {
			foundInAll := true
			for _, df := range frames {
				if !df.HasColumn(colName) {
					foundInAll = false
					break
				}
			}
			if foundInAll {
				finalColumns = append(finalColumns, colName)
			}
		}
	} else {
		// Keep all columns (outer join)
		finalColumns = columnOrder
	}

	if len(finalColumns) == 0 {
		return nil, fmt.Errorf("no common columns found for inner join")
	}

	// Sort columns if requested
	if options.Sort {
		sortStrings(finalColumns)
	}

	// Build concatenated series for each column
	resultColumns := make([]series.Series, len(finalColumns))
	
	for i, colName := range finalColumns {
		// Collect series from each DataFrame
		seriesList := make([]series.Series, 0, len(frames))
		totalRows := 0
		
		for _, df := range frames {
			if df.HasColumn(colName) {
				col, _ := df.Column(colName)
				seriesList = append(seriesList, col)
				totalRows += col.Len()
			} else {
				// Create null series for missing columns
				nullCount := df.Height()
				nullSeries := createNullSeriesForConcat(colName, nullCount, columnSet[colName])
				seriesList = append(seriesList, nullSeries)
				totalRows += nullCount
			}
		}
		
		// Concatenate series
		concatenated := concatenateSeries(colName, seriesList)
		resultColumns[i] = concatenated
	}

	return NewDataFrame(resultColumns...)
}

// concatHorizontal concatenates DataFrames horizontally (adding columns)
func concatHorizontal(frames []*DataFrame, options ConcatOptions) (*DataFrame, error) {
	// Verify all DataFrames have the same height
	height := frames[0].Height()
	for i, df := range frames[1:] {
		if df.Height() != height {
			return nil, fmt.Errorf("cannot concatenate DataFrames horizontally with different heights: %d vs %d (frame %d)",
				height, df.Height(), i+1)
		}
	}

	// Collect all columns
	allColumns := make([]series.Series, 0)
	columnNames := make(map[string]int) // Track duplicate column names
	
	for _, df := range frames {
		for _, col := range df.columns {
			colName := col.Name()
			
			// Handle duplicate column names
			if count, exists := columnNames[colName]; exists {
				// Rename duplicate columns
				newName := fmt.Sprintf("%s_%d", colName, count)
				// Create a copy with new name
				col = renameSeriesForConcat(col, newName)
				columnNames[colName] = count + 1
			} else {
				columnNames[colName] = 1
			}
			
			allColumns = append(allColumns, col)
		}
	}

	return NewDataFrame(allColumns...)
}

// Helper function to concatenate series vertically
func concatenateSeries(name string, seriesList []series.Series) series.Series {
	if len(seriesList) == 0 {
		return nil
	}
	
	// Get the data type from the first non-null series
	var dataType datatypes.DataType
	for _, s := range seriesList {
		if s != nil {
			dataType = s.DataType()
			break
		}
	}
	
	// Count total length
	totalLen := 0
	for _, s := range seriesList {
		totalLen += s.Len()
	}
	
	// Build concatenated data based on type
	switch dataType.(type) {
	case datatypes.Int64:
		values := make([]int64, 0, totalLen)
		validity := make([]bool, 0, totalLen)
		
		for _, s := range seriesList {
			for i := 0; i < s.Len(); i++ {
				if s.IsNull(i) {
					values = append(values, 0)
					validity = append(validity, false)
				} else {
					values = append(values, s.Get(i).(int64))
					validity = append(validity, true)
				}
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, dataType)
		
	case datatypes.Float64:
		values := make([]float64, 0, totalLen)
		validity := make([]bool, 0, totalLen)
		
		for _, s := range seriesList {
			for i := 0; i < s.Len(); i++ {
				if s.IsNull(i) {
					values = append(values, 0)
					validity = append(validity, false)
				} else {
					values = append(values, s.Get(i).(float64))
					validity = append(validity, true)
				}
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, dataType)
		
	case datatypes.String:
		values := make([]string, 0, totalLen)
		validity := make([]bool, 0, totalLen)
		
		for _, s := range seriesList {
			for i := 0; i < s.Len(); i++ {
				if s.IsNull(i) {
					values = append(values, "")
					validity = append(validity, false)
				} else {
					values = append(values, s.Get(i).(string))
					validity = append(validity, true)
				}
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, dataType)
		
	default:
		// Handle other types generically
		return concatenateSeriesGeneric(name, seriesList, dataType)
	}
}

// Generic series concatenation for other types
func concatenateSeriesGeneric(name string, seriesList []series.Series, dataType datatypes.DataType) series.Series {
	totalLen := 0
	for _, s := range seriesList {
		totalLen += s.Len()
	}
	
	// Use interface{} slice for generic handling
	values := make([]interface{}, 0, totalLen)
	validity := make([]bool, 0, totalLen)
	
	for _, s := range seriesList {
		for i := 0; i < s.Len(); i++ {
			if s.IsNull(i) {
				values = append(values, nil)
				validity = append(validity, false)
			} else {
				values = append(values, s.Get(i))
				validity = append(validity, true)
			}
		}
	}
	
	// Convert to appropriate type
	return createSeriesFromInterface(name, values, validity, dataType)
}

// Helper to create a null series of specified type and length
func createNullSeriesForConcat(name string, length int, dataType datatypes.DataType) series.Series {
	validity := make([]bool, length) // All false
	
	switch dataType.(type) {
	case datatypes.Int64:
		values := make([]int64, length)
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	case datatypes.Int32:
		values := make([]int32, length)
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	case datatypes.Float64:
		values := make([]float64, length)
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	case datatypes.Float32:
		values := make([]float32, length)
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	case datatypes.String:
		values := make([]string, length)
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	case datatypes.Boolean:
		values := make([]bool, length)
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	default:
		// For other types, create with nil values
		values := make([]interface{}, length)
		return createSeriesFromInterface(name, values, validity, dataType)
	}
}

// Helper to rename a series
func renameSeriesForConcat(s series.Series, newName string) series.Series {
	// Extract data and validity
	length := s.Len()
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		validity[i] = !s.IsNull(i)
	}
	
	// Create new series with same data but different name
	switch s.DataType().(type) {
	case datatypes.Int64:
		values := make([]int64, length)
		for i := 0; i < length; i++ {
			if !s.IsNull(i) {
				values[i] = s.Get(i).(int64)
			}
		}
		return series.NewSeriesWithValidity(newName, values, validity, s.DataType())
		
	case datatypes.Float64:
		values := make([]float64, length)
		for i := 0; i < length; i++ {
			if !s.IsNull(i) {
				values[i] = s.Get(i).(float64)
			}
		}
		return series.NewSeriesWithValidity(newName, values, validity, s.DataType())
		
	case datatypes.String:
		values := make([]string, length)
		for i := 0; i < length; i++ {
			if !s.IsNull(i) {
				values[i] = s.Get(i).(string)
			}
		}
		return series.NewSeriesWithValidity(newName, values, validity, s.DataType())
		
	default:
		// Generic handling
		values := make([]interface{}, length)
		for i := 0; i < length; i++ {
			if !s.IsNull(i) {
				values[i] = s.Get(i)
			}
		}
		return createSeriesFromInterface(newName, values, validity, s.DataType())
	}
}

// Helper to sort string slice
func sortStrings(s []string) {
	// Simple bubble sort for small slices
	n := len(s)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if s[j] > s[j+1] {
				s[j], s[j+1] = s[j+1], s[j]
			}
		}
	}
}