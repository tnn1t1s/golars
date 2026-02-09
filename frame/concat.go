package frame

import (
	"fmt"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
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
		return nil, fmt.Errorf("no DataFrames provided")
	}

	if options.Join == "" {
		options.Join = "outer"
	}

	if options.Join != "inner" && options.Join != "outer" {
		return nil, fmt.Errorf("invalid join type: %s (expected 'inner' or 'outer')", options.Join)
	}

	if len(frames) == 1 {
		return frames[0].Clone(), nil
	}

	if options.Axis == 1 {
		return concatHorizontal(frames, options)
	}
	return concatVertical(frames, options)
}

// concatVertical concatenates DataFrames vertically (stacking rows)
func concatVertical(frames []*DataFrame, options ConcatOptions) (*DataFrame, error) {
	// Collect all unique column names in order
	colOrder := make([]string, 0)
	colSet := make(map[string]bool)
	colTypes := make(map[string]datatypes.DataType)

	for _, f := range frames {
		for _, col := range f.columns {
			if !colSet[col.Name()] {
				colSet[col.Name()] = true
				colOrder = append(colOrder, col.Name())
				colTypes[col.Name()] = col.DataType()
			}
		}
	}

	// Verify schema if requested
	if options.VerifySchema {
		for _, f := range frames[1:] {
			for _, col := range f.columns {
				if existing, ok := colTypes[col.Name()]; ok {
					if existing != col.DataType() {
						return nil, fmt.Errorf("data type mismatch for column %q: %v vs %v", col.Name(), existing, col.DataType())
					}
				}
			}
		}
	}

	// Filter columns based on join type
	var resultCols []string
	if options.Join == "inner" {
		// Only keep columns present in all DataFrames
		for _, name := range colOrder {
			inAll := true
			for _, f := range frames {
				if !f.HasColumn(name) {
					inAll = false
					break
				}
			}
			if inAll {
				resultCols = append(resultCols, name)
			}
		}
	} else {
		resultCols = colOrder
	}

	if options.Sort {
		sortStrings(resultCols)
	}

	// Build concatenated series for each column
	result := make([]series.Series, len(resultCols))
	for i, colName := range resultCols {
		var seriesList []series.Series
		for _, f := range frames {
			if f.HasColumn(colName) {
				col, _ := f.Column(colName)
				seriesList = append(seriesList, col)
			} else {
				// Create null series for missing columns
				seriesList = append(seriesList, createNullSeriesForConcat(colName, f.height, colTypes[colName]))
			}
		}
		result[i] = concatenateSeries(colName, seriesList)
	}

	return NewDataFrame(result...)
}

// concatHorizontal concatenates DataFrames horizontally (adding columns)
func concatHorizontal(frames []*DataFrame, options ConcatOptions) (*DataFrame, error) {
	// Verify all DataFrames have the same height
	height := frames[0].height
	for i, f := range frames[1:] {
		if f.height != height {
			return nil, fmt.Errorf("cannot concat frames with different heights: frame %d has height %d, expected %d", i+1, f.height, height)
		}
	}

	// Collect all columns, handling duplicates
	nameCount := make(map[string]int)
	var allCols []series.Series

	for _, f := range frames {
		for _, col := range f.columns {
			name := col.Name()
			if nameCount[name] > 0 {
				// Rename duplicate columns
				newName := fmt.Sprintf("%s_%d", name, nameCount[name])
				allCols = append(allCols, col.Rename(newName))
			} else {
				allCols = append(allCols, col)
			}
			nameCount[name]++
		}
	}

	return NewDataFrame(allCols...)
}

// Helper function to concatenate series vertically
func concatenateSeries(name string, seriesList []series.Series) series.Series {
	if len(seriesList) == 0 {
		return series.NewFloat64Series(name, nil)
	}
	if len(seriesList) == 1 {
		return seriesList[0].Rename(name)
	}

	// Get the data type from the first non-null series
	var dt datatypes.DataType
	for _, s := range seriesList {
		if s.Len() > 0 {
			dt = s.DataType()
			break
		}
	}
	if dt == nil {
		dt = datatypes.Float64{}
	}

	// Count total length
	totalLen := 0
	for _, s := range seriesList {
		totalLen += s.Len()
	}

	return concatenateSeriesGeneric(name, seriesList, dt)
}

// Generic series concatenation for other types
func concatenateSeriesGeneric(name string, seriesList []series.Series, dataType datatypes.DataType) series.Series {
	totalLen := 0
	for _, s := range seriesList {
		totalLen += s.Len()
	}

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

	return createSeriesFromInterface(name, values, validity, dataType)
}

// Helper to create a null series of specified type and length
func createNullSeriesForConcat(name string, length int, dataType datatypes.DataType) series.Series {
	validity := make([]bool, length) // All false

	switch dataType.(type) {
	case datatypes.Int64:
		return series.NewSeriesWithValidity(name, make([]int64, length), validity, dataType)
	case datatypes.Int32:
		return series.NewSeriesWithValidity(name, make([]int32, length), validity, dataType)
	case datatypes.Float64:
		return series.NewSeriesWithValidity(name, make([]float64, length), validity, dataType)
	case datatypes.Float32:
		return series.NewSeriesWithValidity(name, make([]float32, length), validity, dataType)
	case datatypes.String:
		return series.NewSeriesWithValidity(name, make([]string, length), validity, dataType)
	case datatypes.Boolean:
		return series.NewSeriesWithValidity(name, make([]bool, length), validity, dataType)
	default:
		return series.NewSeriesWithValidity(name, make([]float64, length), validity, datatypes.Float64{})
	}
}

// Helper to rename a series
func renameSeriesForConcat(s series.Series, newName string) series.Series {
	return s.Rename(newName)
}

// Helper to sort string slice
func sortStrings(s []string) {
	sort.Strings(s)
}
