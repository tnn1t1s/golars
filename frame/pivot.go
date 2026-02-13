package frame

import (
	"fmt"
	"strings"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// PivotOptions configures the pivot operation
type PivotOptions struct {
	Index     []string    // Columns to use as index
	Columns   string      // Column to pivot (becomes new column names)
	Values    string      // Column containing values
	AggFunc   string      // Aggregation function: "sum", "mean", "count", "first", "last", "min", "max"
	FillValue interface{} // Value to use for missing combinations
}

// Pivot reshapes data from long to wide format.
func (df *DataFrame) Pivot(options PivotOptions) (*DataFrame, error) {
	if options.Columns == "" {
		return nil, fmt.Errorf("pivot column must be specified")
	}
	if options.Values == "" {
		return nil, fmt.Errorf("values column must be specified")
	}
	if options.AggFunc == "" {
		options.AggFunc = "first"
	}

	// Validate columns exist
	pivotCol, err := df.Column(options.Columns)
	if err != nil {
		return nil, err
	}
	valCol, err := df.Column(options.Values)
	if err != nil {
		return nil, err
	}

	for _, name := range options.Index {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("index column %q not found", name)
		}
	}

	// Get unique values from pivot column
	uniquePivotVals := getUniqueValues(pivotCol)

	// Create index combinations
	indexGroups := make(map[string]int) // key -> row number
	var indexKeys []string

	if len(options.Index) == 0 {
		indexGroups[""] = 0
		indexKeys = append(indexKeys, "")
	} else {
		for row := 0; row < df.height; row++ {
			var parts []string
			for _, name := range options.Index {
				col, _ := df.Column(name)
				parts = append(parts, col.GetAsString(row))
			}
			key := strings.Join(parts, "\x00")
			if _, exists := indexGroups[key]; !exists {
				indexGroups[key] = len(indexKeys)
				indexKeys = append(indexKeys, key)
			}
		}
	}

	numRows := len(indexKeys)

	// Initialize result columns
	var resultCols []series.Series

	// Add index columns
	for _, idxName := range options.Index {
		idxCol, _ := df.Column(idxName)
		vals := make([]interface{}, numRows)
		validity := make([]bool, numRows)

		for row := 0; row < df.height; row++ {
			var parts []string
			for _, name := range options.Index {
				col, _ := df.Column(name)
				parts = append(parts, col.GetAsString(row))
			}
			key := strings.Join(parts, "\x00")
			rowIdx := indexGroups[key]
			if !validity[rowIdx] {
				if idxCol.IsNull(row) {
					vals[rowIdx] = nil
				} else {
					vals[rowIdx] = idxCol.Get(row)
				}
				validity[rowIdx] = true
			}
		}

		resultCols = append(resultCols, createSeriesFromInterface(idxName, vals, validity, idxCol.DataType()))
	}

	// Create pivot value columns
	for _, pv := range uniquePivotVals {
		pvStr := fmt.Sprintf("%v", pv)
		collected := make(map[int][]interface{})

		for row := 0; row < df.height; row++ {
			if pivotCol.IsNull(row) {
				continue
			}
			if pivotCol.GetAsString(row) == pvStr {
				var parts []string
				for _, name := range options.Index {
					col, _ := df.Column(name)
					parts = append(parts, col.GetAsString(row))
				}
				key := strings.Join(parts, "\x00")
				rowIdx := indexGroups[key]
				if !valCol.IsNull(row) {
					collected[rowIdx] = append(collected[rowIdx], valCol.Get(row))
				}
			}
		}

		// Aggregate values and create column
		vals := make([]interface{}, numRows)
		validity := make([]bool, numRows)
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if items, ok := collected[rowIdx]; ok && len(items) > 0 {
				vals[rowIdx] = aggregate(items, options.AggFunc, valCol.DataType())
				validity[rowIdx] = true
			} else if options.FillValue != nil {
				vals[rowIdx] = options.FillValue
				validity[rowIdx] = true
			}
		}

		resultCols = append(resultCols, createSeriesFromInterface(pvStr, vals, validity, valCol.DataType()))
	}

	return NewDataFrame(resultCols...)
}

// PivotTable creates a pivot table with aggregation
func (df *DataFrame) PivotTable(options PivotOptions) (*DataFrame, error) {
	return df.Pivot(options)
}

// Helper function to get unique values from a series
func getUniqueValues(s series.Series) []interface{} {
	seen := make(map[string]bool)
	var result []interface{}
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			continue
		}
		key := s.GetAsString(i)
		if !seen[key] {
			seen[key] = true
			result = append(result, s.Get(i))
		}
	}
	return result
}

// Helper function to aggregate values
func aggregate(values []interface{}, aggFunc string, dataType datatypes.DataType) interface{} {
	if len(values) == 0 {
		return nil
	}

	switch aggFunc {
	case "first":
		return values[0]
	case "last":
		return values[len(values)-1]
	case "count":
		return int64(len(values))
	case "sum":
		sum := 0.0
		for _, v := range values {
			sum += toFloat64Value(v)
		}
		return convertToOriginalType(sum, dataType)
	case "mean":
		sum := 0.0
		for _, v := range values {
			sum += toFloat64Value(v)
		}
		return sum / float64(len(values)) // mean always returns float64
	case "min":
		min := toFloat64Value(values[0])
		for _, v := range values[1:] {
			fv := toFloat64Value(v)
			if fv < min {
				min = fv
			}
		}
		return convertToOriginalType(min, dataType)
	case "max":
		max := toFloat64Value(values[0])
		for _, v := range values[1:] {
			fv := toFloat64Value(v)
			if fv > max {
				max = fv
			}
		}
		return convertToOriginalType(max, dataType)
	default:
		return values[0]
	}
}

// Helper functions for type conversion
func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	return toFloat64Value(v)
}
