package frame

import (
	"fmt"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// PivotOptions configures the pivot operation
type PivotOptions struct {
	Index     []string // Columns to use as index
	Columns   string   // Column to pivot (becomes new column names)
	Values    string   // Column containing values
	AggFunc   string   // Aggregation function: "sum", "mean", "count", "first", "last", "min", "max"
	FillValue interface{} // Value to use for missing combinations
}

// Pivot reshapes data from long to wide format.
// Similar to pandas pivot and Excel pivot tables.
func (df *DataFrame) Pivot(options PivotOptions) (*DataFrame, error) {
	// Validate inputs
	if options.Columns == "" {
		return nil, fmt.Errorf("columns parameter is required")
	}
	if options.Values == "" {
		return nil, fmt.Errorf("values parameter is required")
	}
	if options.AggFunc == "" {
		options.AggFunc = "first" // Default aggregation
	}

	// Get column indices
	columnNames := df.Columns()
	columnMap := make(map[string]int)
	for i, name := range columnNames {
		columnMap[name] = i
	}

	// Validate columns exist
	pivotColIdx, exists := columnMap[options.Columns]
	if !exists {
		return nil, fmt.Errorf("pivot column '%s' not found", options.Columns)
	}
	pivotCol := df.columns[pivotColIdx]
	
	valueColIdx, exists := columnMap[options.Values]
	if !exists {
		return nil, fmt.Errorf("value column '%s' not found", options.Values)
	}
	valueCol := df.columns[valueColIdx]

	// Validate index columns
	indexIndices := make([]int, 0, len(options.Index))
	for _, idxName := range options.Index {
		idx, exists := columnMap[idxName]
		if !exists {
			return nil, fmt.Errorf("index column '%s' not found", idxName)
		}
		indexIndices = append(indexIndices, idx)
	}

	// Get unique values from pivot column (these become new column names)
	uniquePivotValues := getUniqueValues(pivotCol)
	if len(uniquePivotValues) == 0 {
		return nil, fmt.Errorf("no unique values found in pivot column")
	}

	// Create index combinations
	type indexKey string
	indexMap := make(map[indexKey]int) // Maps index combination to row number
	indexValues := make([][]interface{}, 0)
	
	if len(options.Index) == 0 {
		// No index columns, just one row
		indexMap[indexKey("")] = 0
		indexValues = append(indexValues, []interface{}{})
	} else {
		// Get unique combinations of index values
		for i := 0; i < df.height; i++ {
			// Build key from index column values
			keyParts := make([]interface{}, len(options.Index))
			for j, idxIdx := range indexIndices {
				keyParts[j] = df.columns[idxIdx].Get(i)
			}
			key := fmt.Sprintf("%v", keyParts)
			
			if _, exists := indexMap[indexKey(key)]; !exists {
				indexMap[indexKey(key)] = len(indexValues)
				indexValues = append(indexValues, keyParts)
			}
		}
	}

	// Initialize result columns
	resultColumns := make([]series.Series, 0)
	
	// Add index columns to result
	for i, idxName := range options.Index {
		idxIdx := columnMap[idxName]
		idxCol := df.columns[idxIdx]
		values := make([]interface{}, len(indexValues))
		validity := make([]bool, len(indexValues))
		for j, idxVals := range indexValues {
			values[j] = idxVals[i]
			validity[j] = true
		}
		resultColumns = append(resultColumns, createSeriesFromValues(idxName, values, validity, idxCol.DataType()))
	}

	// Create pivot value columns
	for _, pivotVal := range uniquePivotValues {
		// Collect values for this pivot value grouped by index
		groupedValues := make(map[indexKey][]interface{})
		
		for i := 0; i < df.height; i++ {
			if pivotCol.Get(i) == pivotVal && !valueCol.IsNull(i) {
				// Build index key
				var key indexKey
				if len(options.Index) == 0 {
					key = ""
				} else {
					keyParts := make([]interface{}, len(options.Index))
					for j, idxIdx := range indexIndices {
						keyParts[j] = df.columns[idxIdx].Get(i)
					}
					key = indexKey(fmt.Sprintf("%v", keyParts))
				}
				
				if _, exists := groupedValues[key]; !exists {
					groupedValues[key] = make([]interface{}, 0)
				}
				groupedValues[key] = append(groupedValues[key], valueCol.Get(i))
			}
		}
		
		// Aggregate values and create column
		colName := fmt.Sprintf("%v", pivotVal)
		values := make([]interface{}, len(indexValues))
		validity := make([]bool, len(indexValues))
		
		for key, rowIdx := range indexMap {
			if vals, exists := groupedValues[key]; exists && len(vals) > 0 {
				aggValue := aggregate(vals, options.AggFunc, valueCol.DataType())
				values[rowIdx] = aggValue
				validity[rowIdx] = true
			} else {
				if options.FillValue != nil {
					values[rowIdx] = options.FillValue
					validity[rowIdx] = true
				} else {
					values[rowIdx] = nil
					validity[rowIdx] = false
				}
			}
		}
		
		resultColumns = append(resultColumns, createSeriesFromValues(colName, values, validity, valueCol.DataType()))
	}

	return NewDataFrame(resultColumns...)
}

// PivotTable creates a pivot table with aggregation
// This is a convenience wrapper around Pivot with additional features
func (df *DataFrame) PivotTable(options PivotOptions) (*DataFrame, error) {
	// PivotTable is essentially the same as Pivot but with more emphasis on aggregation
	// For now, we'll just call Pivot
	return df.Pivot(options)
}

// Helper function to get unique values from a series
func getUniqueValues(s series.Series) []interface{} {
	seen := make(map[string]bool)
	unique := make([]interface{}, 0)
	
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			val := s.Get(i)
			key := fmt.Sprintf("%v", val)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, val)
			}
		}
	}
	
	return unique
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
		// Type-specific sum
		switch dataType.(type) {
		case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64:
			sum := int64(0)
			for _, v := range values {
				if v != nil {
					sum += toInt64(v)
				}
			}
			return sum
		case datatypes.Float32, datatypes.Float64:
			sum := float64(0)
			for _, v := range values {
				if v != nil {
					sum += toFloat64(v)
				}
			}
			return sum
		default:
			return values[0] // Can't sum non-numeric types
		}
		
	case "mean", "avg":
		// Type-specific mean
		switch dataType.(type) {
		case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64, 
		     datatypes.Float32, datatypes.Float64:
			sum := float64(0)
			count := 0
			for _, v := range values {
				if v != nil {
					sum += toFloat64(v)
					count++
				}
			}
			if count > 0 {
				return sum / float64(count)
			}
			return nil
		default:
			return values[0] // Can't average non-numeric types
		}
		
	case "min":
		// Type-specific min
		switch dataType.(type) {
		case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64:
			min := toInt64(values[0])
			for _, v := range values[1:] {
				if v != nil {
					val := toInt64(v)
					if val < min {
						min = val
					}
				}
			}
			return min
		case datatypes.Float32, datatypes.Float64:
			min := toFloat64(values[0])
			for _, v := range values[1:] {
				if v != nil {
					val := toFloat64(v)
					if val < min {
						min = val
					}
				}
			}
			return min
		default:
			return values[0]
		}
		
	case "max":
		// Type-specific max
		switch dataType.(type) {
		case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64:
			max := toInt64(values[0])
			for _, v := range values[1:] {
				if v != nil {
					val := toInt64(v)
					if val > max {
						max = val
					}
				}
			}
			return max
		case datatypes.Float32, datatypes.Float64:
			max := toFloat64(values[0])
			for _, v := range values[1:] {
				if v != nil {
					val := toFloat64(v)
					if val > max {
						max = val
					}
				}
			}
			return max
		default:
			return values[0]
		}
		
	default:
		return values[0]
	}
}

// Helper functions for type conversion
func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float32:
		return float64(val)
	case float64:
		return val
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}

