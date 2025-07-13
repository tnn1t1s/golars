package frame

import (
	"fmt"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// MeltOptions configures the melt operation
type MeltOptions struct {
	IDVars        []string // Columns to use as ID variables
	ValueVars     []string // Columns to unpivot (empty means all non-ID columns)
	VariableName  string   // Name for the variable column (default: "variable")
	ValueName     string   // Name for the value column (default: "value")
	IgnoreIndex   bool     // Whether to ignore the index
}

// Melt unpivots a DataFrame from wide to long format.
// ID columns are kept as identifier variables, while value columns are "melted" into two columns:
// one for the variable names and one for the values.
func (df *DataFrame) Melt(options MeltOptions) (*DataFrame, error) {
	// Set defaults
	if options.VariableName == "" {
		options.VariableName = "variable"
	}
	if options.ValueName == "" {
		options.ValueName = "value"
	}

	// Get columns by name
	columnNames := df.Columns()
	columnMap := make(map[string]int)
	for i, name := range columnNames {
		columnMap[name] = i
	}

	// Validate and collect ID columns
	idIndices := make([]int, 0, len(options.IDVars))
	idColsMap := make(map[string]bool)
	for _, idVar := range options.IDVars {
		idx, exists := columnMap[idVar]
		if !exists {
			return nil, fmt.Errorf("ID variable '%s' not found in DataFrame", idVar)
		}
		idIndices = append(idIndices, idx)
		idColsMap[idVar] = true
	}

	// Determine value variables
	valueIndices := make([]int, 0)
	valueNames := make([]string, 0)
	if len(options.ValueVars) == 0 {
		// Use all non-ID columns
		for i, name := range columnNames {
			if !idColsMap[name] {
				valueIndices = append(valueIndices, i)
				valueNames = append(valueNames, name)
			}
		}
	} else {
		// Use specified value columns
		for _, valueVar := range options.ValueVars {
			idx, exists := columnMap[valueVar]
			if !exists {
				return nil, fmt.Errorf("value variable '%s' not found in DataFrame", valueVar)
			}
			valueIndices = append(valueIndices, idx)
			valueNames = append(valueNames, valueVar)
		}
	}

	if len(valueIndices) == 0 {
		return nil, fmt.Errorf("no value columns to melt")
	}

	// Calculate the size of the melted DataFrame
	originalRows := df.Height()
	numValueCols := len(valueIndices)
	meltedRows := originalRows * numValueCols

	// Create result columns
	resultColumns := make([]series.Series, 0, len(idIndices)+2)

	// Replicate ID columns
	for _, idVar := range options.IDVars {
		idIdx := columnMap[idVar]
		idCol := df.columns[idIdx]
		
		// Create a new series by repeating values
		// We need to handle different data types
		newSeries := createRepeatedSeries(idCol, idVar, originalRows, numValueCols)
		resultColumns = append(resultColumns, newSeries)
	}

	// Create variable column
	varValues := make([]string, meltedRows)
	varIdx := 0
	for i := 0; i < originalRows; i++ {
		for _, varName := range valueNames {
			varValues[varIdx] = varName
			varIdx++
		}
	}
	resultColumns = append(resultColumns, 
		series.NewStringSeries(options.VariableName, varValues))

	// Create value column - need to determine common type
	var commonType datatypes.DataType
	firstType := true
	for _, idx := range valueIndices {
		col := df.columns[idx]
		if firstType {
			commonType = col.DataType()
			firstType = false
		} else {
			// Here we'd implement type promotion logic
			// For simplicity, if types differ, use string
			if commonType.String() != col.DataType().String() {
				commonType = datatypes.String{}
			}
		}
	}

	// Create value column data based on common type
	valueSeries := createValueSeries(df, valueIndices, originalRows, options.ValueName, commonType)
	resultColumns = append(resultColumns, valueSeries)

	return NewDataFrame(resultColumns...)
}

// Unpivot is an alias for Melt
func (df *DataFrame) Unpivot(options MeltOptions) (*DataFrame, error) {
	return df.Melt(options)
}

// Helper function to create a repeated series
func createRepeatedSeries(original series.Series, name string, originalRows, repeatCount int) series.Series {
	totalRows := originalRows * repeatCount
	
	// Handle different data types
	switch original.DataType().(type) {
	case datatypes.String:
		values := make([]string, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			val := ""
			if !original.IsNull(i) {
				val = original.Get(i).(string)
			}
			for j := 0; j < repeatCount; j++ {
				values[idx] = val
				idx++
			}
		}
		return series.NewStringSeries(name, values)
		
	case datatypes.Int64:
		values := make([]int64, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			val := int64(0)
			if !original.IsNull(i) {
				val = original.Get(i).(int64)
			}
			for j := 0; j < repeatCount; j++ {
				values[idx] = val
				idx++
			}
		}
		return series.NewInt64Series(name, values)
		
	case datatypes.Float64:
		values := make([]float64, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			val := float64(0)
			if !original.IsNull(i) {
				val = original.Get(i).(float64)
			}
			for j := 0; j < repeatCount; j++ {
				values[idx] = val
				idx++
			}
		}
		return series.NewFloat64Series(name, values)
		
	case datatypes.Boolean:
		values := make([]bool, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			val := false
			if !original.IsNull(i) {
				val = original.Get(i).(bool)
			}
			for j := 0; j < repeatCount; j++ {
				values[idx] = val
				idx++
			}
		}
		return series.NewBooleanSeries(name, values)
		
	default:
		// For other types, collect as interface{} and convert to string
		values := make([]string, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			val := ""
			if !original.IsNull(i) {
				val = fmt.Sprintf("%v", original.Get(i))
			}
			for j := 0; j < repeatCount; j++ {
				values[idx] = val
				idx++
			}
		}
		return series.NewStringSeries(name, values)
	}
}

// Helper function to create the value series for melting
func createValueSeries(df *DataFrame, valueIndices []int, originalRows int, name string, dataType datatypes.DataType) series.Series {
	totalRows := originalRows * len(valueIndices)
	
	switch dataType.(type) {
	case datatypes.Int64:
		values := make([]int64, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			for _, colIdx := range valueIndices {
				col := df.columns[colIdx]
				if !col.IsNull(i) {
					values[idx] = col.Get(i).(int64)
				}
				idx++
			}
		}
		return series.NewInt64Series(name, values)
		
	case datatypes.Float64:
		values := make([]float64, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			for _, colIdx := range valueIndices {
				col := df.columns[colIdx]
				if !col.IsNull(i) {
					values[idx] = col.Get(i).(float64)
				}
				idx++
			}
		}
		return series.NewFloat64Series(name, values)
		
	case datatypes.String:
		values := make([]string, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			for _, colIdx := range valueIndices {
				col := df.columns[colIdx]
				if !col.IsNull(i) {
					// Convert to string if needed
					switch v := col.Get(i).(type) {
					case string:
						values[idx] = v
					default:
						values[idx] = fmt.Sprintf("%v", v)
					}
				}
				idx++
			}
		}
		return series.NewStringSeries(name, values)
		
	default:
		// Default to string representation
		values := make([]string, totalRows)
		idx := 0
		for i := 0; i < originalRows; i++ {
			for _, colIdx := range valueIndices {
				col := df.columns[colIdx]
				if !col.IsNull(i) {
					values[idx] = fmt.Sprintf("%v", col.Get(i))
				}
				idx++
			}
		}
		return series.NewStringSeries(name, values)
	}
}