package frame

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// MeltOptions configures the melt operation
type MeltOptions struct {
	IDVars       []string // Columns to use as ID variables
	ValueVars    []string // Columns to unpivot (empty means all non-ID columns)
	VariableName string   // Name for the variable column (default: "variable")
	ValueName    string   // Name for the value column (default: "value")
	IgnoreIndex  bool     // Whether to ignore the index
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

	// Validate ID columns exist
	idSet := make(map[string]bool)
	for _, name := range options.IDVars {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("id column %q not found", name)
		}
		idSet[name] = true
	}

	// Determine value variables
	var valueVars []string
	if len(options.ValueVars) == 0 {
		// Use all non-ID columns
		for _, col := range df.columns {
			if !idSet[col.Name()] {
				valueVars = append(valueVars, col.Name())
			}
		}
	} else {
		// Use specified value columns
		for _, name := range options.ValueVars {
			if !df.HasColumn(name) {
				return nil, fmt.Errorf("value column %q not found", name)
			}
			valueVars = append(valueVars, name)
		}
	}

	if len(valueVars) == 0 {
		return nil, fmt.Errorf("no value columns to melt")
	}

	// Calculate the size of the melted DataFrame
	originalRows := df.height
	numValueVars := len(valueVars)
	totalRows := originalRows * numValueVars

	// Create result columns
	var resultCols []series.Series

	// Row-major ordering: for each original row, emit one row per value variable
	// Result row index = r * numValueVars + v

	// Replicate ID columns
	for _, idName := range options.IDVars {
		idCol, _ := df.Column(idName)
		resultCols = append(resultCols, createRepeatedSeriesRowMajor(idCol, idName, originalRows, numValueVars))
	}

	// Create variable column (for each row, cycle through variable names)
	varNames := make([]string, totalRows)
	for r := 0; r < originalRows; r++ {
		for v, vName := range valueVars {
			varNames[r*numValueVars+v] = vName
		}
	}
	resultCols = append(resultCols, series.NewStringSeries(options.VariableName, varNames))

	// Determine common type for value column
	var commonType datatypes.DataType
	allSameType := true
	for i, vName := range valueVars {
		col, _ := df.Column(vName)
		if i == 0 {
			commonType = col.DataType()
		} else if col.DataType().String() != commonType.String() {
			allSameType = false
			break
		}
	}

	if !allSameType {
		// Different types: convert everything to string
		vals := make([]string, totalRows)
		validity := make([]bool, totalRows)
		for r := 0; r < originalRows; r++ {
			for v, vName := range valueVars {
				col, _ := df.Column(vName)
				idx := r*numValueVars + v
				if col.IsNull(r) {
					validity[idx] = false
				} else {
					vals[idx] = col.GetAsString(r)
					validity[idx] = true
				}
			}
		}
		resultCols = append(resultCols, series.NewSeriesWithValidity(options.ValueName, vals, validity, datatypes.String{}))
	} else {
		// Same type: preserve the original type
		valIndices := make([]int, len(valueVars))
		for i, vName := range valueVars {
			for j, col := range df.columns {
				if col.Name() == vName {
					valIndices[i] = j
					break
				}
			}
		}
		resultCols = append(resultCols, createValueSeriesRowMajor(df, valIndices, originalRows, options.ValueName, commonType))
	}

	return NewDataFrame(resultCols...)
}

// Unpivot is an alias for Melt
func (df *DataFrame) Unpivot(options MeltOptions) (*DataFrame, error) {
	return df.Melt(options)
}

// Helper function to create a repeated series (row-major: each row repeated numValueVars times)
func createRepeatedSeriesRowMajor(original series.Series, name string, originalRows, numValueVars int) series.Series {
	totalRows := originalRows * numValueVars
	vals := make([]interface{}, totalRows)
	validity := make([]bool, totalRows)

	for r := 0; r < originalRows; r++ {
		for v := 0; v < numValueVars; v++ {
			idx := r*numValueVars + v
			if original.IsNull(r) {
				validity[idx] = false
			} else {
				vals[idx] = original.Get(r)
				validity[idx] = true
			}
		}
	}

	return createSeriesFromInterface(name, vals, validity, original.DataType())
}

// Helper function to create the value series for melting (row-major order)
func createValueSeriesRowMajor(df *DataFrame, valueIndices []int, originalRows int, name string, dataType datatypes.DataType) series.Series {
	totalRows := originalRows * len(valueIndices)
	vals := make([]interface{}, totalRows)
	validity := make([]bool, totalRows)

	for r := 0; r < originalRows; r++ {
		for v, colIdx := range valueIndices {
			col := df.columns[colIdx]
			idx := r*len(valueIndices) + v
			if col.IsNull(r) {
				validity[idx] = false
			} else {
				vals[idx] = col.Get(r)
				validity[idx] = true
			}
		}
	}

	return createSeriesFromInterface(name, vals, validity, dataType)
}
