package frame

import (
	"fmt"

	"github.com/davidpalaitis/golars/series"
)

// Stack reshapes a DataFrame from wide to long format by stacking specified columns.
// It's similar to melt but preserves multi-level structure.
func (df *DataFrame) Stack(columns ...string) (*DataFrame, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be specified for stacking")
	}
	
	// Get column indices
	columnNames := df.Columns()
	columnMap := make(map[string]int)
	for i, name := range columnNames {
		columnMap[name] = i
	}
	
	// Validate columns exist
	stackIndices := make([]int, 0, len(columns))
	for _, col := range columns {
		idx, exists := columnMap[col]
		if !exists {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
		stackIndices = append(stackIndices, idx)
	}
	
	// Get non-stack column indices
	idIndices := make([]int, 0)
	idNames := make([]string, 0)
	for i, name := range columnNames {
		isStack := false
		for _, stackIdx := range stackIndices {
			if i == stackIdx {
				isStack = true
				break
			}
		}
		if !isStack {
			idIndices = append(idIndices, i)
			idNames = append(idNames, name)
		}
	}
	
	// Create result
	numStackCols := len(stackIndices)
	originalRows := df.Height()
	newRows := originalRows * numStackCols
	
	resultColumns := make([]series.Series, 0)
	
	// Repeat ID columns
	for idx, idIdx := range idIndices {
		idName := idNames[idx]
		idCol := df.columns[idIdx]
		
		// Use the helper function from melt.go
		newSeries := createRepeatedSeries(idCol, idName, originalRows, numStackCols)
		resultColumns = append(resultColumns, newSeries)
	}
	
	// Create level column (column names)
	levelValues := make([]string, newRows)
	levelIdx := 0
	for i := 0; i < originalRows; i++ {
		for _, colName := range columns {
			levelValues[levelIdx] = colName
			levelIdx++
		}
	}
	resultColumns = append(resultColumns, series.NewStringSeries("level_1", levelValues))
	
	// Determine common type from stack columns
	var commonType = df.columns[stackIndices[0]].DataType()
	
	// Create value column data
	valueIndices := stackIndices
	valueSeries := createValueSeries(df, valueIndices, originalRows, "value", commonType)
	resultColumns = append(resultColumns, valueSeries)
	
	return NewDataFrame(resultColumns...)
}

// Unstack reshapes a DataFrame from long to wide format.
// It's the inverse of Stack operation.
func (df *DataFrame) Unstack(levelColumn string, fillValue interface{}) (*DataFrame, error) {
	// This is essentially a pivot operation
	// Find the value column (last non-level column)
	var valueCol string
	columns := df.Columns()
	for i := len(columns) - 1; i >= 0; i-- {
		col := columns[i]
		if col != levelColumn {
			valueCol = col
			break
		}
	}
	
	if valueCol == "" {
		return nil, fmt.Errorf("could not determine value column")
	}
	
	// Get index columns (all except level and value)
	indexCols := make([]string, 0)
	for _, col := range columns {
		if col != levelColumn && col != valueCol {
			indexCols = append(indexCols, col)
		}
	}
	
	// Use pivot to unstack
	return df.Pivot(PivotOptions{
		Index:     indexCols,
		Columns:   levelColumn,
		Values:    valueCol,
		AggFunc:   "first",
		FillValue: fillValue,
	})
}

// Transpose swaps rows and columns of a DataFrame.
// Column names become the index and index becomes column names.
func (df *DataFrame) Transpose() (*DataFrame, error) {
	if df.Height() == 0 {
		return nil, fmt.Errorf("cannot transpose empty DataFrame")
	}
	
	columnNames := df.Columns()
	resultColumns := make([]series.Series, 0)
	
	// The original column names become the first column
	resultColumns = append(resultColumns, series.NewStringSeries("index", columnNames))
	
	// Each row becomes a new column
	for i := 0; i < df.Height(); i++ {
		colName := fmt.Sprintf("row_%d", i)
		values := make([]string, len(columnNames))
		
		for j, col := range df.columns {
			// Convert all values to string for simplicity
			if !col.IsNull(i) {
				values[j] = fmt.Sprintf("%v", col.Get(i))
			} else {
				values[j] = ""
			}
		}
		
		// Use string type for now (could be improved with type detection)
		resultColumns = append(resultColumns, series.NewStringSeries(colName, values))
	}
	
	return NewDataFrame(resultColumns...)
}