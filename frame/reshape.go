package frame

import (
	"fmt"

	"github.com/tnn1t1s/golars/series"
)

// Stack reshapes a DataFrame from wide to long format by stacking specified columns.
// It's similar to melt but preserves multi-level structure.
func (df *DataFrame) Stack(columns ...string) (*DataFrame, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be specified for Stack")
	}

	// Validate columns exist
	stackSet := make(map[string]bool)
	for _, name := range columns {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("column %q not found", name)
		}
		stackSet[name] = true
	}

	// Get non-stack columns (ID columns)
	var idCols []string
	for _, col := range df.columns {
		if !stackSet[col.Name()] {
			idCols = append(idCols, col.Name())
		}
	}

	// Use Melt to perform the stack operation
	return df.Melt(MeltOptions{
		IDVars:       idCols,
		ValueVars:    columns,
		VariableName: "level_1",
		ValueName:    "value",
	})
}

// Unstack reshapes a DataFrame from long to wide format.
// It's the inverse of Stack operation.
func (df *DataFrame) Unstack(levelColumn string, fillValue interface{}) (*DataFrame, error) {
	if !df.HasColumn(levelColumn) {
		return nil, fmt.Errorf("level column %q not found", levelColumn)
	}

	// Find the value column (last non-level column)
	var valueCol string
	var indexCols []string
	for _, col := range df.columns {
		name := col.Name()
		if name == levelColumn {
			continue
		}
		valueCol = name
	}

	// Get index columns (all except level and value)
	for _, col := range df.columns {
		name := col.Name()
		if name != levelColumn && name != valueCol {
			indexCols = append(indexCols, name)
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
	if len(df.columns) == 0 {
		return nil, fmt.Errorf("cannot transpose an empty DataFrame")
	}

	// The original column names become the first column
	colNames := make([]string, len(df.columns))
	for i, col := range df.columns {
		colNames[i] = col.Name()
	}

	var resultCols []series.Series
	resultCols = append(resultCols, series.NewStringSeries("index", colNames))

	// Each row becomes a new column
	for row := 0; row < df.height; row++ {
		vals := make([]string, len(df.columns))
		for colIdx, col := range df.columns {
			if col.IsNull(row) {
				vals[colIdx] = "null"
			} else {
				vals[colIdx] = col.GetAsString(row)
			}
		}
		colName := fmt.Sprintf("row_%d", row)
		resultCols = append(resultCols, series.NewStringSeries(colName, vals))
	}

	return NewDataFrame(resultCols...)
}
