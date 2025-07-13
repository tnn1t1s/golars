package frame

import (
	"fmt"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// FillNullOptions configures null filling operations
type FillNullOptions struct {
	Value    interface{}      // Value to fill nulls with
	Method   string          // Method: "forward", "backward", "value"
	Limit    int             // Maximum number of consecutive nulls to fill
	Columns  []string        // Specific columns to fill (empty means all)
}

// FillNull fills null values in the DataFrame
func (df *DataFrame) FillNull(options FillNullOptions) (*DataFrame, error) {
	// Default to value method if not specified
	if options.Method == "" && options.Value != nil {
		options.Method = "value"
	}

	// Get columns to process
	columnsToFill := options.Columns
	if len(columnsToFill) == 0 {
		columnsToFill = df.Columns()
	}

	// Create result columns
	resultColumns := make([]series.Series, len(df.columns))
	
	// Process each column
	for i, col := range df.columns {
		colName := col.Name()
		
		// Check if this column should be filled
		shouldFill := false
		if len(options.Columns) == 0 {
			shouldFill = true
		} else {
			for _, name := range options.Columns {
				if name == colName {
					shouldFill = true
					break
				}
			}
		}
		
		if shouldFill {
			// Fill nulls based on method
			switch options.Method {
			case "forward", "ffill":
				resultColumns[i] = forwardFillSeries(col, options.Limit)
			case "backward", "bfill":
				resultColumns[i] = backwardFillSeries(col, options.Limit)
			case "value":
				resultColumns[i] = valueFillSeries(col, options.Value)
			default:
				return nil, fmt.Errorf("unknown fill method: %s", options.Method)
			}
		} else {
			// Keep column as is
			resultColumns[i] = col
		}
	}
	
	return NewDataFrame(resultColumns...)
}

// ForwardFill fills null values with the previous non-null value
func (df *DataFrame) ForwardFill(columns ...string) (*DataFrame, error) {
	return df.FillNull(FillNullOptions{
		Method:  "forward",
		Columns: columns,
	})
}

// BackwardFill fills null values with the next non-null value
func (df *DataFrame) BackwardFill(columns ...string) (*DataFrame, error) {
	return df.FillNull(FillNullOptions{
		Method:  "backward",
		Columns: columns,
	})
}

// Helper function to forward fill a series
func forwardFillSeries(s series.Series, limit int) series.Series {
	length := s.Len()
	
	// Collect values
	values := make([]interface{}, length)
	validity := make([]bool, length)
	
	var lastValidValue interface{}
	consecutiveNulls := 0
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			consecutiveNulls++
			if lastValidValue != nil && (limit <= 0 || consecutiveNulls <= limit) {
				values[i] = lastValidValue
				validity[i] = true
			} else {
				values[i] = nil
				validity[i] = false
			}
		} else {
			values[i] = s.Get(i)
			validity[i] = true
			lastValidValue = values[i]
			consecutiveNulls = 0
		}
	}
	
	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper function to backward fill a series
func backwardFillSeries(s series.Series, limit int) series.Series {
	length := s.Len()
	
	// Collect values
	values := make([]interface{}, length)
	validity := make([]bool, length)
	
	var nextValidValue interface{}
	consecutiveNulls := 0
	
	// Process from end to start
	for i := length - 1; i >= 0; i-- {
		if s.IsNull(i) {
			consecutiveNulls++
			if nextValidValue != nil && (limit <= 0 || consecutiveNulls <= limit) {
				values[i] = nextValidValue
				validity[i] = true
			} else {
				values[i] = nil
				validity[i] = false
			}
		} else {
			values[i] = s.Get(i)
			validity[i] = true
			nextValidValue = values[i]
			consecutiveNulls = 0
		}
	}
	
	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper function to fill with a specific value
func valueFillSeries(s series.Series, fillValue interface{}) series.Series {
	length := s.Len()
	
	// Collect values
	values := make([]interface{}, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			values[i] = fillValue
			validity[i] = true
		} else {
			values[i] = s.Get(i)
			validity[i] = true
		}
	}
	
	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper to create series from interface values
func createSeriesFromInterface(name string, values []interface{}, validity []bool, dataType datatypes.DataType) series.Series {
	// If validity is nil, assume all values are valid
	if validity == nil {
		validity = make([]bool, len(values))
		for i := range validity {
			validity[i] = true
		}
	}
	
	switch dataType.(type) {
	case datatypes.Int64:
		intVals := make([]int64, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				intVals[i] = v.(int64)
			}
		}
		// If any values are invalid, use NewSeriesWithValidity
		hasNulls := false
		for _, valid := range validity {
			if !valid {
				hasNulls = true
				break
			}
		}
		if hasNulls {
			return series.NewSeriesWithValidity(name, intVals, validity, datatypes.Int64{})
		}
		return series.NewInt64Series(name, intVals)
		
	case datatypes.Float64:
		floatVals := make([]float64, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				floatVals[i] = v.(float64)
			}
		}
		// If any values are invalid, use NewSeriesWithValidity
		hasNulls := false
		for _, valid := range validity {
			if !valid {
				hasNulls = true
				break
			}
		}
		if hasNulls {
			return series.NewSeriesWithValidity(name, floatVals, validity, datatypes.Float64{})
		}
		return series.NewFloat64Series(name, floatVals)
		
	case datatypes.String:
		strVals := make([]string, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				strVals[i] = v.(string)
			}
		}
		// If any values are invalid, use NewSeriesWithValidity
		hasNulls := false
		for _, valid := range validity {
			if !valid {
				hasNulls = true
				break
			}
		}
		if hasNulls {
			return series.NewSeriesWithValidity(name, strVals, validity, datatypes.String{})
		}
		return series.NewStringSeries(name, strVals)
		
	case datatypes.Boolean:
		boolVals := make([]bool, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				boolVals[i] = v.(bool)
			}
		}
		// If any values are invalid, use NewSeriesWithValidity
		hasNulls := false
		for _, valid := range validity {
			if !valid {
				hasNulls = true
				break
			}
		}
		if hasNulls {
			return series.NewSeriesWithValidity(name, boolVals, validity, datatypes.Boolean{})
		}
		return series.NewBooleanSeries(name, boolVals)
		
	default:
		// Convert to string
		strVals := make([]string, len(values))
		for i, v := range values {
			if validity[i] && v != nil {
				strVals[i] = fmt.Sprintf("%v", v)
			}
		}
		return series.NewStringSeries(name, strVals)
	}
}

// DropNull removes rows with null values
func (df *DataFrame) DropNull(subset ...string) (*DataFrame, error) {
	// Determine which columns to check
	checkColumns := subset
	if len(checkColumns) == 0 {
		checkColumns = df.Columns()
	}
	
	// Find column indices
	columnMap := make(map[string]int)
	for i, name := range df.Columns() {
		columnMap[name] = i
	}
	
	checkIndices := make([]int, 0, len(checkColumns))
	for _, name := range checkColumns {
		idx, exists := columnMap[name]
		if !exists {
			return nil, fmt.Errorf("column '%s' not found", name)
		}
		checkIndices = append(checkIndices, idx)
	}
	
	// Find rows to keep
	keepRows := make([]int, 0, df.Height())
	for i := 0; i < df.Height(); i++ {
		hasNull := false
		for _, colIdx := range checkIndices {
			if df.columns[colIdx].IsNull(i) {
				hasNull = true
				break
			}
		}
		if !hasNull {
			keepRows = append(keepRows, i)
		}
	}
	
	// Create new columns with only kept rows
	resultColumns := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		values := make([]interface{}, len(keepRows))
		validity := make([]bool, len(keepRows))
		for j, rowIdx := range keepRows {
			values[j] = col.Get(rowIdx)
			validity[j] = !col.IsNull(rowIdx)
		}
		resultColumns[i] = createSeriesFromInterface(col.Name(), values, validity, col.DataType())
	}
	
	return NewDataFrame(resultColumns...)
}

// DropDuplicates removes duplicate rows
type DropDuplicatesOptions struct {
	Subset []string // Columns to consider for duplicates (empty means all)
	Keep   string   // Which duplicate to keep: "first", "last", "none"
}

// DropDuplicates removes duplicate rows from the DataFrame
func (df *DataFrame) DropDuplicates(options DropDuplicatesOptions) (*DataFrame, error) {
	// Default to keeping first
	if options.Keep == "" {
		options.Keep = "first"
	}
	
	// Validate keep option
	if options.Keep != "first" && options.Keep != "last" && options.Keep != "none" {
		return nil, fmt.Errorf("invalid keep option: %s (must be 'first', 'last', or 'none')", options.Keep)
	}
	
	// Determine which columns to check
	checkColumns := options.Subset
	if len(checkColumns) == 0 {
		checkColumns = df.Columns()
	}
	
	// Find column indices
	columnMap := make(map[string]int)
	for i, name := range df.Columns() {
		columnMap[name] = i
	}
	
	checkIndices := make([]int, 0, len(checkColumns))
	for _, name := range checkColumns {
		idx, exists := columnMap[name]
		if !exists {
			return nil, fmt.Errorf("column '%s' not found", name)
		}
		checkIndices = append(checkIndices, idx)
	}
	
	// Track seen rows and which to keep
	seen := make(map[string][]int) // Maps row key to indices where it appears
	
	for i := 0; i < df.Height(); i++ {
		// Build key from subset columns
		keyParts := make([]interface{}, len(checkIndices))
		for j, colIdx := range checkIndices {
			if df.columns[colIdx].IsNull(i) {
				keyParts[j] = "<NULL>"
			} else {
				keyParts[j] = df.columns[colIdx].Get(i)
			}
		}
		key := fmt.Sprintf("%v", keyParts)
		
		if indices, exists := seen[key]; exists {
			seen[key] = append(indices, i)
		} else {
			seen[key] = []int{i}
		}
	}
	
	// Determine which rows to keep based on keep option
	keepRows := make([]int, 0, df.Height())
	
	for _, indices := range seen {
		switch options.Keep {
		case "first":
			keepRows = append(keepRows, indices[0])
		case "last":
			keepRows = append(keepRows, indices[len(indices)-1])
		case "none":
			// Only keep if not duplicated
			if len(indices) == 1 {
				keepRows = append(keepRows, indices[0])
			}
		}
	}
	
	// Sort keepRows to maintain order
	sort.Ints(keepRows)
	
	// Create new columns with only kept rows
	resultColumns := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		values := make([]interface{}, len(keepRows))
		validity := make([]bool, len(keepRows))
		for j, rowIdx := range keepRows {
			values[j] = col.Get(rowIdx)
			validity[j] = !col.IsNull(rowIdx)
		}
		resultColumns[i] = createSeriesFromInterface(col.Name(), values, validity, col.DataType())
	}
	
	return NewDataFrame(resultColumns...)
}

// Imports needed at the top
// import "sort"