package golars

import (
	"fmt"
	"reflect"
	
	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
)

// DataFrameOption represents an option for DataFrame creation
type DataFrameOption func(*dataFrameConfig)

type dataFrameConfig struct {
	orient string   // "col" or "row"
	schema []string // column names
}

// WithOrient specifies the orientation of the data ("col" or "row")
func WithOrient(orient string) DataFrameOption {
	return func(cfg *dataFrameConfig) {
		cfg.orient = orient
	}
}

// WithSchema specifies column names for the DataFrame
func WithSchema(columns []string) DataFrameOption {
	return func(cfg *dataFrameConfig) {
		cfg.schema = columns
	}
}

// NewDataFrameAuto creates a new DataFrame with automatic type inference
// This is the main constructor that mimics Polars' pl.DataFrame()
// Accepts:
// - map[string]interface{}: column name to slice of values
// - []map[string]interface{}: list of records  
// - [][]interface{}: list of rows (requires WithSchema option)
func NewDataFrameAuto(data interface{}, options ...DataFrameOption) (*frame.DataFrame, error) {
	cfg := &dataFrameConfig{
		orient: "col", // default to column orientation
	}
	
	for _, opt := range options {
		opt(cfg)
	}
	
	switch d := data.(type) {
	case map[string]interface{}:
		return dataFrameFromMap(d)
	case []map[string]interface{}:
		return dataFrameFromRecords(d)
	case [][]interface{}:
		if len(cfg.schema) == 0 {
			return nil, fmt.Errorf("schema required for row-oriented data")
		}
		return dataFrameFromRows(d, cfg.schema, cfg.orient)
	case nil:
		// Empty DataFrame
		return frame.NewDataFrame()
	default:
		return nil, fmt.Errorf("unsupported data type for DataFrame: %T", data)
	}
}

// dataFrameFromMap creates a DataFrame from a map of column names to values
func dataFrameFromMap(data map[string]interface{}) (*frame.DataFrame, error) {
	if len(data) == 0 {
		return frame.NewDataFrame()
	}
	
	columns := make([]Series, 0, len(data))
	
	for name, values := range data {
		var s Series
		
		// Check if values is already a Series
		if existingSeries, ok := values.(Series); ok {
			s = existingSeries
		} else {
			// Convert values to []interface{} if needed
			interfaceValues, err := toInterfaceSlice(values)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			
			// Create series with type inference
			s, err = createSeriesWithInference(name, interfaceValues)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
		}
		
		columns = append(columns, s)
	}
	
	return frame.NewDataFrame(columns...)
}

// dataFrameFromRecords creates a DataFrame from a list of records
func dataFrameFromRecords(records []map[string]interface{}) (*frame.DataFrame, error) {
	if len(records) == 0 {
		return frame.NewDataFrame()
	}
	
	// Collect all unique column names
	columnSet := make(map[string]bool)
	for _, record := range records {
		for col := range record {
			columnSet[col] = true
		}
	}
	
	// Create column data
	columnData := make(map[string][]interface{})
	for col := range columnSet {
		columnData[col] = make([]interface{}, 0, len(records))
	}
	
	// Fill column data from records
	for _, record := range records {
		for col := range columnSet {
			if val, exists := record[col]; exists {
				columnData[col] = append(columnData[col], val)
			} else {
				columnData[col] = append(columnData[col], nil)
			}
		}
	}
	
	// Create DataFrame from column data
	resultData := make(map[string]interface{})
	for k, v := range columnData {
		resultData[k] = v
	}
	return dataFrameFromMap(resultData)
}

// dataFrameFromRows creates a DataFrame from row-oriented data
func dataFrameFromRows(rows [][]interface{}, schema []string, orient string) (*frame.DataFrame, error) {
	if len(rows) == 0 {
		// Create empty DataFrame with schema
		columns := make([]Series, len(schema))
		for i, name := range schema {
			s, _ := createSeriesWithInference(name, []interface{}{})
			columns[i] = s
		}
		return frame.NewDataFrame(columns...)
	}
	
	if orient == "row" {
		// Validate row lengths
		expectedLen := len(schema)
		for i, row := range rows {
			if len(row) != expectedLen {
				return nil, fmt.Errorf("row %d has %d values, expected %d", i, len(row), expectedLen)
			}
		}
		
		// Transpose to column-oriented data
		columnData := make(map[string][]interface{})
		for i, name := range schema {
			columnData[name] = make([]interface{}, len(rows))
			for j, row := range rows {
				columnData[name][j] = row[i]
			}
		}
		
		resultData := make(map[string]interface{})
		for k, v := range columnData {
			resultData[k] = v
		}
		return dataFrameFromMap(resultData)
	} else {
		// Column-oriented: each row is a column
		if len(rows) != len(schema) {
			return nil, fmt.Errorf("number of data rows (%d) doesn't match schema length (%d)", len(rows), len(schema))
		}
		
		columnData := make(map[string][]interface{})
		for i, name := range schema {
			columnData[name] = make([]interface{}, len(rows[i]))
			for j, val := range rows[i] {
				columnData[name][j] = val
			}
		}
		
		resultData := make(map[string]interface{})
		for k, v := range columnData {
			resultData[k] = v
		}
		return dataFrameFromMap(resultData)
	}
}

// toInterfaceSlice converts various slice types to []interface{}
func toInterfaceSlice(values interface{}) ([]interface{}, error) {
	if values == nil {
		return []interface{}{}, nil
	}
	
	// Check if already []interface{}
	if iface, ok := values.([]interface{}); ok {
		return iface, nil
	}
	
	// Use reflection for other slice types
	v := reflect.ValueOf(values)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected slice, got %T", values)
	}
	
	length := v.Len()
	result := make([]interface{}, length)
	
	for i := 0; i < length; i++ {
		result[i] = v.Index(i).Interface()
	}
	
	return result, nil
}

// createSeriesWithInference creates a series with automatic type inference
func createSeriesWithInference(name string, values []interface{}) (Series, error) {
	// Infer the data type
	dtype, err := inferType(values)
	if err != nil {
		return nil, fmt.Errorf("failed to infer type: %w", err)
	}
	
	// Convert values to the appropriate type
	typedValues, validity, err := convertToType(values, dtype)
	if err != nil {
		return nil, fmt.Errorf("failed to convert values: %w", err)
	}
	
	// Check if any values are null
	hasNulls := false
	for _, v := range validity {
		if !v {
			hasNulls = true
			break
		}
	}
	
	// Create the series based on the inferred type
	switch dt := dtype.(type) {
	case datatypes.Boolean:
		vals := typedValues.([]bool)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewBooleanSeries(name, vals), nil
		
	case datatypes.Int8:
		vals := typedValues.([]int8)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewInt8Series(name, vals), nil
		
	case datatypes.Int16:
		vals := typedValues.([]int16)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewInt16Series(name, vals), nil
		
	case datatypes.Int32:
		vals := typedValues.([]int32)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewInt32Series(name, vals), nil
		
	case datatypes.Int64:
		vals := typedValues.([]int64)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewInt64Series(name, vals), nil
		
	case datatypes.Float32:
		vals := typedValues.([]float32)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewFloat32Series(name, vals), nil
		
	case datatypes.Float64:
		vals := typedValues.([]float64)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewFloat64Series(name, vals), nil
		
	case datatypes.String:
		vals := typedValues.([]string)
		if hasNulls {
			return series.NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return series.NewStringSeries(name, vals), nil
		
	default:
		return nil, fmt.Errorf("unsupported data type for series creation: %v", dtype)
	}
}

// inferType analyzes a slice of interface{} values and determines the most appropriate data type
func inferType(values []interface{}) (datatypes.DataType, error) {
	if len(values) == 0 {
		return datatypes.Null{}, nil
	}
	
	// Track type occurrences
	typeCounts := make(map[reflect.Type]int)
	
	// First pass: count non-null types
	for _, v := range values {
		if v == nil {
			continue
		}
		typeCounts[reflect.TypeOf(v)]++
	}
	
	// If all values are null, return appropriate type
	if len(typeCounts) == 0 {
		return datatypes.Null{}, nil
	}
	
	// Find the most common type
	var dominantType reflect.Type
	maxCount := 0
	for t, count := range typeCounts {
		if count > maxCount {
			maxCount = count
			dominantType = t
		}
	}
	
	// Map Go types to Golars data types
	switch dominantType.Kind() {
	case reflect.Bool:
		return datatypes.Boolean{}, nil
	case reflect.Int:
		return datatypes.Int64{}, nil // Default to Int64 for int
	case reflect.Int8:
		return datatypes.Int8{}, nil
	case reflect.Int16:
		return datatypes.Int16{}, nil
	case reflect.Int32:
		return datatypes.Int32{}, nil
	case reflect.Int64:
		return datatypes.Int64{}, nil
	case reflect.Uint:
		return datatypes.UInt64{}, nil // Default to UInt64 for uint
	case reflect.Uint8:
		return datatypes.UInt8{}, nil
	case reflect.Uint16:
		return datatypes.UInt16{}, nil
	case reflect.Uint32:
		return datatypes.UInt32{}, nil
	case reflect.Uint64:
		return datatypes.UInt64{}, nil
	case reflect.Float32:
		return datatypes.Float32{}, nil
	case reflect.Float64:
		return datatypes.Float64{}, nil
	case reflect.String:
		return datatypes.String{}, nil
	case reflect.Slice:
		if dominantType.Elem().Kind() == reflect.Uint8 {
			return datatypes.Binary{}, nil
		}
	}
	
	// If we can't determine the type, check if all values can be converted to float64
	allNumeric := true
	for _, v := range values {
		if v == nil {
			continue
		}
		switch v.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			// numeric types are ok
		default:
			allNumeric = false
			break
		}
	}
	
	if allNumeric {
		return datatypes.Float64{}, nil
	}
	
	// Default to string type if nothing else matches
	return datatypes.String{}, nil
}

// convertToType converts a slice of interface{} to the specific type needed for series creation
func convertToType(values []interface{}, dtype datatypes.DataType) (interface{}, []bool, error) {
	validity := make([]bool, len(values))
	
	switch dtype.(type) {
	case datatypes.Boolean:
		result := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else if b, ok := v.(bool); ok {
				result[i] = b
				validity[i] = true
			} else {
				return nil, nil, fmt.Errorf("cannot convert %v to bool", v)
			}
		}
		return result, validity, nil
		
	case datatypes.Int8:
		result := make([]int8, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case int8:
					result[i] = val
				case int:
					result[i] = int8(val)
				case int32:
					result[i] = int8(val)
				case int64:
					result[i] = int8(val)
				case float64:
					result[i] = int8(val)
				default:
					return nil, nil, fmt.Errorf("cannot convert %v to int8", v)
				}
			}
		}
		return result, validity, nil
		
	case datatypes.Int16:
		result := make([]int16, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case int16:
					result[i] = val
				case int:
					result[i] = int16(val)
				case int8:
					result[i] = int16(val)
				case int32:
					result[i] = int16(val)
				case int64:
					result[i] = int16(val)
				case float64:
					result[i] = int16(val)
				default:
					return nil, nil, fmt.Errorf("cannot convert %v to int16", v)
				}
			}
		}
		return result, validity, nil
		
	case datatypes.Int32:
		result := make([]int32, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case int32:
					result[i] = val
				case int:
					result[i] = int32(val)
				case int8:
					result[i] = int32(val)
				case int16:
					result[i] = int32(val)
				case int64:
					result[i] = int32(val)
				case float64:
					result[i] = int32(val)
				default:
					return nil, nil, fmt.Errorf("cannot convert %v to int32", v)
				}
			}
		}
		return result, validity, nil
		
	case datatypes.Int64:
		result := make([]int64, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case int64:
					result[i] = val
				case int:
					result[i] = int64(val)
				case int8:
					result[i] = int64(val)
				case int16:
					result[i] = int64(val)
				case int32:
					result[i] = int64(val)
				case float64:
					result[i] = int64(val)
				default:
					return nil, nil, fmt.Errorf("cannot convert %v to int64", v)
				}
			}
		}
		return result, validity, nil
		
	case datatypes.Float32:
		result := make([]float32, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case float32:
					result[i] = val
				case float64:
					result[i] = float32(val)
				case int:
					result[i] = float32(val)
				case int8:
					result[i] = float32(val)
				case int16:
					result[i] = float32(val)
				case int32:
					result[i] = float32(val)
				case int64:
					result[i] = float32(val)
				default:
					return nil, nil, fmt.Errorf("cannot convert %v to float32", v)
				}
			}
		}
		return result, validity, nil
		
	case datatypes.Float64:
		result := make([]float64, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case float64:
					result[i] = val
				case float32:
					result[i] = float64(val)
				case int:
					result[i] = float64(val)
				case int8:
					result[i] = float64(val)
				case int16:
					result[i] = float64(val)
				case int32:
					result[i] = float64(val)
				case int64:
					result[i] = float64(val)
				default:
					return nil, nil, fmt.Errorf("cannot convert %v to float64", v)
				}
			}
		}
		return result, validity, nil
		
	case datatypes.String:
		result := make([]string, len(values))
		for i, v := range values {
			if v == nil {
				validity[i] = false
			} else {
				validity[i] = true
				switch val := v.(type) {
				case string:
					result[i] = val
				default:
					result[i] = fmt.Sprint(val)
				}
			}
		}
		return result, validity, nil
		
	default:
		return nil, nil, fmt.Errorf("unsupported data type: %v", dtype)
	}
}