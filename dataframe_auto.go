package golars

import (
	"fmt"
	"reflect"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
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
	cfg := &dataFrameConfig{orient: "col"}
	for _, opt := range options {
		opt(cfg)
	}

	if data == nil {
		return frame.NewDataFrame()
	}

	switch d := data.(type) {
	case map[string]interface{}:
		return dataFrameFromMap(d)
	case []map[string]interface{}:
		return dataFrameFromRecords(d)
	case [][]interface{}:
		return dataFrameFromRows(d, cfg.schema, cfg.orient)
	default:
		return nil, fmt.Errorf("unsupported data type: %T", data)
	}
}

// dataFrameFromMap creates a DataFrame from a map of column names to values
func dataFrameFromMap(data map[string]interface{}) (*frame.DataFrame, error) {
	if len(data) == 0 {
		return frame.NewDataFrame()
	}

	var cols []series.Series
	for name, values := range data {
		// Check if values is already a Series
		if s, ok := values.(series.Series); ok {
			cols = append(cols, s.Rename(name))
			continue
		}

		// Convert values to []interface{} if needed
		iface, err := toInterfaceSlice(values)
		if err != nil {
			// Try creating from typed slice directly
			s, serr := createSeriesFromTypedSlice(name, values, nil)
			if serr != nil {
				return nil, fmt.Errorf("column %q: %w", name, err)
			}
			cols = append(cols, s)
			continue
		}

		s, err := createSeriesWithInference(name, iface)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", name, err)
		}
		cols = append(cols, s)
	}
	return frame.NewDataFrame(cols...)
}

// dataFrameFromRecords creates a DataFrame from a list of records
func dataFrameFromRecords(records []map[string]interface{}) (*frame.DataFrame, error) {
	if len(records) == 0 {
		return frame.NewDataFrame()
	}

	// Collect all unique column names in order
	colOrder := make([]string, 0)
	colSet := make(map[string]bool)
	for _, rec := range records {
		for name := range rec {
			if !colSet[name] {
				colSet[name] = true
				colOrder = append(colOrder, name)
			}
		}
	}

	// Create column data
	colData := make(map[string][]interface{})
	colValidity := make(map[string][]bool)
	for _, name := range colOrder {
		colData[name] = make([]interface{}, len(records))
		colValidity[name] = make([]bool, len(records))
	}

	// Fill column data from records
	for i, rec := range records {
		for _, name := range colOrder {
			if val, ok := rec[name]; ok {
				colData[name][i] = val
				colValidity[name][i] = true
			}
		}
	}

	// Create DataFrame from column data
	var cols []series.Series
	for _, name := range colOrder {
		s, err := createSeriesWithInference(name, colData[name])
		if err != nil {
			return nil, err
		}
		cols = append(cols, s)
	}
	return frame.NewDataFrame(cols...)
}

// dataFrameFromRows creates a DataFrame from row-oriented data
func dataFrameFromRows(rows [][]interface{}, schema []string, orient string) (*frame.DataFrame, error) {
	if len(rows) == 0 {
		return frame.NewDataFrame()
	}

	if orient == "col" {
		// Column-oriented: each row is a column
		if schema == nil {
			schema = make([]string, len(rows))
			for i := range schema {
				schema[i] = fmt.Sprintf("column_%d", i)
			}
		}
		var cols []series.Series
		for i, row := range rows {
			name := schema[i]
			s, err := createSeriesWithInference(name, row)
			if err != nil {
				return nil, err
			}
			cols = append(cols, s)
		}
		return frame.NewDataFrame(cols...)
	}

	// Row-oriented: transpose to column-oriented
	if schema == nil {
		return nil, fmt.Errorf("schema required for row-oriented data")
	}

	numCols := len(schema)
	colData := make([][]interface{}, numCols)
	for i := range colData {
		colData[i] = make([]interface{}, len(rows))
	}

	for i, row := range rows {
		if len(row) != numCols {
			return nil, fmt.Errorf("row %d has %d values, expected %d", i, len(row), numCols)
		}
		for j, val := range row {
			colData[j][i] = val
		}
	}

	var cols []series.Series
	for i, name := range schema {
		s, err := createSeriesWithInference(name, colData[i])
		if err != nil {
			return nil, err
		}
		cols = append(cols, s)
	}
	return frame.NewDataFrame(cols...)
}

// toInterfaceSlice converts various slice types to []interface{}
func toInterfaceSlice(values interface{}) ([]interface{}, error) {
	if iface, ok := values.([]interface{}); ok {
		return iface, nil
	}

	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected a slice, got %T", values)
	}

	result := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result, nil
}

// createSeriesWithInference creates a series with automatic type inference
func createSeriesWithInference(name string, values []interface{}) (Series, error) {
	dtype, err := inferType(values)
	if err != nil {
		return nil, err
	}

	typedValues, validity, err := convertToType(values, dtype)
	if err != nil {
		return nil, err
	}

	hasNulls := false
	for _, v := range validity {
		if !v {
			hasNulls = true
			break
		}
	}

	return createSeriesFromConvertedValues(name, typedValues, validity, dtype, hasNulls)
}

// inferType analyzes a slice of interface{} values and determines the most appropriate data type
func inferType(values []interface{}) (datatypes.DataType, error) {
	if len(values) == 0 {
		return datatypes.String{}, nil
	}

	// Find first non-nil value to determine type
	var sampleType reflect.Type
	for _, v := range values {
		if v != nil {
			sampleType = reflect.TypeOf(v)
			break
		}
	}

	if sampleType == nil {
		return datatypes.String{}, nil
	}

	switch sampleType.Kind() {
	case reflect.Bool:
		return datatypes.Boolean{}, nil
	case reflect.Int, reflect.Int64:
		return datatypes.Int64{}, nil
	case reflect.Int8:
		return datatypes.Int8{}, nil
	case reflect.Int16:
		return datatypes.Int16{}, nil
	case reflect.Int32:
		return datatypes.Int32{}, nil
	case reflect.Uint, reflect.Uint64:
		return datatypes.UInt64{}, nil
	case reflect.Uint8:
		return datatypes.UInt8{}, nil
	case reflect.Uint16:
		return datatypes.UInt16{}, nil
	case reflect.Uint32:
		return datatypes.UInt32{}, nil
	case reflect.Float32:
		return datatypes.Float32{}, nil
	case reflect.Float64:
		return datatypes.Float64{}, nil
	case reflect.String:
		return datatypes.String{}, nil
	default:
		return datatypes.String{}, nil
	}
}

// convertToType converts a slice of interface{} to the specific type needed for series creation
func convertToType(values []interface{}, dtype datatypes.DataType) (interface{}, []bool, error) {
	validity := make([]bool, len(values))

	switch dtype.(type) {
	case datatypes.Boolean:
		result := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(bool)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.Int8:
		result := make([]int8, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(int8)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.Int16:
		result := make([]int16, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(int16)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.Int32:
		result := make([]int32, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(int32)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.Int64:
		result := make([]int64, len(values))
		for i, v := range values {
			if v != nil {
				switch n := v.(type) {
				case int:
					result[i] = int64(n)
				case int64:
					result[i] = n
				default:
					result[i] = reflect.ValueOf(v).Int()
				}
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.UInt8:
		result := make([]uint8, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(uint8)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.UInt16:
		result := make([]uint16, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(uint16)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.UInt32:
		result := make([]uint32, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(uint32)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.UInt64:
		result := make([]uint64, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(uint64)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.Float32:
		result := make([]float32, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(float32)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.Float64:
		result := make([]float64, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = v.(float64)
				validity[i] = true
			}
		}
		return result, validity, nil

	case datatypes.String:
		result := make([]string, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = fmt.Sprintf("%v", v)
				validity[i] = true
			}
		}
		return result, validity, nil

	default:
		result := make([]string, len(values))
		for i, v := range values {
			if v != nil {
				result[i] = fmt.Sprintf("%v", v)
				validity[i] = true
			}
		}
		return result, validity, nil
	}
}
