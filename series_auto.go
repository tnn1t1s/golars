package golars

import (
	"fmt"
	
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// SeriesFrom creates a new series with automatic type inference
// The name parameter can be:
// - A string (series name) followed by values
// - Omitted (anonymous series with just values)
// 
// The values parameter can be:
// - A slice of any type
// - []interface{} with mixed types (will infer common type)
func SeriesFrom(args ...interface{}) (Series, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Series requires at least one argument")
	}
	
	var name string
	var values interface{}
	var dtype datatypes.DataType
	
	// Parse arguments
	switch len(args) {
	case 1:
		// Series(values) - anonymous series
		name = ""
		values = args[0]
	case 2:
		// Series(name, values) or Series(values, dtype)
		switch arg0 := args[0].(type) {
		case string:
			// Series(name, values)
			name = arg0
			values = args[1]
		default:
			// Series(values, dtype)
			name = ""
			values = args[0]
			if dt, ok := args[1].(datatypes.DataType); ok {
				dtype = dt
			} else {
				return nil, fmt.Errorf("second argument must be a DataType when first is not a string")
			}
		}
	case 3:
		// Series(name, values, dtype)
		n, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("first argument must be a string when providing 3 arguments")
		}
		name = n
		values = args[1]
		
		if dt, ok := args[2].(datatypes.DataType); ok {
			dtype = dt
		} else {
			return nil, fmt.Errorf("third argument must be a DataType")
		}
	default:
		return nil, fmt.Errorf("Series accepts 1-3 arguments, got %d", len(args))
	}
	
	// Convert values to []interface{} if needed
	interfaceValues, err := toInterfaceSlice(values)
	if err != nil {
		// If conversion fails, try to handle specific types directly
		return createSeriesFromTypedSlice(name, values, dtype)
	}
	
	// If dtype is specified, use it; otherwise infer
	if dtype != nil {
		typedValues, validity, err := convertToType(interfaceValues, dtype)
		if err != nil {
			return nil, err
		}
		
		// Check if we have nulls
		hasNulls := false
		for _, v := range validity {
			if !v {
				hasNulls = true
				break
			}
		}
		
		return createSeriesFromConvertedValues(name, typedValues, validity, dtype, hasNulls)
	}
	
	// Use type inference
	return createSeriesWithInference(name, interfaceValues)
}

// createSeriesFromTypedSlice handles strongly-typed slices directly
func createSeriesFromTypedSlice(name string, values interface{}, dtype datatypes.DataType) (Series, error) {
	switch v := values.(type) {
	case []bool:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Boolean{}) {
			return nil, fmt.Errorf("cannot create %v series from []bool", dtype)
		}
		return NewBooleanSeries(name, v), nil
		
	case []int:
		// Convert []int to appropriate integer type
		if dtype == nil {
			dtype = datatypes.Int64{} // Default for int
		}
		return createIntSeriesFromInt(name, v, dtype)
		
	case []int8:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Int8{}) {
			return nil, fmt.Errorf("cannot create %v series from []int8", dtype)
		}
		return NewInt8Series(name, v), nil
		
	case []int16:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Int16{}) {
			return nil, fmt.Errorf("cannot create %v series from []int16", dtype)
		}
		return NewInt16Series(name, v), nil
		
	case []int32:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Int32{}) {
			return nil, fmt.Errorf("cannot create %v series from []int32", dtype)
		}
		return NewInt32Series(name, v), nil
		
	case []int64:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Int64{}) {
			return nil, fmt.Errorf("cannot create %v series from []int64", dtype)
		}
		return NewInt64Series(name, v), nil
		
	case []float32:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Float32{}) {
			return nil, fmt.Errorf("cannot create %v series from []float32", dtype)
		}
		return NewFloat32Series(name, v), nil
		
	case []float64:
		if dtype != nil && !isCompatibleType(dtype, datatypes.Float64{}) {
			return nil, fmt.Errorf("cannot create %v series from []float64", dtype)
		}
		return NewFloat64Series(name, v), nil
		
	case []string:
		if dtype != nil && !isCompatibleType(dtype, datatypes.String{}) {
			return nil, fmt.Errorf("cannot create %v series from []string", dtype)
		}
		return NewStringSeries(name, v), nil
		
	default:
		return nil, fmt.Errorf("unsupported slice type: %T", values)
	}
}

// createIntSeriesFromInt converts []int to the appropriate integer series type
func createIntSeriesFromInt(name string, values []int, dtype datatypes.DataType) (Series, error) {
	switch dtype.(type) {
	case datatypes.Int8:
		result := make([]int8, len(values))
		for i, v := range values {
			result[i] = int8(v)
		}
		return NewInt8Series(name, result), nil
		
	case datatypes.Int16:
		result := make([]int16, len(values))
		for i, v := range values {
			result[i] = int16(v)
		}
		return NewInt16Series(name, result), nil
		
	case datatypes.Int32:
		result := make([]int32, len(values))
		for i, v := range values {
			result[i] = int32(v)
		}
		return NewInt32Series(name, result), nil
		
	case datatypes.Int64:
		result := make([]int64, len(values))
		for i, v := range values {
			result[i] = int64(v)
		}
		return NewInt64Series(name, result), nil
		
	default:
		return nil, fmt.Errorf("cannot convert []int to %v", dtype)
	}
}

// createSeriesFromConvertedValues creates a series from pre-converted values
func createSeriesFromConvertedValues(name string, typedValues interface{}, validity []bool, dtype datatypes.DataType, hasNulls bool) (Series, error) {
	switch dt := dtype.(type) {
	case datatypes.Boolean:
		vals := typedValues.([]bool)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewBooleanSeries(name, vals), nil
		
	case datatypes.Int8:
		vals := typedValues.([]int8)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewInt8Series(name, vals), nil
		
	case datatypes.Int16:
		vals := typedValues.([]int16)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewInt16Series(name, vals), nil
		
	case datatypes.Int32:
		vals := typedValues.([]int32)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewInt32Series(name, vals), nil
		
	case datatypes.Int64:
		vals := typedValues.([]int64)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewInt64Series(name, vals), nil
		
	case datatypes.Float32:
		vals := typedValues.([]float32)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewFloat32Series(name, vals), nil
		
	case datatypes.Float64:
		vals := typedValues.([]float64)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewFloat64Series(name, vals), nil
		
	case datatypes.String:
		vals := typedValues.([]string)
		if hasNulls {
			return NewSeriesWithValidity(name, vals, validity, dt), nil
		}
		return NewStringSeries(name, vals), nil
		
	default:
		return nil, fmt.Errorf("unsupported data type: %v", dtype)
	}
}

// isCompatibleType checks if two data types are compatible
func isCompatibleType(a, b datatypes.DataType) bool {
	// For now, require exact type match
	// Could be extended to allow compatible conversions
	return a.Equals(b)
}