package golars

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
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
		return nil, fmt.Errorf("at least one argument required")
	}

	var name string
	var values interface{}
	var dtype datatypes.DataType

	switch len(args) {
	case 1:
		// SeriesFrom(values) - anonymous series
		name = ""
		values = args[0]
	case 2:
		// SeriesFrom(name, values) or SeriesFrom(values, dtype)
		if n, ok := args[0].(string); ok {
			name = n
			values = args[1]
		} else if dt, ok := args[1].(datatypes.DataType); ok {
			values = args[0]
			dtype = dt
		} else {
			name = ""
			values = args[0]
		}
	case 3:
		// SeriesFrom(name, values, dtype)
		name, _ = args[0].(string)
		values = args[1]
		dtype, _ = args[2].(datatypes.DataType)
	default:
		return nil, fmt.Errorf("too many arguments: expected 1-3, got %d", len(args))
	}

	// Try creating from typed slice directly
	if dtype != nil {
		s, err := createSeriesFromTypedSlice(name, values, dtype)
		if err == nil {
			return s, nil
		}
	}

	// Convert values to []interface{} if needed
	iface, err := toInterfaceSlice(values)
	if err != nil {
		// Try to handle specific types directly
		s, serr := createSeriesFromTypedSlice(name, values, dtype)
		if serr != nil {
			return nil, fmt.Errorf("cannot create series: %w", err)
		}
		return s, nil
	}

	// If dtype is specified, use it; otherwise infer
	if dtype == nil {
		dtype, err = inferType(iface)
		if err != nil {
			return nil, err
		}
	}

	typedValues, validity, err := convertToType(iface, dtype)
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

// createSeriesFromTypedSlice handles strongly-typed slices directly
func createSeriesFromTypedSlice(name string, values interface{}, dtype datatypes.DataType) (Series, error) {
	switch v := values.(type) {
	case []bool:
		return series.NewBooleanSeries(name, v), nil
	case []int:
		vals := make([]int64, len(v))
		for i, n := range v {
			vals[i] = int64(n)
		}
		return series.NewInt64Series(name, vals), nil
	case []int8:
		return series.NewInt8Series(name, v), nil
	case []int16:
		return series.NewInt16Series(name, v), nil
	case []int32:
		return series.NewInt32Series(name, v), nil
	case []int64:
		return series.NewInt64Series(name, v), nil
	case []uint8:
		return series.NewUInt8Series(name, v), nil
	case []uint16:
		return series.NewUInt16Series(name, v), nil
	case []uint32:
		return series.NewUInt32Series(name, v), nil
	case []uint64:
		return series.NewUInt64Series(name, v), nil
	case []float32:
		return series.NewFloat32Series(name, v), nil
	case []float64:
		return series.NewFloat64Series(name, v), nil
	case []string:
		return series.NewStringSeries(name, v), nil
	case [][]byte:
		return series.NewBinarySeries(name, v), nil
	default:
		return nil, fmt.Errorf("unsupported slice type: %T", values)
	}
}

// createIntSeriesFromInt converts []int to the appropriate integer series type
func createIntSeriesFromInt(name string, values []int, dtype datatypes.DataType) (Series, error) {
	if dtype == nil {
		vals := make([]int64, len(values))
		for i, n := range values {
			vals[i] = int64(n)
		}
		return series.NewInt64Series(name, vals), nil
	}

	switch dtype.(type) {
	case datatypes.Int8:
		vals := make([]int8, len(values))
		for i, n := range values {
			vals[i] = int8(n)
		}
		return series.NewInt8Series(name, vals), nil
	case datatypes.Int16:
		vals := make([]int16, len(values))
		for i, n := range values {
			vals[i] = int16(n)
		}
		return series.NewInt16Series(name, vals), nil
	case datatypes.Int32:
		vals := make([]int32, len(values))
		for i, n := range values {
			vals[i] = int32(n)
		}
		return series.NewInt32Series(name, vals), nil
	default:
		vals := make([]int64, len(values))
		for i, n := range values {
			vals[i] = int64(n)
		}
		return series.NewInt64Series(name, vals), nil
	}
}

// createSeriesFromConvertedValues creates a series from pre-converted values
func createSeriesFromConvertedValues(name string, typedValues interface{}, validity []bool, dtype datatypes.DataType, hasNulls bool) (Series, error) {
	if !hasNulls {
		switch v := typedValues.(type) {
		case []bool:
			return series.NewBooleanSeries(name, v), nil
		case []int8:
			return series.NewInt8Series(name, v), nil
		case []int16:
			return series.NewInt16Series(name, v), nil
		case []int32:
			return series.NewInt32Series(name, v), nil
		case []int64:
			return series.NewInt64Series(name, v), nil
		case []uint8:
			return series.NewUInt8Series(name, v), nil
		case []uint16:
			return series.NewUInt16Series(name, v), nil
		case []uint32:
			return series.NewUInt32Series(name, v), nil
		case []uint64:
			return series.NewUInt64Series(name, v), nil
		case []float32:
			return series.NewFloat32Series(name, v), nil
		case []float64:
			return series.NewFloat64Series(name, v), nil
		case []string:
			return series.NewStringSeries(name, v), nil
		default:
			return nil, fmt.Errorf("unsupported typed values: %T", typedValues)
		}
	}

	// With nulls, use NewSeriesWithValidity
	switch v := typedValues.(type) {
	case []bool:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Boolean{}), nil
	case []int8:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Int8{}), nil
	case []int16:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Int16{}), nil
	case []int32:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Int32{}), nil
	case []int64:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Int64{}), nil
	case []uint8:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.UInt8{}), nil
	case []uint16:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.UInt16{}), nil
	case []uint32:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.UInt32{}), nil
	case []uint64:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.UInt64{}), nil
	case []float32:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Float32{}), nil
	case []float64:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.Float64{}), nil
	case []string:
		return series.NewSeriesWithValidity(name, v, validity, datatypes.String{}), nil
	default:
		return nil, fmt.Errorf("unsupported typed values: %T", typedValues)
	}
}

// isCompatibleType checks if two data types are compatible
func isCompatibleType(a, b datatypes.DataType) bool {
	return a == b
}
