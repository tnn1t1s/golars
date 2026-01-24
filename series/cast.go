package series

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

func castSeries(s Series, target datatypes.DataType) (Series, error) {
	if s.DataType().Equals(target) {
		return s.Clone(), nil
	}

	switch target.(type) {
	case datatypes.Int64:
		values := make([]int64, s.Len())
		validity := make([]bool, s.Len())
		for i := 0; i < s.Len(); i++ {
			val := s.Get(i)
			if val == nil {
				continue
			}
			converted, ok := castToInt64(val)
			if !ok {
				return nil, fmt.Errorf("cannot cast %T to int64", val)
			}
			values[i] = converted
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, datatypes.Int64{}), nil

	case datatypes.Int32:
		values := make([]int32, s.Len())
		validity := make([]bool, s.Len())
		for i := 0; i < s.Len(); i++ {
			val := s.Get(i)
			if val == nil {
				continue
			}
			converted, ok := castToInt32(val)
			if !ok {
				return nil, fmt.Errorf("cannot cast %T to int32", val)
			}
			values[i] = converted
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, datatypes.Int32{}), nil

	case datatypes.Float64:
		values := make([]float64, s.Len())
		validity := make([]bool, s.Len())
		for i := 0; i < s.Len(); i++ {
			val := s.Get(i)
			if val == nil {
				continue
			}
			converted, ok := castToFloat64(val)
			if !ok {
				return nil, fmt.Errorf("cannot cast %T to float64", val)
			}
			values[i] = converted
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, datatypes.Float64{}), nil

	case datatypes.Float32:
		values := make([]float32, s.Len())
		validity := make([]bool, s.Len())
		for i := 0; i < s.Len(); i++ {
			val := s.Get(i)
			if val == nil {
				continue
			}
			converted, ok := castToFloat32(val)
			if !ok {
				return nil, fmt.Errorf("cannot cast %T to float32", val)
			}
			values[i] = converted
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, datatypes.Float32{}), nil

	case datatypes.Boolean:
		values := make([]bool, s.Len())
		validity := make([]bool, s.Len())
		for i := 0; i < s.Len(); i++ {
			val := s.Get(i)
			if val == nil {
				continue
			}
			converted, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("cannot cast %T to bool", val)
			}
			values[i] = converted
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, datatypes.Boolean{}), nil

	case datatypes.String:
		values := make([]string, s.Len())
		validity := make([]bool, s.Len())
		for i := 0; i < s.Len(); i++ {
			val := s.Get(i)
			if val == nil {
				continue
			}
			converted, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("cannot cast %T to string", val)
			}
			values[i] = converted
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, datatypes.String{}), nil
	default:
		return nil, fmt.Errorf("unsupported cast target %s", target.String())
	}
}

func castToInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func castToInt32(v interface{}) (int32, bool) {
	switch val := v.(type) {
	case int:
		return int32(val), true
	case int8:
		return int32(val), true
	case int16:
		return int32(val), true
	case int32:
		return val, true
	case int64:
		return int32(val), true
	case uint8:
		return int32(val), true
	case uint16:
		return int32(val), true
	case uint32:
		return int32(val), true
	case uint64:
		return int32(val), true
	case float32:
		return int32(val), true
	case float64:
		return int32(val), true
	default:
		return 0, false
	}
}

func castToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

func castToFloat32(v interface{}) (float32, bool) {
	switch val := v.(type) {
	case int:
		return float32(val), true
	case int8:
		return float32(val), true
	case int16:
		return float32(val), true
	case int32:
		return float32(val), true
	case int64:
		return float32(val), true
	case uint8:
		return float32(val), true
	case uint16:
		return float32(val), true
	case uint32:
		return float32(val), true
	case uint64:
		return float32(val), true
	case float32:
		return val, true
	case float64:
		return float32(val), true
	default:
		return 0, false
	}
}
