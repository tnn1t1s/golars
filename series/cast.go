package series

import (
	"fmt"
	"strconv"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

func castSeries(s Series, target datatypes.DataType) (Series, error) {
	if s.DataType().Equals(target) {
		return s.Clone(), nil
	}

	n := s.Len()
	switch target.(type) {
	case datatypes.Int64:
		values := make([]int64, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if s.IsNull(i) {
				validity[i] = false
				continue
			}
			v, ok := castToInt64(s.Get(i))
			if !ok {
				return nil, fmt.Errorf("cannot cast value at index %d to int64", i)
			}
			values[i] = v
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, target), nil

	case datatypes.Int32:
		values := make([]int32, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if s.IsNull(i) {
				validity[i] = false
				continue
			}
			v, ok := castToInt32(s.Get(i))
			if !ok {
				return nil, fmt.Errorf("cannot cast value at index %d to int32", i)
			}
			values[i] = v
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, target), nil

	case datatypes.Float64:
		values := make([]float64, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if s.IsNull(i) {
				validity[i] = false
				continue
			}
			v, ok := castToFloat64(s.Get(i))
			if !ok {
				return nil, fmt.Errorf("cannot cast value at index %d to float64", i)
			}
			values[i] = v
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, target), nil

	case datatypes.Float32:
		values := make([]float32, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if s.IsNull(i) {
				validity[i] = false
				continue
			}
			v, ok := castToFloat32(s.Get(i))
			if !ok {
				return nil, fmt.Errorf("cannot cast value at index %d to float32", i)
			}
			values[i] = v
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, target), nil

	case datatypes.String:
		values := make([]string, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if s.IsNull(i) {
				validity[i] = false
				continue
			}
			values[i] = fmt.Sprintf("%v", s.Get(i))
			validity[i] = true
		}
		return NewSeriesWithValidity(s.Name(), values, validity, target), nil

	default:
		return nil, fmt.Errorf("unsupported cast from %s to %s", s.DataType().String(), target.String())
	}
}

func castToInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
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
	case bool:
		if val {
			return 1, true
		}
		return 0, true
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func castToInt32(v interface{}) (int32, bool) {
	i64, ok := castToInt64(v)
	if !ok {
		return 0, false
	}
	return int32(i64), true
}

func castToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
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
	case bool:
		if val {
			return 1.0, true
		}
		return 0.0, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func castToFloat32(v interface{}) (float32, bool) {
	f64, ok := castToFloat64(v)
	if !ok {
		return 0, false
	}
	return float32(f64), true
}
