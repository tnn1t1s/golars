package series

import (
	"math"
	"sort"
)

// Sum returns the sum of all non-null values
func (s *TypedSeries[T]) Sum() float64 {
	values, validity := s.chunkedArray.ToSlice()
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for i, v := range values {
		if validity[i] {
			sum += toFloat64(v)
		}
	}
	return sum
}

// Mean returns the mean of all non-null values
func (s *TypedSeries[T]) Mean() float64 {
	count := s.Count()
	if count == 0 {
		return math.NaN()
	}
	return s.Sum() / float64(count)
}

// Min returns the minimum value
func (s *TypedSeries[T]) Min() interface{} {
	values, validity := s.chunkedArray.ToSlice()
	if len(values) == 0 {
		return nil
	}
	var minVal interface{}
	first := true
	for i, v := range values {
		if !validity[i] {
			continue
		}
		if first {
			minVal = v
			first = false
			continue
		}
		if compareValuesAgg(v, minVal) < 0 {
			minVal = v
		}
	}
	if first {
		return nil
	}
	return minVal
}

// Max returns the maximum value
func (s *TypedSeries[T]) Max() interface{} {
	values, validity := s.chunkedArray.ToSlice()
	if len(values) == 0 {
		return nil
	}
	var maxVal interface{}
	first := true
	for i, v := range values {
		if !validity[i] {
			continue
		}
		if first {
			maxVal = v
			first = false
			continue
		}
		if compareValuesAgg(v, maxVal) > 0 {
			maxVal = v
		}
	}
	if first {
		return nil
	}
	return maxVal
}

// Count returns the number of non-null values
func (s *TypedSeries[T]) Count() int {
	return s.Len() - s.NullCount()
}

// Std returns the standard deviation
func (s *TypedSeries[T]) Std() float64 {
	v := s.Var()
	if math.IsNaN(v) {
		return math.NaN()
	}
	return math.Sqrt(v)
}

// Var returns the variance
func (s *TypedSeries[T]) Var() float64 {
	count := s.Count()
	if count < 2 {
		return math.NaN()
	}
	mean := s.Mean()
	if math.IsNaN(mean) {
		return math.NaN()
	}

	// Sample variance (n-1 denominator)
	values, validity := s.chunkedArray.ToSlice()
	sumSqDiff := 0.0
	for i, v := range values {
		if validity[i] {
			diff := toFloat64(v) - mean
			sumSqDiff += diff * diff
		}
	}
	return sumSqDiff / float64(count-1)
}

// Median returns the median value
func (s *TypedSeries[T]) Median() float64 {
	count := s.Count()
	if count == 0 {
		return math.NaN()
	}

	// Collect non-null values as float64
	values, validity := s.chunkedArray.ToSlice()
	floats := make([]float64, 0, count)
	for i, v := range values {
		if validity[i] {
			floats = append(floats, toFloat64(v))
		}
	}

	sort.Float64s(floats)

	n := len(floats)
	if n%2 == 0 {
		// Even number of values
		return (floats[n/2-1] + floats[n/2]) / 2.0
	}
	// Odd number of values
	return floats[n/2]
}

// Helper function to convert values to float64
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return math.NaN()
	}
}

// Helper function to compare values for aggregation
func compareValuesAgg(a, b interface{}) int {
	// For numeric types, convert to float64 for comparison
	switch av := a.(type) {
	case bool:
		bv := b.(bool)
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	case string:
		bv := b.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		af := toFloat64(a)
		bf := toFloat64(b)
		if math.IsNaN(af) && math.IsNaN(bf) {
			return 0
		}
		if math.IsNaN(af) {
			return 1
		}
		if math.IsNaN(bf) {
			return -1
		}
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
}
