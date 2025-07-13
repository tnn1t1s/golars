package series

import (
	"math"
	"sort"
)

// Sum returns the sum of all non-null values
func (s *TypedSeries[T]) Sum() float64 {
	if s.Len() == 0 {
		return 0
	}
	
	var sum float64
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			val, _ := s.chunkedArray.Get(int64(i))
			sum += toFloat64(val)
		}
	}
	return sum
}

// Mean returns the mean of all non-null values
func (s *TypedSeries[T]) Mean() float64 {
	count := 0
	var sum float64
	
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			val, _ := s.chunkedArray.Get(int64(i))
			sum += toFloat64(val)
			count++
		}
	}
	
	if count == 0 {
		return math.NaN()
	}
	return sum / float64(count)
}

// Min returns the minimum value
func (s *TypedSeries[T]) Min() interface{} {
	if s.Len() == 0 {
		return nil
	}
	
	var min interface{}
	minSet := false
	
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			val, _ := s.chunkedArray.Get(int64(i))
			if !minSet || compareValuesAgg(val, min) < 0 {
				min = val
				minSet = true
			}
		}
	}
	
	if !minSet {
		return nil
	}
	return min
}

// Max returns the maximum value
func (s *TypedSeries[T]) Max() interface{} {
	if s.Len() == 0 {
		return nil
	}
	
	var max interface{}
	maxSet := false
	
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			val, _ := s.chunkedArray.Get(int64(i))
			if !maxSet || compareValuesAgg(val, max) > 0 {
				max = val
				maxSet = true
			}
		}
	}
	
	if !maxSet {
		return nil
	}
	return max
}

// Count returns the number of non-null values
func (s *TypedSeries[T]) Count() int {
	return s.Len() - s.NullCount()
}

// Std returns the standard deviation
func (s *TypedSeries[T]) Std() float64 {
	return math.Sqrt(s.Var())
}

// Var returns the variance
func (s *TypedSeries[T]) Var() float64 {
	mean := s.Mean()
	if math.IsNaN(mean) {
		return math.NaN()
	}
	
	count := 0
	var sumSquaredDiff float64
	
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			val, _ := s.chunkedArray.Get(int64(i))
			diff := toFloat64(val) - mean
			sumSquaredDiff += diff * diff
			count++
		}
	}
	
	if count <= 1 {
		return math.NaN()
	}
	
	// Sample variance (n-1 denominator)
	return sumSquaredDiff / float64(count-1)
}

// Median returns the median value
func (s *TypedSeries[T]) Median() float64 {
	values := make([]float64, 0, s.Count())
	
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			val, _ := s.chunkedArray.Get(int64(i))
			values = append(values, toFloat64(val))
		}
	}
	
	if len(values) == 0 {
		return math.NaN()
	}
	
	sort.Float64s(values)
	
	n := len(values)
	if n%2 == 0 {
		// Even number of values
		return (values[n/2-1] + values[n/2]) / 2
	}
	// Odd number of values
	return values[n/2]
}

// Helper function to convert values to float64
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
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
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	default:
		return math.NaN()
	}
}

// Helper function to compare values for aggregation
func compareValuesAgg(a, b interface{}) int {
	// For numeric types, convert to float64 for comparison
	switch a.(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
		fa := toFloat64(a)
		fb := toFloat64(b)
		if fa < fb {
			return -1
		} else if fa > fb {
			return 1
		}
		return 0
	case string:
		sa := a.(string)
		sb := b.(string)
		if sa < sb {
			return -1
		} else if sa > sb {
			return 1
		}
		return 0
	case bool:
		ba := a.(bool)
		bb := b.(bool)
		if !ba && bb {
			return -1
		} else if ba && !bb {
			return 1
		}
		return 0
	default:
		return 0
	}
}