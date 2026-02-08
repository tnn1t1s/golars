package series

import (
	_ "math"
	_ "sort"
)

// Sum returns the sum of all non-null values
func (s *TypedSeries[T]) Sum() float64 {
	panic("not implemented")

}

// Mean returns the mean of all non-null values
func (s *TypedSeries[T]) Mean() float64 {
	panic("not implemented")

}

// Min returns the minimum value
func (s *TypedSeries[T]) Min() interface{} {
	panic("not implemented")

}

// Max returns the maximum value
func (s *TypedSeries[T]) Max() interface{} {
	panic("not implemented")

}

// Count returns the number of non-null values
func (s *TypedSeries[T]) Count() int {
	panic("not implemented")

}

// Std returns the standard deviation
func (s *TypedSeries[T]) Std() float64 {
	panic("not implemented")

}

// Var returns the variance
func (s *TypedSeries[T]) Var() float64 {
	panic("not implemented")

	// Sample variance (n-1 denominator)

}

// Median returns the median value
func (s *TypedSeries[T]) Median() float64 {
	panic("not implemented")

	// Even number of values

	// Odd number of values

}

// Helper function to convert values to float64
func toFloat64(val interface{}) float64 {
	panic("not implemented")

}

// Helper function to compare values for aggregation
func compareValuesAgg(a, b interface{}) int {
	panic(
		// For numeric types, convert to float64 for comparison
		"not implemented")

}
