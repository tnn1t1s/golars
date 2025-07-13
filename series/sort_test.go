package series

import (
	"fmt"
	"math"
	"testing"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/stretchr/testify/assert"
)

func TestSeriesSort(t *testing.T) {
	// Test integer sorting
	t.Run("Int32 Ascending", func(t *testing.T) {
		s := NewInt32Series("values", []int32{3, 1, 4, 1, 5, 9, 2, 6})
		sorted := s.Sort(true)
		
		expected := []int32{1, 1, 2, 3, 4, 5, 6, 9}
		for i, exp := range expected {
			assert.Equal(t, exp, sorted.Get(i))
		}
	})

	t.Run("Int32 Descending", func(t *testing.T) {
		s := NewInt32Series("values", []int32{3, 1, 4, 1, 5, 9, 2, 6})
		sorted := s.Sort(false)
		
		expected := []int32{9, 6, 5, 4, 3, 2, 1, 1}
		for i, exp := range expected {
			assert.Equal(t, exp, sorted.Get(i))
		}
	})

	// Test float sorting with NaN
	t.Run("Float64 with NaN", func(t *testing.T) {
		nan := math.NaN() // NaN
		s := NewFloat64Series("values", []float64{3.0, nan, 1.0, 4.0, nan, 2.0})
		sorted := s.Sort(true)
		
		// NaN values should be at the end by default
		assert.Equal(t, 1.0, sorted.Get(0))
		assert.Equal(t, 2.0, sorted.Get(1))
		assert.Equal(t, 3.0, sorted.Get(2))
		assert.Equal(t, 4.0, sorted.Get(3))
		assert.True(t, sorted.Get(4).(float64) != sorted.Get(4).(float64)) // NaN check
		assert.True(t, sorted.Get(5).(float64) != sorted.Get(5).(float64)) // NaN check
	})

	// Test string sorting
	t.Run("String Sorting", func(t *testing.T) {
		s := NewStringSeries("names", []string{"Charlie", "Alice", "Bob", "David", "Eve"})
		sorted := s.Sort(true)
		
		expected := []string{"Alice", "Bob", "Charlie", "David", "Eve"}
		for i, exp := range expected {
			assert.Equal(t, exp, sorted.Get(i))
		}
	})

	// Test with nulls
	t.Run("With Nulls", func(t *testing.T) {
		values := []float64{3.0, 1.0, 4.0, 2.0}
		validity := []bool{true, false, true, false} // 1.0 and 2.0 are null
		
		s := NewSeriesWithValidity("values", values, validity, datatypes.Float64{})
		sorted := s.Sort(true)
		
		// Non-null values first (nulls last by default)
		assert.Equal(t, 3.0, sorted.Get(0))
		assert.Equal(t, 4.0, sorted.Get(1))
		assert.True(t, sorted.IsNull(2))
		assert.True(t, sorted.IsNull(3))
	})
}

func TestSeriesSortWithConfig(t *testing.T) {
	// Test nulls first
	t.Run("Nulls First", func(t *testing.T) {
		values := []int32{3, 1, 4, 2}
		validity := []bool{true, false, true, false}
		
		s := NewSeriesWithValidity("values", values, validity, datatypes.Int32{})
		sorted := s.(*TypedSeries[int32]).SortWithConfig(SortConfig{
			Order:      Ascending,
			NullsFirst: true,
			Stable:     true,
		})
		
		// Nulls should be first
		assert.True(t, sorted.IsNull(0))
		assert.True(t, sorted.IsNull(1))
		assert.Equal(t, int32(3), sorted.Get(2))
		assert.Equal(t, int32(4), sorted.Get(3))
	})

	// Test stable sort
	t.Run("Stable Sort", func(t *testing.T) {
		// Create series with duplicate values
		s := NewInt32Series("values", []int32{3, 1, 3, 1, 3, 1})
		
		// Get original indices for values
		indices := s.(*TypedSeries[int32]).ArgSort(SortConfig{
			Order:  Ascending,
			Stable: true,
		})
		
		// With stable sort, original order should be preserved for equal values
		// Original indices of 1s: 1, 3, 5
		// Original indices of 3s: 0, 2, 4
		assert.Equal(t, 1, indices[0]) // First 1
		assert.Equal(t, 3, indices[1]) // Second 1
		assert.Equal(t, 5, indices[2]) // Third 1
		assert.Equal(t, 0, indices[3]) // First 3
		assert.Equal(t, 2, indices[4]) // Second 3
		assert.Equal(t, 4, indices[5]) // Third 3
	})
}

func TestSeriesArgSort(t *testing.T) {
	s := NewInt32Series("values", []int32{30, 10, 40, 20})
	
	indices := s.(*TypedSeries[int32]).ArgSort(SortConfig{
		Order: Ascending,
	})
	
	// Expected indices: [1, 3, 0, 2] (10, 20, 30, 40)
	assert.Equal(t, 1, indices[0])
	assert.Equal(t, 3, indices[1])
	assert.Equal(t, 0, indices[2])
	assert.Equal(t, 2, indices[3])
}

func TestSeriesTake(t *testing.T) {
	s := NewStringSeries("letters", []string{"A", "B", "C", "D", "E"})
	
	// Take specific indices
	taken := s.Take([]int{4, 2, 0, 3, 1})
	
	assert.Equal(t, 5, taken.Len())
	assert.Equal(t, "E", taken.Get(0))
	assert.Equal(t, "C", taken.Get(1))
	assert.Equal(t, "A", taken.Get(2))
	assert.Equal(t, "D", taken.Get(3))
	assert.Equal(t, "B", taken.Get(4))
	
	// Test with invalid indices (should skip them)
	taken2 := s.Take([]int{0, 10, 2, -1, 4})
	assert.Equal(t, 3, taken2.Len()) // Only valid indices
}

func BenchmarkSeriesSort(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Int32_Size_%d", size), func(b *testing.B) {
			// Create random data
			data := make([]int32, size)
			for i := range data {
				data[i] = int32(i * 7 % size) // Pseudo-random
			}
			s := NewInt32Series("bench", data)
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.Sort(true)
			}
		})
		
		b.Run(fmt.Sprintf("String_Size_%d", size), func(b *testing.B) {
			// Create random string data
			data := make([]string, size)
			for i := range data {
				data[i] = fmt.Sprintf("item_%05d", i*7%size)
			}
			s := NewStringSeries("bench", data)
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.Sort(true)
			}
		})
	}
}