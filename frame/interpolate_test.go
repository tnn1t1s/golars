package frame

import (
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterpolate(t *testing.T) {
	t.Run("Linear interpolation", func(t *testing.T) {
		// Create series with nulls
		values := []float64{1.0, 0, 0, 4.0, 0, 6.0}
		validity := []bool{true, false, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Linear interpolate
		interp, err := df.Interpolate(InterpolateOptions{
			Method: "linear",
		})
		require.NoError(t, err)
		
		col, err := interp.Column("col")
		require.NoError(t, err)
		
		// Check interpolated values
		assert.Equal(t, 1.0, col.Get(0))
		assert.Equal(t, 2.0, col.Get(1))  // Interpolated: 1 + 1/3*(4-1) = 2
		assert.Equal(t, 3.0, col.Get(2))  // Interpolated: 1 + 2/3*(4-1) = 3
		assert.Equal(t, 4.0, col.Get(3))
		assert.Equal(t, 5.0, col.Get(4))  // Interpolated: 4 + 1/2*(6-4) = 5
		assert.Equal(t, 6.0, col.Get(5))
	})
	
	t.Run("Linear interpolation with integers", func(t *testing.T) {
		// Test that integer types are preserved
		values := []int64{10, 0, 0, 40}
		validity := []bool{true, false, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Int64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		interp, err := df.Interpolate(InterpolateOptions{
			Method: "linear",
		})
		require.NoError(t, err)
		
		col, err := interp.Column("col")
		require.NoError(t, err)
		
		// Values should be rounded for integer types
		assert.Equal(t, int64(10), col.Get(0))
		assert.Equal(t, int64(20), col.Get(1))  // 10 + 1/3*(40-10) = 20
		assert.Equal(t, int64(30), col.Get(2))  // 10 + 2/3*(40-10) = 30
		assert.Equal(t, int64(40), col.Get(3))
	})
	
	t.Run("Nearest neighbor interpolation", func(t *testing.T) {
		values := []float64{1.0, 0, 0, 4.0, 0, 6.0}
		validity := []bool{true, false, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		interp, err := df.Interpolate(InterpolateOptions{
			Method: "nearest",
		})
		require.NoError(t, err)
		
		col, err := interp.Column("col")
		require.NoError(t, err)
		
		// Check nearest neighbor values
		assert.Equal(t, 1.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1))  // Nearest is 1.0
		assert.Equal(t, 4.0, col.Get(2))  // Nearest is 4.0 (equal distance, take next)
		assert.Equal(t, 4.0, col.Get(3))
		assert.Equal(t, 4.0, col.Get(4))  // Nearest is 4.0
		assert.Equal(t, 6.0, col.Get(5))
	})
	
	t.Run("Zero-order hold interpolation", func(t *testing.T) {
		values := []float64{1.0, 0, 0, 4.0, 0, 6.0}
		validity := []bool{true, false, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		interp, err := df.Interpolate(InterpolateOptions{
			Method: "zero",
		})
		require.NoError(t, err)
		
		col, err := interp.Column("col")
		require.NoError(t, err)
		
		// Zero-order hold is forward fill
		assert.Equal(t, 1.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1))  // Forward filled
		assert.Equal(t, 1.0, col.Get(2))  // Forward filled
		assert.Equal(t, 4.0, col.Get(3))
		assert.Equal(t, 4.0, col.Get(4))  // Forward filled
		assert.Equal(t, 6.0, col.Get(5))
	})
	
	t.Run("Interpolation with limit", func(t *testing.T) {
		values := []float64{1.0, 0, 0, 0, 5.0}
		validity := []bool{true, false, false, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Interpolate with limit of 2
		interp, err := df.Interpolate(InterpolateOptions{
			Method: "linear",
			Limit:  2,
		})
		require.NoError(t, err)
		
		col, err := interp.Column("col")
		require.NoError(t, err)
		
		// Only first 2 nulls should be interpolated
		assert.Equal(t, 1.0, col.Get(0))
		assert.True(t, col.IsNull(1))  // Exceeds limit (3 consecutive nulls)
		assert.True(t, col.IsNull(2))  // Exceeds limit
		assert.True(t, col.IsNull(3))  // Exceeds limit
		assert.Equal(t, 5.0, col.Get(4))
	})
	
	t.Run("Interpolation with limit area inside", func(t *testing.T) {
		values := []float64{0, 2.0, 0, 4.0, 0}
		validity := []bool{false, true, false, true, false}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Only interpolate inside valid values
		interp, err := df.Interpolate(InterpolateOptions{
			Method:    "linear",
			LimitArea: "inside",
		})
		require.NoError(t, err)
		
		col, err := interp.Column("col")
		require.NoError(t, err)
		
		// Edge nulls should not be interpolated
		assert.True(t, col.IsNull(0))   // Outside (no previous value)
		assert.Equal(t, 2.0, col.Get(1))
		assert.Equal(t, 3.0, col.Get(2)) // Inside (between 2 and 4)
		assert.Equal(t, 4.0, col.Get(3))
		assert.True(t, col.IsNull(4))   // Outside (no next value)
	})
	
	t.Run("Multiple columns", func(t *testing.T) {
		// Create DataFrame with numeric and non-numeric columns
		s1 := series.NewSeriesWithValidity("nums", []float64{1, 0, 3}, []bool{true, false, true}, datatypes.Float64{})
		s2 := series.NewStringSeries("names", []string{"a", "b", "c"})
		
		df, err := NewDataFrame(s1, s2)
		require.NoError(t, err)
		
		// Interpolate (should only affect numeric column)
		interp, err := df.Interpolate(InterpolateOptions{
			Method: "linear",
		})
		require.NoError(t, err)
		
		nums, err := interp.Column("nums")
		require.NoError(t, err)
		names, err := interp.Column("names")
		require.NoError(t, err)
		
		// Numeric column should be interpolated
		assert.Equal(t, 1.0, nums.Get(0))
		assert.Equal(t, 2.0, nums.Get(1)) // Interpolated
		assert.Equal(t, 3.0, nums.Get(2))
		
		// String column should be unchanged
		assert.Equal(t, "a", names.Get(0))
		assert.Equal(t, "b", names.Get(1))
		assert.Equal(t, "c", names.Get(2))
	})
	
	t.Run("Specific columns", func(t *testing.T) {
		s1 := series.NewSeriesWithValidity("col1", []float64{1, 0, 3}, []bool{true, false, true}, datatypes.Float64{})
		s2 := series.NewSeriesWithValidity("col2", []float64{4, 0, 6}, []bool{true, false, true}, datatypes.Float64{})
		
		df, err := NewDataFrame(s1, s2)
		require.NoError(t, err)
		
		// Only interpolate col1
		interp, err := df.Interpolate(InterpolateOptions{
			Method:  "linear",
			Columns: []string{"col1"},
		})
		require.NoError(t, err)
		
		col1, err := interp.Column("col1")
		require.NoError(t, err)
		col2, err := interp.Column("col2")
		require.NoError(t, err)
		
		// col1 should be interpolated
		assert.Equal(t, 2.0, col1.Get(1))
		
		// col2 should have original null
		assert.True(t, col2.IsNull(1))
	})
	
	t.Run("Invalid method", func(t *testing.T) {
		df, err := NewDataFrame(series.NewFloat64Series("col", []float64{1, 2, 3}))
		require.NoError(t, err)
		
		_, err = df.Interpolate(InterpolateOptions{
			Method: "invalid",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid interpolation method")
	})
}