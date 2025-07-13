package window

import (
	"testing"

	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNthValue(t *testing.T) {
	t.Run("Basic NTH_VALUE test", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 30, 40, 50}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)
		
		// Create NTH_VALUE(value, 2) function
		fn := NthValue("value", 2)
		
		// Default frame is UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
		// So all rows should see the 2nd value (20)
		result, err := fn.Compute(partition)
		require.NoError(t, err)
		require.NotNil(t, result)
		
		// Check all values
		for i := 0; i < 5; i++ {
			assert.Equal(t, int32(20), result.Get(i))
		}
	})
	
	t.Run("NTH_VALUE with ordering and frame", func(t *testing.T) {
		// Create test data
		values := []int32{50, 40, 30, 20, 10}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)
		
		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)
		
		// Create spec with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
		spec := NewSpec().RowsBetween(-1, 1)
		
		// Create NTH_VALUE(value, 2) function with spec
		fn := NthValue("value", 2)
		fn.SetSpec(spec)
		
		// Test each row
		// After ordering: [10, 20, 30, 40, 50]
		result, err := fn.Compute(partition)
		require.NoError(t, err)
		require.NotNil(t, result)
		
		// Row 0 (value=10): Window [0,1] -> 2nd value is 20
		assert.Equal(t, int32(20), result.Get(0))
		
		// Row 1 (value=20): Window [0,2] -> 2nd value is 20
		assert.Equal(t, int32(20), result.Get(1))
		
		// Row 2 (value=30): Window [1,3] -> 2nd value is 30
		assert.Equal(t, int32(30), result.Get(2))
		
		// Row 3 (value=40): Window [2,4] -> 2nd value is 40
		assert.Equal(t, int32(40), result.Get(3))
		
		// Row 4 (value=50): Window [3,4] -> 2nd value is 50
		assert.Equal(t, int32(50), result.Get(4))
	})
	
	t.Run("NTH_VALUE out of bounds", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 30}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2}
		partition := NewPartition(seriesMap, indices)
		
		// Create spec with ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
		spec := NewSpec().RowsBetween(0, 1)
		
		// Create NTH_VALUE(value, 3) function - asking for 3rd value
		fn := NthValue("value", 3)
		fn.SetSpec(spec)
		
		result, err := fn.Compute(partition)
		require.NoError(t, err)
		require.NotNil(t, result)
		
		// Row 0: Window has 2 values, asking for 3rd -> null
		assert.True(t, result.IsNull(0))
		
		// Row 2: Window has only 1 value, asking for 3rd -> null
		assert.True(t, result.IsNull(2))
	})
	
	t.Run("NTH_VALUE with different n values", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 30, 40, 50}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)
		
		// Test NTH_VALUE with n=1 (first value)
		fn1 := NthValue("value", 1)
		result, err := fn1.Compute(partition)
		require.NoError(t, err)
		assert.Equal(t, int32(10), result.Get(2))
		
		// Test NTH_VALUE with n=5 (last value)
		fn5 := NthValue("value", 5)
		result, err = fn5.Compute(partition)
		require.NoError(t, err)
		assert.Equal(t, int32(50), result.Get(2))
	})
	
	t.Run("NTH_VALUE validation", func(t *testing.T) {
		// Test with invalid n values
		fn := &nthValueFunc{column: "value", n: 0}
		err := fn.Validate(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "n must be positive")
		
		fn2 := &nthValueFunc{column: "value", n: -1}
		err = fn2.Validate(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "n must be positive")
	})
	
	t.Run("NTH_VALUE with null values", func(t *testing.T) {
		// Create test data with nulls
		values := []int32{10, 0, 30, 0, 50}
		validity := []bool{true, false, true, false, true}
		s := series.NewSeriesWithValidity("value", values, validity, series.NewInt32Series("", nil).DataType())
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)
		
		// Create NTH_VALUE(value, 2) function
		fn := NthValue("value", 2)
		
		// Compute for all rows
		result, err := fn.Compute(partition)
		require.NoError(t, err)
		
		// The 2nd value is null, so all rows should see null
		assert.True(t, result.IsNull(0))
		
		// Create NTH_VALUE(value, 3) function
		fn3 := NthValue("value", 3)
		
		// Should return the 3rd value (30) for all rows
		result3, err := fn3.Compute(partition)
		require.NoError(t, err)
		assert.Equal(t, int32(30), result3.Get(0))
	})
}