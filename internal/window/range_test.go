package window

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/series"
)

func TestRangeBetween(t *testing.T) {
	t.Run("RANGE with numeric offset", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 20, 30, 40, 50}
		s := series.NewInt32Series("value", values)

		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4, 5}
		partition := NewPartition(seriesMap, indices)

		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)

		// Test RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING
		spec := &FrameSpec{
			Type:  RangeFrame,
			Start: FrameBound{Type: Preceding, Offset: int32(5)},
			End:   FrameBound{Type: Following, Offset: int32(5)},
		}

		// Row 0 (value=10): Range [5,15] includes only row 0
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 1, end)

		// Row 1 (value=20): Range [15,25] includes rows 1,2 (both have value 20)
		start, end = partition.FrameBounds(1, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 3, end)

		// Row 3 (value=30): Range [25,35] includes only row 3
		start, end = partition.FrameBounds(3, spec)
		assert.Equal(t, 3, start)
		assert.Equal(t, 4, end)

		// Row 4 (value=40): Range [35,45] includes only row 4
		start, end = partition.FrameBounds(4, spec)
		assert.Equal(t, 4, start)
		assert.Equal(t, 5, end)
	})

	t.Run("RANGE CURRENT ROW includes peers", func(t *testing.T) {
		// Create test data with duplicate values
		values := []int32{10, 20, 20, 20, 30}
		s := series.NewInt32Series("value", values)

		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)

		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)

		// Test RANGE BETWEEN CURRENT ROW AND CURRENT ROW
		spec := &FrameSpec{
			Type:  RangeFrame,
			Start: FrameBound{Type: CurrentRow},
			End:   FrameBound{Type: CurrentRow},
		}

		// Row 0 (value=10): Only includes row 0
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 1, end)

		// Row 1 (value=20): Includes all rows with value 20 (rows 1,2,3)
		start, end = partition.FrameBounds(1, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 4, end)

		// Row 2 (value=20): Also includes all rows with value 20
		start, end = partition.FrameBounds(2, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 4, end)

		// Row 4 (value=30): Only includes row 4
		start, end = partition.FrameBounds(4, spec)
		assert.Equal(t, 4, start)
		assert.Equal(t, 5, end)
	})

	t.Run("RANGE UNBOUNDED PRECEDING TO CURRENT ROW", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 20, 30}
		s := series.NewInt32Series("value", values)

		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3}
		partition := NewPartition(seriesMap, indices)

		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)

		// Test default RANGE frame
		spec := &FrameSpec{
			Type:  RangeFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}

		// Row 0: Includes row 0
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 1, end)

		// Row 1: Includes rows 0,1,2 (peer rows included)
		start, end = partition.FrameBounds(1, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 3, end)

		// Row 3: Includes all rows
		start, end = partition.FrameBounds(3, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 4, end)
	})

	t.Run("RANGE with float values", func(t *testing.T) {
		// Create test data
		values := []float64{1.5, 2.0, 2.5, 3.0, 3.5}
		s := series.NewFloat64Series("value", values)

		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)

		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)

		// Test RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING
		spec := &FrameSpec{
			Type:  RangeFrame,
			Start: FrameBound{Type: Preceding, Offset: 0.5},
			End:   FrameBound{Type: Following, Offset: 0.5},
		}

		// Row 2 (value=2.5): Range [2.0,3.0] includes rows 1,2,3
		start, end := partition.FrameBounds(2, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 4, end)
	})

	t.Run("RANGE with null values", func(t *testing.T) {
		// Create test data with nulls
		values := []int32{10, 0, 0, 20, 30}
		validity := []bool{true, false, false, true, true}
		s := series.NewSeriesWithValidity("value", values, validity, series.NewInt32Series("", nil).DataType())

		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)

		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)

		// Test RANGE BETWEEN CURRENT ROW AND CURRENT ROW
		spec := &FrameSpec{
			Type:  RangeFrame,
			Start: FrameBound{Type: CurrentRow},
			End:   FrameBound{Type: CurrentRow},
		}

		// Null values should be grouped together
		// Row 1 (null): Should include both null rows (1,2)
		start, end := partition.FrameBounds(1, spec)
		assert.Equal(t, 0, start) // Nulls are typically sorted first
		assert.Equal(t, 2, end)
	})

	t.Run("Using RangeBetween method", func(t *testing.T) {
		// Test the spec builder method
		spec := NewSpec().OrderBy("value").RangeBetween(-10, 10)

		assert.Equal(t, RangeFrame, spec.frame.Type)
		assert.Equal(t, Preceding, spec.frame.Start.Type)
		assert.Equal(t, 10, spec.frame.Start.Offset)
		assert.Equal(t, Following, spec.frame.End.Type)
		assert.Equal(t, 10, spec.frame.End.Offset)

		// Test with strings
		spec2 := NewSpec().OrderBy("value").RangeBetween("UNBOUNDED PRECEDING", "CURRENT ROW")
		assert.Equal(t, UnboundedPreceding, spec2.frame.Start.Type)
		assert.Equal(t, CurrentRow, spec2.frame.End.Type)
	})
}
