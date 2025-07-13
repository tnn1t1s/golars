package window

import (
	"testing"

	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupsFrame(t *testing.T) {
	t.Run("GROUPS BETWEEN basic test", func(t *testing.T) {
		// Create test data with duplicate values
		values := []int32{10, 20, 20, 30, 30, 30, 40}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4, 5, 6}
		partition := NewPartition(seriesMap, indices)
		
		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)
		
		// Test GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
		spec := &FrameSpec{
			Type:  GroupsFrame,
			Start: FrameBound{Type: Preceding, Offset: 1},
			End:   FrameBound{Type: Following, Offset: 1},
		}
		
		// Row 0 (value=10, group 0): Groups [-1,1] -> [0,1] includes rows 0-2
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 3, end) // Up to but not including row 3
		
		// Row 1 (value=20, group 1): Groups [0,2] includes rows 0-5
		start, end = partition.FrameBounds(1, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 6, end)
		
		// Row 3 (value=30, group 2): Groups [1,3] includes rows 1-6
		start, end = partition.FrameBounds(3, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 7, end) // Actually p.size since group 3 would be beyond
		
		// Row 6 (value=40, group 3): Groups [2,4] includes rows 3-6 (no group 4)
		start, end = partition.FrameBounds(6, spec)
		assert.Equal(t, 3, start)
		assert.Equal(t, 7, end)
	})
	
	t.Run("GROUPS CURRENT ROW", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 20, 20, 30}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4}
		partition := NewPartition(seriesMap, indices)
		
		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)
		
		// Test GROUPS BETWEEN CURRENT ROW AND CURRENT ROW
		spec := &FrameSpec{
			Type:  GroupsFrame,
			Start: FrameBound{Type: CurrentRow},
			End:   FrameBound{Type: CurrentRow},
		}
		
		// Row 0 (value=10): Only group 0 (row 0)
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 1, end)
		
		// Row 1 (value=20): Only group 1 (rows 1-3)
		start, end = partition.FrameBounds(1, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 4, end)
		
		// Row 2 (value=20): Same group as row 1
		start, end = partition.FrameBounds(2, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 4, end)
		
		// Row 4 (value=30): Only group 2 (row 4)
		start, end = partition.FrameBounds(4, spec)
		assert.Equal(t, 4, start)
		assert.Equal(t, 5, end)
	})
	
	t.Run("GROUPS UNBOUNDED", func(t *testing.T) {
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
		
		// Test GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		spec := &FrameSpec{
			Type:  GroupsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
		
		// Row 0: Groups [0,0] -> row 0 only
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 1, end)
		
		// Row 1: Groups [0,1] -> rows 0-2
		start, end = partition.FrameBounds(1, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 3, end)
		
		// Row 3: Groups [0,2] -> all rows
		start, end = partition.FrameBounds(3, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 4, end)
	})
	
	t.Run("GROUPS with no ordering", func(t *testing.T) {
		// Create test data
		values := []int32{10, 20, 30}
		s := series.NewInt32Series("value", values)
		
		// Create partition WITHOUT ordering
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2}
		partition := NewPartition(seriesMap, indices)
		
		// Test GROUPS frame without ordering - should include entire partition
		spec := &FrameSpec{
			Type:  GroupsFrame,
			Start: FrameBound{Type: Preceding, Offset: 1},
			End:   FrameBound{Type: Following, Offset: 1},
		}
		
		// All rows should see the entire partition
		start, end := partition.FrameBounds(0, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 3, end)
	})
	
	t.Run("Using GroupsBetween method", func(t *testing.T) {
		// Test the spec builder method
		spec := NewSpec().OrderBy("value").GroupsBetween(-2, 2)
		
		assert.Equal(t, GroupsFrame, spec.frame.Type)
		assert.Equal(t, Preceding, spec.frame.Start.Type)
		assert.Equal(t, 2, spec.frame.Start.Offset)
		assert.Equal(t, Following, spec.frame.End.Type)
		assert.Equal(t, 2, spec.frame.End.Offset)
	})
	
	t.Run("GROUPS with complex peer groups", func(t *testing.T) {
		// Create test data with multiple peer groups
		values := []int32{10, 20, 20, 30, 30, 30, 40, 40, 50}
		s := series.NewInt32Series("value", values)
		
		// Create partition
		seriesMap := map[string]series.Series{"value": s}
		indices := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
		partition := NewPartition(seriesMap, indices)
		
		// Apply ordering
		err := partition.ApplyOrder([]OrderClause{{Column: "value", Ascending: true}})
		require.NoError(t, err)
		
		// Test GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW
		spec := &FrameSpec{
			Type:  GroupsFrame,
			Start: FrameBound{Type: Preceding, Offset: 2},
			End:   FrameBound{Type: CurrentRow},
		}
		
		// Row 4 (value=30, group 2): Groups [0,2] includes rows 0-5
		start, end := partition.FrameBounds(4, spec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 6, end)
		
		// Row 7 (value=40, group 3): Groups [1,3] includes rows 1-7
		start, end = partition.FrameBounds(7, spec)
		assert.Equal(t, 1, start)
		assert.Equal(t, 8, end)
		
		// Row 8 (value=50, group 4): Groups [2,4] includes rows 3-8
		start, end = partition.FrameBounds(8, spec)
		assert.Equal(t, 3, start)
		assert.Equal(t, 9, end)
	})
}