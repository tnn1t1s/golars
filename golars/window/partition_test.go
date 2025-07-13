package window

import (
	"testing"

	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowPartition(t *testing.T) {
	tests := []struct {
		name     string
		series   map[string]series.Series
		indices  []int
		expected struct {
			size    int
			ordered bool
		}
	}{
		{
			name: "basic partition",
			series: map[string]series.Series{
				"value": series.NewInt32Series("value", []int32{10, 20, 30}),
			},
			indices: []int{0, 1, 2},
			expected: struct {
				size    int
				ordered bool
			}{size: 3, ordered: false},
		},
		{
			name: "empty partition",
			series: map[string]series.Series{
				"value": series.NewInt32Series("value", []int32{}),
			},
			indices: []int{},
			expected: struct {
				size    int
				ordered bool
			}{size: 0, ordered: false},
		},
		{
			name: "single row partition",
			series: map[string]series.Series{
				"value": series.NewInt32Series("value", []int32{42}),
			},
			indices: []int{0},
			expected: struct {
				size    int
				ordered bool
			}{size: 1, ordered: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &WindowPartition{
				series:    tt.series,
				indices:   tt.indices,
				size:      len(tt.indices),
				isOrdered: false,
			}

			assert.Equal(t, tt.expected.size, p.Size())
			assert.Equal(t, tt.expected.ordered, p.IsOrdered())
			assert.Equal(t, tt.indices, p.Indices())
		})
	}
}

func TestWindowPartitionColumn(t *testing.T) {
	p := &WindowPartition{
		series: map[string]series.Series{
			"col1": series.NewInt32Series("col1", []int32{10, 20, 30}),
			"col2": series.NewStringSeries("col2", []string{"a", "b", "c"}),
		},
		indices: []int{0, 1, 2},
		size:    3,
	}

	tests := []struct {
		name    string
		column  string
		wantErr bool
	}{
		{"existing column int32", "col1", false},
		{"existing column string", "col2", false},
		{"non-existent column", "col3", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := p.Column(tt.column)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, s)
				assert.Equal(t, tt.column, s.Name())
			}
		})
	}
}

// Note: Sort is an internal method not exposed by the Partition interface
// These tests verify the sorting behavior through window function results

// Note: FrameIndices is now FrameBounds in the Partition interface
// These frame boundary tests are covered by aggregate function tests with frames

func TestComplexPartitionScenarios(t *testing.T) {
	t.Run("partition with subset of indices", func(t *testing.T) {
		// Test when partition only contains a subset of rows
		p := &WindowPartition{
			series: map[string]series.Series{
				"id":    series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
				"value": series.NewInt32Series("value", []int32{10, 20, 30, 40, 50}),
			},
			indices: []int{1, 3}, // Only rows at index 1 and 3
			size:    2,
		}

		// Verify size and indices
		assert.Equal(t, 2, p.Size())
		assert.Equal(t, []int{1, 3}, p.Indices())
		
		// Verify we can access the correct values
		valueSeries, err := p.Column("value")
		require.NoError(t, err)
		assert.Equal(t, int32(20), valueSeries.Get(1))
		assert.Equal(t, int32(40), valueSeries.Get(3))
	})

	t.Run("partition preserves row mapping", func(t *testing.T) {
		// Create a partition with specific indices
		p := &WindowPartition{
			series: map[string]series.Series{
				"group": series.NewStringSeries("group", []string{"A", "B", "A", "B", "A"}),
				"value": series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
			},
			indices:      []int{0, 2, 4}, // Only rows with group "A"
			orderIndices: []int{0, 2, 4}, // Already in order
			size:         3,
			isOrdered:    true,
		}

		// Verify partition correctly maps back to original indices
		for _, idx := range p.Indices() {
			s, _ := p.Column("group")
			assert.Equal(t, "A", s.Get(idx))
			
			s, _ = p.Column("value")
			expectedValue := int32(idx + 1) // Values are 1-based
			assert.Equal(t, expectedValue, s.Get(idx))
		}
	})
}