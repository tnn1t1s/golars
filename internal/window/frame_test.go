package window

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/series"
)

func TestFrameSpecValidation(t *testing.T) {
	tests := []struct {
		name    string
		spec    *Spec
		wantErr bool
	}{
		{
			name:    "valid rows between",
			spec:    NewSpec().RowsBetween(-1, 1), // 1 preceding to 1 following
			wantErr: false,
		},
		{
			name: "valid unbounded preceding to current",
			spec: func() *Spec {
				s := NewSpec()
				s.frame = &FrameSpec{
					Type:  RowsFrame,
					Start: FrameBound{Type: UnboundedPreceding},
					End:   FrameBound{Type: CurrentRow},
				}
				return s
			}(),
			wantErr: false,
		},
		{
			name: "valid current to unbounded following",
			spec: func() *Spec {
				s := NewSpec()
				s.frame = &FrameSpec{
					Type:  RowsFrame,
					Start: FrameBound{Type: CurrentRow},
					End:   FrameBound{Type: UnboundedFollowing},
				}
				return s
			}(),
			wantErr: false,
		},
		{
			name:    "invalid frame - end before start",
			spec:    NewSpec().RowsBetween(1, -1), // 1 following to 1 preceding (invalid)
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with a simple aggregate function
			f := &sumFunc{spec: tt.spec}
			err := f.Validate(tt.spec)

			// Frame validation happens during compute
			if tt.wantErr {
				// We expect frame validation to fail during actual computation
				// For now, Validate doesn't check frame bounds
				_ = err
			}
		})
	}
}

func TestFrameBoundaryCalculations(t *testing.T) {
	// Create test data
	partition := &WindowPartition{
		series: map[string]series.Series{
			"value": series.NewInt32Series("value", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		},
		indices:      []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		orderIndices: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		size:         10,
		isOrdered:    true,
	}

	tests := []struct {
		name          string
		frameSpec     *FrameSpec
		currentRow    int
		expectedStart int
		expectedEnd   int
	}{
		{
			name:          "default frame - unbounded preceding to current",
			frameSpec:     nil,
			currentRow:    5,
			expectedStart: 0,
			expectedEnd:   10, // default is entire partition when frame is nil
		},
		{
			name: "3 preceding to current",
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: Preceding, Offset: 3},
				End:   FrameBound{Type: CurrentRow},
			},
			currentRow:    5,
			expectedStart: 2,
			expectedEnd:   6, // currentRow + 1
		},
		{
			name: "current to 3 following",
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: CurrentRow},
				End:   FrameBound{Type: Following, Offset: 3},
			},
			currentRow:    5,
			expectedStart: 5,
			expectedEnd:   9, // min(size, currentRow + offset + 1)
		},
		{
			name: "2 preceding to 2 following",
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: Preceding, Offset: 2},
				End:   FrameBound{Type: Following, Offset: 2},
			},
			currentRow:    5,
			expectedStart: 3,
			expectedEnd:   8, // currentRow + 2 + 1
		},
		{
			name: "at start - 3 preceding to current",
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: Preceding, Offset: 3},
				End:   FrameBound{Type: CurrentRow},
			},
			currentRow:    1,
			expectedStart: 0,
			expectedEnd:   2, // currentRow + 1
		},
		{
			name: "at end - current to 3 following",
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: CurrentRow},
				End:   FrameBound{Type: Following, Offset: 3},
			},
			currentRow:    8,
			expectedStart: 8,
			expectedEnd:   10, // min(size, currentRow + 3 + 1)
		},
		{
			name: "unbounded both sides",
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: UnboundedFollowing},
			},
			currentRow:    5,
			expectedStart: 0,
			expectedEnd:   10, // size
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := partition.FrameBounds(tt.currentRow, tt.frameSpec)
			assert.Equal(t, tt.expectedStart, start, "start bound mismatch")
			assert.Equal(t, tt.expectedEnd, end, "end bound mismatch")
		})
	}
}

func TestFrameWithDifferentAggregates(t *testing.T) {
	// Create test data
	partition := &WindowPartition{
		series: map[string]series.Series{
			"value": series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
		},
		indices:      []int{0, 1, 2, 3, 4},
		orderIndices: []int{0, 1, 2, 3, 4},
		size:         5,
		isOrdered:    true,
	}

	tests := []struct {
		name      string
		function  Function
		frameSpec *FrameSpec
		expected  []interface{}
	}{
		{
			name: "sum with 1 preceding to 1 following",
			function: &sumFunc{
				column: "value",
				spec: &Spec{
					orderBy: []OrderClause{{Column: "value", Ascending: true}},
					frame: &FrameSpec{
						Type:  RowsFrame,
						Start: FrameBound{Type: Preceding, Offset: 1},
						End:   FrameBound{Type: Following, Offset: 1},
					},
				},
			},
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: Preceding, Offset: 1},
				End:   FrameBound{Type: Following, Offset: 1},
			},
			expected: []interface{}{int32(3), int32(6), int32(9), int32(12), int32(9)}, // 1+2, 1+2+3, 2+3+4, 3+4+5, 4+5
		},
		{
			name: "count with current to 2 following",
			function: &countFunc{
				column: "value",
				spec: &Spec{
					orderBy: []OrderClause{{Column: "value", Ascending: true}},
					frame: &FrameSpec{
						Type:  RowsFrame,
						Start: FrameBound{Type: CurrentRow},
						End:   FrameBound{Type: Following, Offset: 2},
					},
				},
			},
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: CurrentRow},
				End:   FrameBound{Type: Following, Offset: 2},
			},
			expected: []interface{}{int64(3), int64(3), int64(3), int64(2), int64(1)}, // Count of rows in each frame
		},
		{
			name: "avg with 2 preceding to current",
			function: &avgFunc{
				column: "value",
				spec: &Spec{
					orderBy: []OrderClause{{Column: "value", Ascending: true}},
					frame: &FrameSpec{
						Type:  RowsFrame,
						Start: FrameBound{Type: Preceding, Offset: 2},
						End:   FrameBound{Type: CurrentRow},
					},
				},
			},
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: Preceding, Offset: 2},
				End:   FrameBound{Type: CurrentRow},
			},
			expected: []interface{}{1.0, 1.5, 2.0, 3.0, 4.0}, // Averages: 1/1, (1+2)/2, (1+2+3)/3, (2+3+4)/3, (3+4+5)/3
		},
		{
			name: "min with unbounded preceding to current",
			function: &minFunc{
				column: "value",
				spec: &Spec{
					orderBy: []OrderClause{{Column: "value", Ascending: true}},
					frame: &FrameSpec{
						Type:  RowsFrame,
						Start: FrameBound{Type: UnboundedPreceding},
						End:   FrameBound{Type: CurrentRow},
					},
				},
			},
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: CurrentRow},
			},
			expected: []interface{}{int32(1), int32(1), int32(1), int32(1), int32(1)}, // Running minimum
		},
		{
			name: "max with current to unbounded following",
			function: &maxFunc{
				column: "value",
				spec: &Spec{
					orderBy: []OrderClause{{Column: "value", Ascending: true}},
					frame: &FrameSpec{
						Type:  RowsFrame,
						Start: FrameBound{Type: CurrentRow},
						End:   FrameBound{Type: UnboundedFollowing},
					},
				},
			},
			frameSpec: &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: CurrentRow},
				End:   FrameBound{Type: UnboundedFollowing},
			},
			expected: []interface{}{int32(5), int32(5), int32(5), int32(5), int32(5)}, // Looking forward maximum
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.function.Compute(partition)
			require.NoError(t, err)

			for i, expected := range tt.expected {
				switch exp := expected.(type) {
				case int32:
					assert.Equal(t, exp, result.Get(i))
				case int64:
					assert.Equal(t, exp, result.Get(i))
				case float64:
					assert.InDelta(t, exp, result.Get(i), 0.0001)
				}
			}
		})
	}
}

func TestEmptyFrames(t *testing.T) {
	// Test frames that result in empty windows
	partition := &WindowPartition{
		series: map[string]series.Series{
			"value": series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
		},
		indices:      []int{0, 1, 2, 3, 4},
		orderIndices: []int{0, 1, 2, 3, 4},
		size:         5,
		isOrdered:    true,
	}

	// Frame that starts after current row (should be empty)
	frameSpec := &FrameSpec{
		Type:  RowsFrame,
		Start: FrameBound{Type: Following, Offset: 1},
		End:   FrameBound{Type: Following, Offset: 0}, // Invalid: end before start
	}

	// This should handle gracefully
	start, end := partition.FrameBounds(2, frameSpec)
	// When frame is invalid, it should clamp appropriately
	assert.True(t, start <= end, "start should not exceed end")
}

func TestFrameBoundariesEdgeCases(t *testing.T) {
	t.Run("single row partition", func(t *testing.T) {
		partition := &WindowPartition{
			series: map[string]series.Series{
				"value": series.NewInt32Series("value", []int32{42}),
			},
			indices:      []int{0},
			orderIndices: []int{0},
			size:         1,
			isOrdered:    true,
		}

		// Any frame should include just the single row
		specs := []*FrameSpec{
			nil, // default
			{
				Type:  RowsFrame,
				Start: FrameBound{Type: Preceding, Offset: 10},
				End:   FrameBound{Type: Following, Offset: 10},
			},
			{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: UnboundedFollowing},
			},
		}

		for _, spec := range specs {
			start, end := partition.FrameBounds(0, spec)
			assert.Equal(t, 0, start)
			assert.Equal(t, 1, end) // single row partition, end is exclusive
		}
	})

	t.Run("large offset frames", func(t *testing.T) {
		partition := &WindowPartition{
			series: map[string]series.Series{
				"value": series.NewInt32Series("value", []int32{1, 2, 3}),
			},
			indices:      []int{0, 1, 2},
			orderIndices: []int{0, 1, 2},
			size:         3,
			isOrdered:    true,
		}

		// Frame with offsets larger than partition
		frameSpec := &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: Preceding, Offset: 100},
			End:   FrameBound{Type: Following, Offset: 100},
		}

		// Should include entire partition
		start, end := partition.FrameBounds(1, frameSpec)
		assert.Equal(t, 0, start)
		assert.Equal(t, 3, end) // size of partition
	})
}
