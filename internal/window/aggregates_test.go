package window

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSum(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		column    string
		expected  interface{}
		wantErr   bool
	}{
		{
			name: "sum int32 - no frame",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40}),
				},
				indices:   []int{0, 1, 2, 3},
				size:      4,
				isOrdered: false,
			},
			spec:     &Spec{},
			column:   "value",
			expected: []int32{100, 100, 100, 100}, // Sum over entire partition
		},
		{
			name: "sum int32 - running total",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []int32{10, 30, 60, 100}, // Running total
		},
		{
			name: "sum int64",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt64Series("value", []int64{100, 200, 300}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{0, 1, 2},
				size:         3,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []int64{100, 300, 600},
		},
		{
			name: "sum float64",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewFloat64Series("value", []float64{1.5, 2.5, 3.5}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{0, 1, 2},
				size:         3,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []float64{1.5, 4.0, 7.5},
		},
		{
			name: "column not found",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20}),
				},
				indices: []int{0, 1},
				size:    2,
			},
			spec:     &Spec{},
			column:   "missing",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &sumFunc{column: tt.column, spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			
			require.NoError(t, err)
			
			switch exp := tt.expected.(type) {
			case []int32:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			case []int64:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			case []float64:
				for i, v := range exp {
					assert.InDelta(t, v, result.Get(i), 0.0001)
				}
			}
		})
	}
}

func TestAvg(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		column    string
		expected  []float64
	}{
		{
			name: "avg int32 - no frame",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40}),
				},
				indices:   []int{0, 1, 2, 3},
				size:      4,
				isOrdered: false,
			},
			spec:     &Spec{},
			column:   "value",
			expected: []float64{25.0, 25.0, 25.0, 25.0}, // Average over entire partition
		},
		{
			name: "avg int32 - running average",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []float64{10.0, 15.0, 20.0, 25.0}, // Running average
		},
		{
			name: "avg different types",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"int64val": series.NewInt64Series("int64val", []int64{100, 200}),
					"floatval": series.NewFloat64Series("floatval", []float64{1.5, 2.5}),
				},
				indices:      []int{0, 1},
				orderIndices: []int{0, 1},
				size:         2,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "int64val", Ascending: true}},
			},
			column:   "int64val",
			expected: []float64{100.0, 150.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &avgFunc{column: tt.column, spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.InDelta(t, expected, result.Get(i), 0.0001)
			}
		})
	}
}

func TestMinMax(t *testing.T) {
	tests := []struct {
		name         string
		partition    Partition
		spec         *Spec
		column       string
		expectedMin  interface{}
		expectedMax  interface{}
	}{
		{
			name: "min/max int32",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{30, 10, 40, 20}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{1, 3, 0, 2}, // Sorted: 10, 20, 30, 40
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:      "value",
			expectedMin: []int32{30, 10, 10, 10}, // Running min
			expectedMax: []int32{30, 30, 40, 40}, // Running max
		},
		{
			name: "min/max float64",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewFloat64Series("value", []float64{3.5, 1.5, 4.5, 2.5}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{1, 3, 0, 2}, // Sorted: 1.5, 2.5, 3.5, 4.5
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:      "value",
			expectedMin: []float64{3.5, 1.5, 1.5, 1.5},
			expectedMax: []float64{3.5, 3.5, 4.5, 4.5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_min", func(t *testing.T) {
			f := &minFunc{column: tt.column, spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			
			switch exp := tt.expectedMin.(type) {
			case []int32:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			case []float64:
				for i, v := range exp {
					assert.InDelta(t, v, result.Get(i), 0.0001)
				}
			}
		})
		
		t.Run(tt.name+"_max", func(t *testing.T) {
			f := &maxFunc{column: tt.column, spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			
			switch exp := tt.expectedMax.(type) {
			case []int32:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			case []float64:
				for i, v := range exp {
					assert.InDelta(t, v, result.Get(i), 0.0001)
				}
			}
		})
	}
}

func TestCount(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		column    string
		expected  []int64
	}{
		{
			name: "count all rows",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40}),
				},
				indices:   []int{0, 1, 2, 3},
				size:      4,
				isOrdered: false,
			},
			spec:     &Spec{},
			column:   "value",
			expected: []int64{4, 4, 4, 4}, // Count over entire partition
		},
		{
			name: "running count",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []int64{1, 2, 3, 4}, // Running count
		},
		{
			name: "count with frame",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30, 40, 50}),
				},
				indices:      []int{0, 1, 2, 3, 4},
				orderIndices: []int{0, 1, 2, 3, 4},
				size:         5,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
				frame: &FrameSpec{
					Type:  RowsFrame,
					Start: FrameBound{Type: Preceding, Offset: 2},
					End:   FrameBound{Type: CurrentRow},
				},
			},
			column:   "value",
			expected: []int64{1, 2, 3, 3, 3}, // 3-row window
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &countFunc{column: tt.column, spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.Equal(t, expected, result.Get(i))
			}
		})
	}
}

func TestAggregateWithFrames(t *testing.T) {
	// Test with specific frame boundaries
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
		name     string
		spec     *Spec
		expected []int32
	}{
		{
			name: "rows between 1 preceding and 1 following",
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
				frame: &FrameSpec{
					Type:  RowsFrame,
					Start: FrameBound{Type: Preceding, Offset: 1},
					End:   FrameBound{Type: Following, Offset: 1},
				},
			},
			expected: []int32{3, 6, 9, 12, 9}, // Centered 3-row sum
		},
		{
			name: "rows between 2 preceding and current row",
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
				frame: &FrameSpec{
					Type:  RowsFrame,
					Start: FrameBound{Type: Preceding, Offset: 2},
					End:   FrameBound{Type: CurrentRow},
				},
			},
			expected: []int32{1, 3, 6, 9, 12}, // 3-row trailing sum
		},
		{
			name: "rows between current row and 2 following",
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
				frame: &FrameSpec{
					Type:  RowsFrame,
					Start: FrameBound{Type: CurrentRow},
					End:   FrameBound{Type: Following, Offset: 2},
				},
			},
			expected: []int32{6, 9, 12, 9, 5}, // 3-row leading sum
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &sumFunc{column: "value", spec: tt.spec}
			result, err := f.Compute(partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.Equal(t, expected, result.Get(i))
			}
		})
	}
}

func TestAggregateDataTypes(t *testing.T) {
	functions := []struct {
		name     string
		function Function
		input    datatypes.DataType
		expected datatypes.DataType
	}{
		{"sum_int32", &sumFunc{}, datatypes.Int32{}, datatypes.Int32{}},
		{"sum_float64", &sumFunc{}, datatypes.Float64{}, datatypes.Float64{}},
		{"avg_int32", &avgFunc{}, datatypes.Int32{}, datatypes.Float64{}},
		{"avg_float64", &avgFunc{}, datatypes.Float64{}, datatypes.Float64{}},
		{"min_int32", &minFunc{}, datatypes.Int32{}, datatypes.Int32{}},
		{"max_int32", &maxFunc{}, datatypes.Int32{}, datatypes.Int32{}},
		{"count_any", &countFunc{}, datatypes.String{}, datatypes.Int64{}},
	}

	for _, f := range functions {
		t.Run(f.name, func(t *testing.T) {
			result := f.function.DataType(f.input)
			assert.Equal(t, f.expected, result)
		})
	}
}