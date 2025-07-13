package window

import (
	"testing"

	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLag(t *testing.T) {
	tests := []struct {
		name         string
		partition    Partition
		spec         *Spec
		column       string
		offset       int
		defaultValue interface{}
		expected     interface{}
		wantErr      bool
	}{
		{
			name: "lag 1 with int32",
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
			column:       "value",
			offset:       1,
			defaultValue: int32(-1),
			expected:     []int32{-1, 10, 20, 30},
		},
		{
			name: "lag 2 with int64",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt64Series("value", []int64{100, 200, 300, 400}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:       "value",
			offset:       2,
			defaultValue: int64(0),
			expected:     []int64{0, 0, 100, 200},
		},
		{
			name: "lag with string",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"name": series.NewStringSeries("name", []string{"A", "B", "C", "D"}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "name", Ascending: true}},
			},
			column:       "name",
			offset:       1,
			defaultValue: "NULL",
			expected:     []string{"NULL", "A", "B", "C"},
		},
		{
			name: "lag beyond partition",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20}),
				},
				indices:      []int{0, 1},
				orderIndices: []int{0, 1},
				size:         2,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:       "value",
			offset:       5,
			defaultValue: int32(99),
			expected:     []int32{99, 99},
		},
		{
			name: "column not found",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20}),
				},
				indices:   []int{0, 1},
				size:      2,
				isOrdered: false,
			},
			spec:     &Spec{},
			column:   "missing",
			offset:   1,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &lagFunc{
				column:       tt.column,
				offset:       tt.offset,
				defaultValue: tt.defaultValue,
				spec:         tt.spec,
			}
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
			case []string:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			}
		})
	}
}

func TestLead(t *testing.T) {
	tests := []struct {
		name         string
		partition    Partition
		spec         *Spec
		column       string
		offset       int
		defaultValue interface{}
		expected     interface{}
	}{
		{
			name: "lead 1 with int32",
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
			column:       "value",
			offset:       1,
			defaultValue: int32(-1),
			expected:     []int32{20, 30, 40, -1},
		},
		{
			name: "lead 2 with int64",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt64Series("value", []int64{100, 200, 300, 400}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:       "value",
			offset:       2,
			defaultValue: int64(0),
			expected:     []int64{300, 400, 0, 0},
		},
		{
			name: "lead with string",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"name": series.NewStringSeries("name", []string{"A", "B", "C", "D"}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "name", Ascending: true}},
			},
			column:       "name",
			offset:       1,
			defaultValue: "NULL",
			expected:     []string{"B", "C", "D", "NULL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &leadFunc{
				column:       tt.column,
				offset:       tt.offset,
				defaultValue: tt.defaultValue,
				spec:         tt.spec,
			}
			result, err := f.Compute(tt.partition)
			
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
			case []string:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			}
		})
	}
}

func TestFirstValue(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		column    string
		expected  interface{}
	}{
		{
			name: "first value int32 ordered",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{30, 10, 20, 40}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{1, 2, 0, 3}, // Sorted: 10, 20, 30, 40
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []int32{10, 10, 10, 10}, // First value in ordered sequence
		},
		{
			name: "first value string unordered",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"name": series.NewStringSeries("name", []string{"D", "B", "C", "A"}),
				},
				indices:   []int{0, 1, 2, 3},
				size:      4,
				isOrdered: false,
			},
			spec:     &Spec{},
			column:   "name",
			expected: []string{"D", "D", "D", "D"}, // First value in natural order
		},
		{
			name: "first value int64",
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
			expected: []int64{100, 100, 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &firstValueFunc{
				column: tt.column,
				spec:   tt.spec,
			}
			result, err := f.Compute(tt.partition)
			
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
			case []string:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			}
		})
	}
}

func TestLastValue(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		column    string
		expected  interface{}
	}{
		{
			name: "last value int32 ordered",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{30, 10, 20, 40}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{1, 2, 0, 3}, // Sorted: 10, 20, 30, 40
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			column:   "value",
			expected: []int32{40, 40, 40, 40}, // Last value in ordered sequence
		},
		{
			name: "last value string unordered",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"name": series.NewStringSeries("name", []string{"D", "B", "C", "A"}),
				},
				indices:   []int{0, 1, 2, 3},
				size:      4,
				isOrdered: false,
			},
			spec:     &Spec{},
			column:   "name",
			expected: []string{"A", "A", "A", "A"}, // Last value in natural order
		},
		{
			name: "last value int64",
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
			expected: []int64{300, 300, 300},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &lastValueFunc{
				column: tt.column,
				spec:   tt.spec,
			}
			result, err := f.Compute(tt.partition)
			
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
			case []string:
				for i, v := range exp {
					assert.Equal(t, v, result.Get(i))
				}
			}
		})
	}
}

func TestOffsetFunctionNames(t *testing.T) {
	functions := []struct {
		function Function
		expected string
	}{
		{&lagFunc{offset: 1}, "lag(1)"},
		{&lagFunc{offset: 3}, "lag(3)"},
		{&leadFunc{offset: 1}, "lead(1)"},
		{&leadFunc{offset: 2}, "lead(2)"},
		{&firstValueFunc{}, "first_value"},
		{&lastValueFunc{}, "last_value"},
	}

	for _, f := range functions {
		t.Run(f.expected, func(t *testing.T) {
			assert.Equal(t, f.expected, f.function.Name())
		})
	}
}

func TestOffsetWithReorderedPartition(t *testing.T) {
	// Test that lag/lead work correctly when partition indices are reordered
	partition := &WindowPartition{
		series: map[string]series.Series{
			"id":    series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
			"value": series.NewInt32Series("value", []int32{50, 30, 40, 10, 20}),
		},
		indices:      []int{0, 1, 2, 3, 4},
		orderIndices: []int{3, 4, 1, 2, 0}, // Sorted by value: 10, 20, 30, 40, 50
		size:         5,
		isOrdered:    true,
	}
	
	spec := &Spec{
		orderBy: []OrderClause{{Column: "value", Ascending: true}},
	}

	t.Run("lag with reordered indices", func(t *testing.T) {
		f := &lagFunc{
			column:       "value",
			offset:       1,
			defaultValue: int32(-1),
			spec:         spec,
		}
		result, err := f.Compute(partition)
		
		require.NoError(t, err)
		// Expected: Original positions get lagged values from sorted order
		// Position 0 (value=50, last in order) -> lag=40
		// Position 1 (value=30, third in order) -> lag=20
		// Position 2 (value=40, fourth in order) -> lag=30
		// Position 3 (value=10, first in order) -> lag=-1 (default)
		// Position 4 (value=20, second in order) -> lag=10
		expected := []int32{40, 20, 30, -1, 10}
		
		for i, v := range expected {
			assert.Equal(t, v, result.Get(i))
		}
	})
}