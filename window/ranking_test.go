package window

import (
	"testing"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowNumber(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		ordered   bool
		expected  []int64
	}{
		{
			name: "unordered partition",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30}),
				},
				indices:   []int{0, 1, 2},
				size:      3,
				isOrdered: false,
			},
			expected: []int64{1, 2, 3},
		},
		{
			name: "ordered partition",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{30, 10, 20}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{1, 2, 0}, // Sorted order: 10, 20, 30
				size:         3,
				isOrdered:    true,
			},
			expected: []int64{3, 1, 2}, // Original positions get their row numbers
		},
		{
			name: "single row",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{42}),
				},
				indices:   []int{0},
				size:      1,
				isOrdered: false,
			},
			expected: []int64{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &rowNumberFunc{}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.Equal(t, expected, result.Get(i))
			}
		})
	}
}

func TestRank(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		expected  []int64
		wantErr   bool
	}{
		{
			name: "no ties",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{0, 1, 2},
				size:         3,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []int64{1, 2, 3},
		},
		{
			name: "with ties",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 20, 30}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []int64{1, 2, 2, 4}, // Skip rank 3 due to tie
		},
		{
			name: "all same values",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{50, 50, 50}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{0, 1, 2},
				size:         3,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []int64{1, 1, 1},
		},
		{
			name: "no order by",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30}),
				},
				indices:   []int{0, 1, 2},
				size:      3,
				isOrdered: false,
			},
			spec:    &Spec{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &rankFunc{spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.Equal(t, expected, result.Get(i))
			}
		})
	}
}

func TestDenseRank(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		expected  []int64
	}{
		{
			name: "no ties",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 30}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{0, 1, 2},
				size:         3,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []int64{1, 2, 3},
		},
		{
			name: "with ties",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 20, 30}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []int64{1, 2, 2, 3}, // No gap after tie
		},
		{
			name: "multiple tie groups",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 10, 20, 20, 30}),
				},
				indices:      []int{0, 1, 2, 3, 4},
				orderIndices: []int{0, 1, 2, 3, 4},
				size:         5,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []int64{1, 1, 2, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &denseRankFunc{spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.Equal(t, expected, result.Get(i))
			}
		})
	}
}

func TestPercentRank(t *testing.T) {
	tests := []struct {
		name      string
		partition Partition
		spec      *Spec
		expected  []float64
	}{
		{
			name: "basic percent rank",
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
			expected: []float64{0.0, 1.0/3.0, 2.0/3.0, 1.0},
		},
		{
			name: "single row",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{42}),
				},
				indices:      []int{0},
				orderIndices: []int{0},
				size:         1,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []float64{0.0},
		},
		{
			name: "with ties",
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{10, 20, 20, 30}),
				},
				indices:      []int{0, 1, 2, 3},
				orderIndices: []int{0, 1, 2, 3},
				size:         4,
				isOrdered:    true,
			},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			expected: []float64{0.0, 1.0/3.0, 1.0/3.0, 1.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &percentRankFunc{spec: tt.spec}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.InDelta(t, expected, result.Get(i), 0.0001)
			}
		})
	}
}

func TestNTile(t *testing.T) {
	tests := []struct {
		name      string
		buckets   int
		partition Partition
		expected  []int64
	}{
		{
			name:    "even distribution",
			buckets: 3,
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{1, 2, 3, 4, 5, 6}),
				},
				indices:      []int{0, 1, 2, 3, 4, 5},
				orderIndices: []int{0, 1, 2, 3, 4, 5},
				size:         6,
				isOrdered:    true,
			},
			expected: []int64{1, 1, 2, 2, 3, 3},
		},
		{
			name:    "uneven distribution",
			buckets: 3,
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
				},
				indices:      []int{0, 1, 2, 3, 4},
				orderIndices: []int{0, 1, 2, 3, 4},
				size:         5,
				isOrdered:    true,
			},
			expected: []int64{1, 1, 2, 2, 3}, // First two buckets get extra row
		},
		{
			name:    "single bucket",
			buckets: 1,
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{1, 2, 3}),
				},
				indices:   []int{0, 1, 2},
				size:      3,
				isOrdered: false,
			},
			expected: []int64{1, 1, 1},
		},
		{
			name:    "more buckets than rows",
			buckets: 5,
			partition: &WindowPartition{
				series: map[string]series.Series{
					"value": series.NewInt32Series("value", []int32{1, 2, 3}),
				},
				indices:      []int{0, 1, 2},
				orderIndices: []int{0, 1, 2},
				size:         3,
				isOrdered:    true,
			},
			expected: []int64{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &ntileFunc{buckets: tt.buckets}
			result, err := f.Compute(tt.partition)
			
			require.NoError(t, err)
			require.Equal(t, len(tt.expected), result.Len())
			
			for i, expected := range tt.expected {
				assert.Equal(t, expected, result.Get(i))
			}
		})
	}
}

func TestRankingFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		spec     *Spec
		wantErr  bool
	}{
		{
			name:     "row_number no order",
			function: &rowNumberFunc{},
			spec:     &Spec{},
			wantErr:  false, // ROW_NUMBER doesn't require ORDER BY
		},
		{
			name:     "rank no order",
			function: &rankFunc{},
			spec:     &Spec{},
			wantErr:  true, // RANK requires ORDER BY
		},
		{
			name:     "rank with order",
			function: &rankFunc{},
			spec: &Spec{
				orderBy: []OrderClause{{Column: "value", Ascending: true}},
			},
			wantErr: false,
		},
		{
			name:     "dense_rank no order",
			function: &denseRankFunc{},
			spec:     &Spec{},
			wantErr:  true, // DENSE_RANK requires ORDER BY
		},
		{
			name:     "percent_rank no order",
			function: &percentRankFunc{},
			spec:     &Spec{},
			wantErr:  true, // PERCENT_RANK requires ORDER BY
		},
		{
			name:     "ntile no order",
			function: &ntileFunc{buckets: 4},
			spec:     &Spec{},
			wantErr:  false, // NTILE doesn't require ORDER BY
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.function.Validate(tt.spec)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRankingFunctionDataTypes(t *testing.T) {
	functions := []struct {
		name     string
		function Function
		expected datatypes.DataType
	}{
		{"row_number", &rowNumberFunc{}, datatypes.Int64{}},
		{"rank", &rankFunc{}, datatypes.Int64{}},
		{"dense_rank", &denseRankFunc{}, datatypes.Int64{}},
		{"percent_rank", &percentRankFunc{}, datatypes.Float64{}},
		{"ntile", &ntileFunc{buckets: 4}, datatypes.Int64{}},
	}

	for _, f := range functions {
		t.Run(f.name, func(t *testing.T) {
			result := f.function.DataType(datatypes.Unknown{})
			assert.Equal(t, f.expected, result)
		})
	}
}

func TestRankingFunctionNames(t *testing.T) {
	functions := []struct {
		function Function
		expected string
	}{
		{&rowNumberFunc{}, "row_number"},
		{&rankFunc{}, "rank"},
		{&denseRankFunc{}, "dense_rank"},
		{&percentRankFunc{}, "percent_rank"},
		{&ntileFunc{buckets: 4}, "ntile(4)"},
	}

	for _, f := range functions {
		t.Run(f.expected, func(t *testing.T) {
			assert.Equal(t, f.expected, f.function.Name())
		})
	}
}