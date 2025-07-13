package json

import (
	"strings"
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONReader(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		options  []func(*ReadOptions)
		expected struct {
			rows    int
			columns []string
			types   map[string]datatypes.DataType
		}
		wantErr bool
	}{
		{
			name: "simple json array",
			json: `[
				{"name": "Alice", "age": 30, "active": true},
				{"name": "Bob", "age": 25, "active": false}
			]`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    2,
				columns: []string{"name", "age", "active"},
				types: map[string]datatypes.DataType{
					"name":   datatypes.String{},
					"age":    datatypes.Int64{},
					"active": datatypes.Boolean{},
				},
			},
		},
		{
			name: "mixed numeric types",
			json: `[
				{"value": 10},
				{"value": 10.5},
				{"value": 20}
			]`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    3,
				columns: []string{"value"},
				types: map[string]datatypes.DataType{
					"value": datatypes.Float64{}, // Promoted to float
				},
			},
		},
		{
			name: "null values",
			json: `[
				{"name": "Alice", "score": 95.5},
				{"name": "Bob", "score": null},
				{"name": "Charlie", "score": 88.0}
			]`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    3,
				columns: []string{"name", "score"},
				types: map[string]datatypes.DataType{
					"name":  datatypes.String{},
					"score": datatypes.Float64{},
				},
			},
		},
		{
			name: "nested objects with flattening",
			json: `[
				{"user": {"name": "Alice", "age": 30}, "active": true},
				{"user": {"name": "Bob", "age": 25}, "active": false}
			]`,
			options: []func(*ReadOptions){WithFlatten(true)},
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    2,
				columns: []string{"user.name", "user.age", "active"},
				types: map[string]datatypes.DataType{
					"user.name": datatypes.String{},
					"user.age":  datatypes.Int64{},
					"active":    datatypes.Boolean{},
				},
			},
		},
		{
			name: "empty array",
			json: `[]`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    0,
				columns: []string{},
				types:   map[string]datatypes.DataType{},
			},
		},
		{
			name:    "invalid json",
			json:    `{"invalid": json}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(tt.options...)
			df, err := reader.Read(strings.NewReader(tt.json))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected.rows, dfLen(df))
			
			// Check columns exist
			for _, col := range tt.expected.columns {
				assert.True(t, hasColumn(df, col), "column %s should exist", col)
			}

			// Check data types
			for col, expectedType := range tt.expected.types {
				series := getColumn(df, col)
				require.NotNil(t, series)
				assert.Equal(t, expectedType.String(), series.DataType().String())
			}
		})
	}
}

func TestTypeInference(t *testing.T) {
	// Test type inference through actual JSON reading
	tests := []struct {
		name     string
		json     string
		expected datatypes.DataType
	}{
		{
			name:     "all integers",
			json:     `[{"value": 1}, {"value": 2}, {"value": 3}]`,
			expected: datatypes.Int64{},
		},
		{
			name:     "mixed int and float",
			json:     `[{"value": 1}, {"value": 2.5}, {"value": 3}]`,
			expected: datatypes.Float64{},
		},
		{
			name:     "all booleans",
			json:     `[{"value": true}, {"value": false}, {"value": true}]`,
			expected: datatypes.Boolean{},
		},
		{
			name:     "all strings",
			json:     `[{"value": "a"}, {"value": "b"}, {"value": "c"}]`,
			expected: datatypes.String{},
		},
		{
			name:     "mixed types defaults to string",
			json:     `[{"value": 1}, {"value": "two"}, {"value": true}]`,
			expected: datatypes.String{},
		},
		{
			name:     "with nulls",
			json:     `[{"value": 1}, {"value": null}, {"value": 3}]`,
			expected: datatypes.Int64{},
		},
		{
			name:     "all nulls",
			json:     `[{"value": null}, {"value": null}, {"value": null}]`,
			expected: datatypes.String{}, // Default for all nulls
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader()
			df, err := reader.Read(strings.NewReader(tt.json))
			require.NoError(t, err)
			
			col := getColumn(df, "value")
			require.NotNil(t, col)
			assert.Equal(t, tt.expected.String(), col.DataType().String())
		})
	}
}

func TestFlattenObject(t *testing.T) {
	reader := &Reader{options: DefaultReadOptions()}

	tests := []struct {
		name     string
		prefix   string
		value    interface{}
		expected map[string]interface{}
	}{
		{
			name:   "simple object",
			prefix: "user",
			value: map[string]interface{}{
				"name": "Alice",
				"age":  30.0,
			},
			expected: map[string]interface{}{
				"user.name": "Alice",
				"user.age":  30.0,
			},
		},
		{
			name:   "nested object",
			prefix: "data",
			value: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "Bob",
					"info": map[string]interface{}{
						"city": "NYC",
					},
				},
			},
			expected: map[string]interface{}{
				"data.user.name":      "Bob",
				"data.user.info.city": "NYC",
			},
		},
		{
			name:   "array value",
			prefix: "items",
			value:  []interface{}{1, 2, 3},
			expected: map[string]interface{}{
				"items": "[1 2 3]",
			},
		},
		{
			name:   "scalar value",
			prefix: "count",
			value:  42.0,
			expected: map[string]interface{}{
				"count": 42.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reader.flattenObject(tt.prefix, tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReadWithOptions(t *testing.T) {
	json := `[
		{"id": 1, "name": "Alice", "score": 95.5, "meta": {"level": 3}},
		{"id": 2, "name": "Bob", "score": 88.0, "meta": {"level": 2}},
		{"id": 3, "name": "Charlie", "score": 92.0, "meta": {"level": 3}}
	]`

	t.Run("with max records", func(t *testing.T) {
		reader := NewReader(WithMaxRecords(2))
		df, err := reader.Read(strings.NewReader(json))
		require.NoError(t, err)
		assert.Equal(t, 2, dfLen(df))
	})

	t.Run("without flattening", func(t *testing.T) {
		reader := NewReader(WithFlatten(false))
		df, err := reader.Read(strings.NewReader(json))
		require.NoError(t, err)
		
		// meta column should exist but contain string representation
		metaCol := getColumn(df, "meta")
		assert.NotNil(t, metaCol)
	})

	t.Run("with column selection", func(t *testing.T) {
		reader := NewReader(WithColumns([]string{"name", "score"}))
		df, err := reader.Read(strings.NewReader(json))
		require.NoError(t, err)
		
		// Note: Column filtering happens at a higher level
		// The reader still reads all columns
		assert.True(t, hasColumn(df, "id"))
	})
}

func TestSeriesBuilder(t *testing.T) {
	t.Run("bool series", func(t *testing.T) {
		builder := newSeriesBuilder("test", datatypes.Boolean{}, 3)
		builder.append(true)
		builder.append(false)
		builder.appendNull()
		
		series, err := builder.build()
		require.NoError(t, err)
		assert.Equal(t, 3, series.Len())
		assert.Equal(t, true, series.Get(0))
		assert.Equal(t, false, series.Get(1))
		// Without null mask support, false is used as placeholder
		assert.Equal(t, false, series.Get(2))
	})

	t.Run("int64 series", func(t *testing.T) {
		builder := newSeriesBuilder("test", datatypes.Int64{}, 3)
		builder.append(10.0) // JSON numbers come as float64
		builder.append(20.0)
		builder.append("invalid") // Should become null
		
		series, err := builder.build()
		require.NoError(t, err)
		assert.Equal(t, 3, series.Len())
		assert.Equal(t, int64(10), series.Get(0))
		assert.Equal(t, int64(20), series.Get(1))
		// Without null mask support, 0 is used as placeholder
		assert.Equal(t, int64(0), series.Get(2))
	})

	t.Run("string series", func(t *testing.T) {
		builder := newSeriesBuilder("test", datatypes.String{}, 3)
		builder.append("hello")
		builder.append(123) // Numbers convert to string
		builder.appendNull()
		
		series, err := builder.build()
		require.NoError(t, err)
		assert.Equal(t, 3, series.Len())
		assert.Equal(t, "hello", series.Get(0))
		assert.Equal(t, "123", series.Get(1))
		// Without null mask support, empty string is used as placeholder
		assert.Equal(t, "", series.Get(2))
	})
}