package json

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

func TestNDJSONReader(t *testing.T) {
	tests := []struct {
		name     string
		ndjson   string
		options  []func(*ReadOptions)
		expected struct {
			rows    int
			columns []string
			types   map[string]datatypes.DataType
		}
		wantErr bool
	}{
		{
			name: "simple NDJSON",
			ndjson: `{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    3,
				columns: []string{"id", "name", "age"},
				types: map[string]datatypes.DataType{
					"id":   datatypes.Int64{},
					"name": datatypes.String{},
					"age":  datatypes.Int64{},
				},
			},
		},
		{
			name: "mixed types with promotion",
			ndjson: `{"value": 10}
{"value": 10.5}
{"value": 20}`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    3,
				columns: []string{"value"},
				types: map[string]datatypes.DataType{
					"value": datatypes.Float64{},
				},
			},
		},
		{
			name: "empty lines ignored",
			ndjson: `{"id": 1, "name": "Alice"}

{"id": 2, "name": "Bob"}

{"id": 3, "name": "Charlie"}`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    3,
				columns: []string{"id", "name"},
			},
		},
		{
			name: "with null values",
			ndjson: `{"id": 1, "score": 95.5}
{"id": 2, "score": null}
{"id": 3, "score": 88.0}`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    3,
				columns: []string{"id", "score"},
				types: map[string]datatypes.DataType{
					"id":    datatypes.Int64{},
					"score": datatypes.Float64{},
				},
			},
		},
		{
			name: "nested objects with flattening",
			ndjson: `{"user": {"name": "Alice", "age": 30}, "active": true}
{"user": {"name": "Bob", "age": 25}, "active": false}`,
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
			name:    "invalid JSON",
			ndjson:  `{"valid": true}\n{invalid json}\n{"valid": false}`,
			wantErr: true,
		},
		{
			name: "skip invalid lines",
			ndjson: `{"id": 1, "valid": true}
{invalid json}
{"id": 2, "valid": true}`,
			options: []func(*ReadOptions){WithSkipInvalid(true)},
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    2,
				columns: []string{"id", "valid"},
			},
		},
		{
			name:    "max records limit",
			ndjson:  strings.Repeat(`{"id": 1}`+"\n", 10),
			options: []func(*ReadOptions){WithMaxRecords(5)},
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    5,
				columns: []string{"id"},
			},
		},
		{
			name:   "empty input",
			ndjson: "",
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows:    0,
				columns: []string{},
			},
		},
		{
			name: "inconsistent schemas",
			ndjson: `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "score": 95.5}`,
			expected: struct {
				rows    int
				columns []string
				types   map[string]datatypes.DataType
			}{
				rows: 3,
				// All columns should be present
				columns: []string{"id", "name", "age", "score"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewNDJSONReader(tt.options...)
			df, err := reader.Read(strings.NewReader(tt.ndjson))

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

func TestNDJSONReaderChunking(t *testing.T) {
	// Create large NDJSON data
	var buf bytes.Buffer
	for i := 1; i <= 100; i++ {
		buf.WriteString(`{"id": `)
		buf.WriteString(fmt.Sprintf("%d", i))
		buf.WriteString(`, "value": `)
		buf.WriteString(fmt.Sprintf("%d", i*10))
		buf.WriteString(`}`)
		buf.WriteString("\n")
	}

	reader := NewNDJSONReader().WithChunkSize(10)
	df, err := reader.Read(&buf)
	require.NoError(t, err)
	assert.Equal(t, 100, dfLen(df))
}

func TestNDJSONReaderStreaming(t *testing.T) {
	ndjson := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
{"id": 3, "name": "Charlie"}
{"id": 4, "name": "David"}
{"id": 5, "name": "Eve"}`

	t.Run("streaming with chunks", func(t *testing.T) {
		reader := NewNDJSONReader().WithChunkSize(2)
		chunkCount := 0
		totalRows := 0

		err := reader.ReadStream(strings.NewReader(ndjson), func(df *frame.DataFrame) error {
			chunkCount++
			totalRows += dfLen(df)

			// Verify each chunk has correct structure
			assert.True(t, hasColumn(df, "id"))
			assert.True(t, hasColumn(df, "name"))

			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 3, chunkCount) // 5 records with chunk size 2 = 3 chunks
		assert.Equal(t, 5, totalRows)
	})

	t.Run("streaming with error in callback", func(t *testing.T) {
		reader := NewNDJSONReader().WithChunkSize(2)
		processedChunks := 0

		err := reader.ReadStream(strings.NewReader(ndjson), func(df *frame.DataFrame) error {
			processedChunks++
			if processedChunks == 2 {
				return assert.AnError
			}
			return nil
		})

		assert.Error(t, err)
		assert.Equal(t, 2, processedChunks)
	})

	t.Run("streaming with schema evolution", func(t *testing.T) {
		// NDJSON with evolving schema
		evolvingNDJSON := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 30, "active": true}`

		reader := NewNDJSONReader()
		schemas := make([][]string, 0)

		err := reader.ReadStream(strings.NewReader(evolvingNDJSON), func(df *frame.DataFrame) error {
			cols := df.Columns()
			schemas = append(schemas, cols)
			return nil
		})

		require.NoError(t, err)
		// Each chunk should have the columns present in its records
		assert.Len(t, schemas, 1) // All records in one chunk
	})
}

func TestNDJSONWithColumnSelection(t *testing.T) {
	ndjson := `{"id": 1, "name": "Alice", "age": 30, "score": 95.5}
{"id": 2, "name": "Bob", "age": 25, "score": 88.0}
{"id": 3, "name": "Charlie", "age": 35, "score": 92.0}`

	reader := NewNDJSONReader(WithColumns([]string{"name", "score"}))
	df, err := reader.Read(strings.NewReader(ndjson))
	require.NoError(t, err)

	// Should only have selected columns
	assert.Equal(t, 2, len(df.Columns()))
	assert.True(t, hasColumn(df, "name"))
	assert.True(t, hasColumn(df, "score"))
	assert.False(t, hasColumn(df, "id"))
	assert.False(t, hasColumn(df, "age"))
}

func TestNDJSONLargeLines(t *testing.T) {
	// Test handling of large JSON lines
	largeValue := strings.Repeat("x", 10000)
	ndjson := `{"id": 1, "data": "` + largeValue + `"}
{"id": 2, "data": "small"}`

	reader := NewNDJSONReader()
	df, err := reader.Read(strings.NewReader(ndjson))
	require.NoError(t, err)
	assert.Equal(t, 2, dfLen(df))

	// Verify large value was read correctly
	dataCol := getColumn(df, "data")
	require.NotNil(t, dataCol)
	assert.Equal(t, largeValue, dataCol.Get(0))
}

func TestNDJSONSchemaInference(t *testing.T) {
	t.Run("infer from first chunk", func(t *testing.T) {
		// Create NDJSON where first few records don't have all fields
		ndjson := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
{"id": 3, "name": "Charlie", "age": 35}
{"id": 4, "name": "David", "age": 40, "score": 95.5}`

		reader := NewNDJSONReader(WithInferSchema(true)).WithChunkSize(2)
		df, err := reader.Read(strings.NewReader(ndjson))
		require.NoError(t, err)

		// Schema should be inferred from first chunk (2 records)
		// So it might miss some columns that appear later
		assert.Equal(t, 4, dfLen(df))
		assert.True(t, hasColumn(df, "id"))
		assert.True(t, hasColumn(df, "name"))
	})

	t.Run("no schema inference", func(t *testing.T) {
		ndjson := `{"value": 1}
{"value": 1.5}
{"value": 2}`

		reader := NewNDJSONReader(WithInferSchema(false))
		df, err := reader.Read(strings.NewReader(ndjson))
		require.NoError(t, err)

		// Without inference, mixed numeric types become strings
		valueCol := getColumn(df, "value")
		require.NotNil(t, valueCol)
		// The behavior depends on implementation
	})
}
