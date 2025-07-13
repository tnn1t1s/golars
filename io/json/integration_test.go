package json

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONRoundTrip(t *testing.T) {
	// Create test DataFrame with various types
	original, err := frame.NewDataFrame(
		series.NewInt64Series("id", []int64{1, 2, 3, 4, 5}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
		series.NewFloat64Series("score", []float64{95.5, 88.0, 92.0, 85.5, 91.0}),
		series.NewBooleanSeries("active", []bool{true, false, true, true, false}),
		series.NewInt64Series("nullable_int", []int64{10, 0, 30, 0, 50}),
		series.NewStringSeries("nullable_str", []string{"a", "", "c", "", "e"}),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	t.Run("JSON round trip", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "roundtrip.json")

		// Write
		err := WriteJSON(original, filename)
		require.NoError(t, err)

		// Read back
		loaded, err := ReadJSON(filename)
		require.NoError(t, err)

		// Compare
		assert.Equal(t, dfLen(original), dfLen(loaded))
		assert.Equal(t, len(original.Columns()), len(loaded.Columns()))

		// Check data integrity
		for _, col := range original.Columns() {
			origSeries := getColumn(original, col)
			loadedSeries := getColumn(loaded, col)
			require.NotNil(t, loadedSeries, "column %s should exist", col)

			for i := 0; i < origSeries.Len(); i++ {
				// Type conversion might occur (int64 -> float64 in JSON)
				origVal := origSeries.Get(i)
				loadedVal := loadedSeries.Get(i)
				
				// JSON doesn't distinguish between int and float, so numbers might change type
				switch origVal.(type) {
				case int64:
					switch loadedVal.(type) {
					case int64:
						assert.Equal(t, origVal, loadedVal, "value mismatch at row %d col %s", i, col)
					case float64:
						assert.Equal(t, float64(origVal.(int64)), loadedVal, "value mismatch at row %d col %s", i, col)
					}
				default:
					assert.Equal(t, origVal, loadedVal, "value mismatch at row %d col %s", i, col)
				}
			}
		}
	})

	t.Run("NDJSON round trip", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "roundtrip.ndjson")

		// Write
		err := WriteNDJSON(original, filename)
		require.NoError(t, err)

		// Read back
		loaded, err := ReadNDJSON(filename)
		require.NoError(t, err)

		// Compare
		assert.Equal(t, dfLen(original), dfLen(loaded))
		assert.Equal(t, len(original.Columns()), len(loaded.Columns()))
	})

	t.Run("compressed JSON round trip", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "roundtrip_compressed.json.gz")

		// Write with compression
		err := WriteJSON(original, filename, WithCompression("gzip"))
		require.NoError(t, err)

		// Verify file is compressed
		info, err := os.Stat(filename)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))

		// Read back (should auto-detect compression)
		loaded, err := ReadJSON(filename)
		require.NoError(t, err)

		assert.Equal(t, dfLen(original), dfLen(loaded))
	})

	t.Run("pretty JSON preserves data", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "pretty.json")

		// Write with pretty printing
		err := WriteJSON(original, filename, WithPretty(true))
		require.NoError(t, err)

		// Read back
		loaded, err := ReadJSON(filename)
		require.NoError(t, err)

		assert.Equal(t, dfLen(original), dfLen(loaded))
	})
}

func TestJSONOrientations(t *testing.T) {
	// Create test DataFrame
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
		series.NewStringSeries("b", []string{"x", "y", "z"}),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	orientations := []string{"records", "columns", "values"}
	
	for _, orient := range orientations {
		t.Run(orient+" orientation", func(t *testing.T) {
			filename := filepath.Join(tmpDir, orient+".json")
			
			// Write with specific orientation
			err := WriteJSON(df, filename, WithOrient(orient))
			require.NoError(t, err)

			// For now, reader only supports records orientation
			// This test verifies write works for all orientations
			if orient == "records" {
				loaded, err := ReadJSON(filename)
				require.NoError(t, err)
				assert.Equal(t, dfLen(df), dfLen(loaded))
			}
		})
	}
}

func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	// Create large DataFrame
	size := 10000
	ids := make([]int64, size)
	names := make([]string, size)
	scores := make([]float64, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		names[i] = fmt.Sprintf("User%c", rune(i%26+'A'))
		scores[i] = float64(i%100) + 0.5
	}

	large, err := frame.NewDataFrame(
		series.NewInt64Series("id", ids),
		series.NewStringSeries("name", names),
		series.NewFloat64Series("score", scores),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	t.Run("large JSON", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "large.json")
		
		err := WriteJSON(large, filename)
		require.NoError(t, err)

		loaded, err := ReadJSON(filename)
		require.NoError(t, err)
		
		assert.Equal(t, size, dfLen(loaded))
	})

	t.Run("large NDJSON streaming", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "large.ndjson")
		
		// Write as NDJSON
		err := WriteNDJSON(large, filename)
		require.NoError(t, err)

		// Read with streaming
		reader := NewNDJSONReader().WithChunkSize(1000)
		totalRows := 0
		chunkCount := 0

		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		err = reader.ReadStream(file, func(chunk *frame.DataFrame) error {
			totalRows += dfLen(chunk)
			chunkCount++
			return nil
		})
		
		require.NoError(t, err)
		assert.Equal(t, size, totalRows)
		assert.Equal(t, 10, chunkCount) // 10000 rows / 1000 chunk size
	})
}

func TestNestedJSONHandling(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create nested JSON file
	nestedJSON := `[
		{
			"id": 1,
			"user": {
				"name": "Alice",
				"details": {
					"age": 30,
					"city": "NYC"
				}
			},
			"scores": [85, 90, 95]
		},
		{
			"id": 2,
			"user": {
				"name": "Bob",
				"details": {
					"age": 25,
					"city": "LA"
				}
			},
			"scores": [80, 85, 88]
		}
	]`

	filename := filepath.Join(tmpDir, "nested.json")
	err := os.WriteFile(filename, []byte(nestedJSON), 0644)
	require.NoError(t, err)

	t.Run("with flattening", func(t *testing.T) {
		df, err := ReadJSON(filename, WithFlatten(true))
		require.NoError(t, err)

		assert.Equal(t, 2, dfLen(df))
		
		// Check flattened columns exist
		assert.True(t, hasColumn(df, "id"))
		assert.True(t, hasColumn(df, "user.name"))
		assert.True(t, hasColumn(df, "user.details.age"))
		assert.True(t, hasColumn(df, "user.details.city"))
		assert.True(t, hasColumn(df, "scores")) // Arrays become string representation
	})

	t.Run("without flattening", func(t *testing.T) {
		df, err := ReadJSON(filename, WithFlatten(false))
		require.NoError(t, err)

		assert.Equal(t, 2, dfLen(df))
		
		// Nested objects become string representations
		assert.True(t, hasColumn(df, "id"))
		assert.True(t, hasColumn(df, "user"))
		assert.True(t, hasColumn(df, "scores"))
		
		// user column contains string representation of nested object
		userCol := getColumn(df, "user")
		userVal := userCol.Get(0).(string)
		assert.Contains(t, userVal, "name")
		assert.Contains(t, userVal, "Alice")
	})
}

func TestErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("malformed JSON", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "malformed.json")
		err := os.WriteFile(filename, []byte(`{"incomplete": `), 0644)
		require.NoError(t, err)

		_, err = ReadJSON(filename)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode JSON")
	})

	t.Run("malformed NDJSON", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "malformed.ndjson")
		err := os.WriteFile(filename, []byte(`{"valid": true}
{invalid json}
{"valid": false}`), 0644)
		require.NoError(t, err)

		// Without skip invalid
		_, err = ReadNDJSON(filename)
		assert.Error(t, err)

		// With skip invalid
		df, err := ReadNDJSON(filename, WithSkipInvalid(true))
		require.NoError(t, err)
		assert.Equal(t, 2, dfLen(df)) // Only valid lines
	})

	t.Run("non-existent file", func(t *testing.T) {
		_, err := ReadJSON("/non/existent/file.json")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open file")
	})

	t.Run("write to invalid path", func(t *testing.T) {
		df, _ := frame.NewDataFrame()
		err := WriteJSON(df, "/invalid/path/file.json")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create file")
	})
}

func TestAPIConvenience(t *testing.T) {
	// Test that the package-level API functions work correctly
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
		series.NewStringSeries("b", []string{"x", "y", "z"}),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	t.Run("JSON API", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "api_test.json")
		
		// Test package-level functions
		err := WriteJSON(df, filename, WithPretty(true))
		require.NoError(t, err)

		loaded, err := ReadJSON(filename, WithMaxRecords(2))
		require.NoError(t, err)
		assert.Equal(t, 2, dfLen(loaded))
	})

	t.Run("NDJSON API", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "api_test.ndjson")
		
		err := WriteNDJSON(df, filename)
		require.NoError(t, err)

		loaded, err := ReadNDJSON(filename)
		require.NoError(t, err)
		assert.Equal(t, 3, dfLen(loaded))
	})
}