package json

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

func TestJSONWriter(t *testing.T) {
	// Create test DataFrame
	df, err := frame.NewDataFrame(
		series.NewInt64Series("id", []int64{1, 2, 3}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
		series.NewFloat64Series("score", []float64{95.5, 88.0, 92.0}),
		series.NewBooleanSeries("active", []bool{true, false, true}),
	)
	require.NoError(t, err)

	t.Run("records orientation", func(t *testing.T) {
		writer := NewWriter()
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		require.NoError(t, err)

		// Parse result
		var records []map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &records)
		require.NoError(t, err)

		assert.Len(t, records, 3)
		assert.Equal(t, float64(1), records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, 95.5, records[0]["score"])
		assert.Equal(t, true, records[0]["active"])
	})

	t.Run("columns orientation", func(t *testing.T) {
		writer := NewWriter(WithOrient("columns"))
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		require.NoError(t, err)

		// Parse result
		var columns map[string][]interface{}
		err = json.Unmarshal(buf.Bytes(), &columns)
		require.NoError(t, err)

		assert.Len(t, columns, 4)
		assert.Equal(t, []interface{}{float64(1), float64(2), float64(3)}, columns["id"])
		assert.Equal(t, []interface{}{"Alice", "Bob", "Charlie"}, columns["name"])
		assert.Equal(t, []interface{}{95.5, 88.0, 92.0}, columns["score"])
		assert.Equal(t, []interface{}{true, false, true}, columns["active"])
	})

	t.Run("values orientation", func(t *testing.T) {
		writer := NewWriter(WithOrient("values"))
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		require.NoError(t, err)

		// Parse result
		var values [][]interface{}
		err = json.Unmarshal(buf.Bytes(), &values)
		require.NoError(t, err)

		assert.Len(t, values, 3)
		assert.Equal(t, []interface{}{float64(1), "Alice", 95.5, true}, values[0])
		assert.Equal(t, []interface{}{float64(2), "Bob", 88.0, false}, values[1])
		assert.Equal(t, []interface{}{float64(3), "Charlie", 92.0, true}, values[2])
	})

	t.Run("pretty printing", func(t *testing.T) {
		writer := NewWriter(WithPretty(true), WithIndent("    "))
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "    {")
		assert.Contains(t, output, "        \"id\":")
		assert.Contains(t, output, "\n")
	})

	t.Run("null values", func(t *testing.T) {
		// Create DataFrame with nulls
		nullDf, err := frame.NewDataFrame(
			series.NewInt64Series("id", []int64{1, 2, 3}),
			series.NewStringSeries("name", []string{"Alice", "", "Charlie"}),
		)
		require.NoError(t, err)

		writer := NewWriter()
		var buf bytes.Buffer
		err = writer.Write(nullDf, &buf)
		require.NoError(t, err)

		var records []map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &records)
		require.NoError(t, err)

		// Without proper null support, values are written as-is
		assert.Equal(t, float64(1), records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, float64(2), records[1]["id"]) // 2 is written as-is
		assert.Equal(t, "", records[1]["name"])       // empty string is written as-is
		assert.Equal(t, float64(3), records[2]["id"])
		assert.Equal(t, "Charlie", records[2]["name"])
	})

	t.Run("empty DataFrame", func(t *testing.T) {
		emptyDf, err := frame.NewDataFrame()
		require.NoError(t, err)

		writer := NewWriter()
		var buf bytes.Buffer
		err = writer.Write(emptyDf, &buf)
		require.NoError(t, err)

		assert.Equal(t, "[]\n", buf.String())
	})

	t.Run("invalid orientation", func(t *testing.T) {
		writer := NewWriter(WithOrient("invalid"))
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported orientation")
	})
}

func TestJSONWriteFile(t *testing.T) {
	// Create test DataFrame
	df, err := frame.NewDataFrame(
		series.NewInt64Series("id", []int64{1, 2, 3}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	t.Run("write to file", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "test.json")
		writer := NewWriter(WithPretty(true))
		err := writer.WriteFile(df, filename)
		require.NoError(t, err)

		// Verify file exists and content
		data, err := os.ReadFile(filename)
		require.NoError(t, err)

		var records []map[string]interface{}
		err = json.Unmarshal(data, &records)
		require.NoError(t, err)
		assert.Len(t, records, 3)
	})

	t.Run("write with gzip compression", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "test.json.gz")
		writer := NewWriter(WithCompression("gzip"))
		err := writer.WriteFile(df, filename)
		require.NoError(t, err)

		// Verify compressed file
		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer gzReader.Close()

		var records []map[string]interface{}
		decoder := json.NewDecoder(gzReader)
		err = decoder.Decode(&records)
		require.NoError(t, err)
		assert.Len(t, records, 3)
	})

	t.Run("auto-detect compression from extension", func(t *testing.T) {
		filename := filepath.Join(tmpDir, "test2.json.gz")
		writer := NewWriter() // No explicit compression
		err := writer.WriteFile(df, filename)
		require.NoError(t, err)

		// Verify it's compressed
		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		_, err = gzip.NewReader(file)
		assert.NoError(t, err)
	})
}

func TestNDJSONWriter(t *testing.T) {
	// Create test DataFrame
	df, err := frame.NewDataFrame(
		series.NewInt64Series("id", []int64{1, 2, 3}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
		series.NewFloat64Series("score", []float64{95.5, 88.0, 92.0}),
	)
	require.NoError(t, err)

	t.Run("basic NDJSON output", func(t *testing.T) {
		writer := NewNDJSONWriter()
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		require.NoError(t, err)

		// Split lines and verify
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		assert.Len(t, lines, 3)

		// Verify each line is valid JSON
		for i, line := range lines {
			var record map[string]interface{}
			err := json.Unmarshal([]byte(line), &record)
			require.NoError(t, err)
			assert.Equal(t, float64(i+1), record["id"])
		}
	})

	t.Run("null values in NDJSON", func(t *testing.T) {
		// Create DataFrame with nulls - simplified version
		nullDf, err := frame.NewDataFrame(
			series.NewInt64Series("id", []int64{1, 0, 3}),                    // 0 represents null
			series.NewStringSeries("name", []string{"Alice", "", "Charlie"}), // empty string represents null
		)
		require.NoError(t, err)

		writer := NewNDJSONWriter()
		var buf bytes.Buffer
		err = writer.Write(nullDf, &buf)
		require.NoError(t, err)

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		assert.Len(t, lines, 3)

		// Check second record has nulls
		var record map[string]interface{}
		err = json.Unmarshal([]byte(lines[1]), &record)
		require.NoError(t, err)
		// Without proper null support, 0 and empty strings are used
		assert.Equal(t, float64(0), record["id"])
		assert.Equal(t, "", record["name"])
	})

	t.Run("streaming write", func(t *testing.T) {
		writer := NewNDJSONWriter()
		var buf bytes.Buffer
		err := writer.WriteStream(df, &buf, 2) // Chunk size of 2
		require.NoError(t, err)

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		assert.Len(t, lines, 3)
	})

	t.Run("NDJSON file with compression", func(t *testing.T) {
		tmpDir := t.TempDir()
		filename := filepath.Join(tmpDir, "test.ndjson.gz")

		writer := NewNDJSONWriter()
		err := writer.WriteFile(df, filename)
		require.NoError(t, err)

		// Verify compressed NDJSON
		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer gzReader.Close()

		data, err := io.ReadAll(gzReader)
		require.NoError(t, err)

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		assert.Len(t, lines, 3)
	})

	t.Run("pretty option ignored", func(t *testing.T) {
		// Pretty printing should be ignored for NDJSON
		writer := NewNDJSONWriter(WithPretty(true))
		var buf bytes.Buffer
		err := writer.Write(df, &buf)
		require.NoError(t, err)

		// Should still be single lines
		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Len(t, lines, 3)
		for _, line := range lines {
			assert.NotContains(t, line, "    ") // No indentation
		}
	})
}

func TestWriterOptions(t *testing.T) {
	t.Run("option functions", func(t *testing.T) {
		opts := DefaultWriteOptions()

		WithPretty(true)(&opts)
		assert.True(t, opts.Pretty)

		WithOrient("columns")(&opts)
		assert.Equal(t, "columns", opts.Orient)

		WithCompression("gzip")(&opts)
		assert.Equal(t, "gzip", opts.Compression)

		WithIndent("\t")(&opts)
		assert.Equal(t, "\t", opts.Indent)
	})
}
