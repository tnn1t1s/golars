package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
)

func TestReadWriteCSV(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	t.Run("BasicReadWrite", func(t *testing.T) {
		// Create test DataFrame
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
			series.NewInt64Series("age", []int64{25, 30, 35}),
			series.NewFloat64Series("score", []float64{95.5, 87.0, 92.3}),
		)
		assert.NoError(t, err)

		// Write to file
		filename := filepath.Join(tempDir, "test.csv")
		err = WriteCSV(df, filename)
		assert.NoError(t, err)

		// Read back
		result, err := ReadCSV(filename)
		assert.NoError(t, err)

		// Verify
		assert.Equal(t, df.Height(), result.Height())
		assert.Equal(t, df.Width(), result.Width())
		assert.Equal(t, df.Columns(), result.Columns())
	})

	t.Run("ReadWithOptions", func(t *testing.T) {
		// Create CSV file with custom delimiter
		filename := filepath.Join(tempDir, "semicolon.csv")
		content := `name;age;score
Alice;25;95.5
Bob;30;87.0`

		err := os.WriteFile(filename, []byte(content), 0644)
		assert.NoError(t, err)

		// Read with delimiter option
		df, err := ReadCSV(filename, WithDelimiter(';'))
		assert.NoError(t, err)
		assert.Equal(t, 2, df.Height())
		assert.Equal(t, 3, df.Width())
	})

	t.Run("WriteWithOptions", func(t *testing.T) {
		// Create test DataFrame
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewFloat64Series("score", []float64{95.567, 87.123}),
		)
		assert.NoError(t, err)

		// Write with options
		filename := filepath.Join(tempDir, "formatted.csv")
		err = WriteCSV(df, filename,
			WithWriteDelimiter(';'),
			WithFloatFormat("%.1f"),
			WithWriteHeader(false),
		)
		assert.NoError(t, err)

		// Read file and verify format
		content, err := os.ReadFile(filename)
		assert.NoError(t, err)
		expected := "Alice;95.6\nBob;87.1\n"
		assert.Equal(t, expected, string(content))
	})

	t.Run("SelectColumns", func(t *testing.T) {
		// Create CSV file
		filename := filepath.Join(tempDir, "columns.csv")
		content := `name,age,score,city
Alice,25,95.5,NYC
Bob,30,87.0,LA`

		err := os.WriteFile(filename, []byte(content), 0644)
		assert.NoError(t, err)

		// Read only specific columns
		df, err := ReadCSV(filename, WithColumns([]string{"name", "score"}))
		assert.NoError(t, err)
		assert.Equal(t, 2, df.Width())
		assert.Equal(t, []string{"name", "score"}, df.Columns())
	})

	t.Run("SkipRows", func(t *testing.T) {
		// Create CSV file with comments
		filename := filepath.Join(tempDir, "skip.csv")
		content := `# This is a comment
# Another comment
name,age,score
Alice,25,95.5
Bob,30,87.0`

		err := os.WriteFile(filename, []byte(content), 0644)
		assert.NoError(t, err)

		// Skip first two rows
		df, err := ReadCSV(filename, WithSkipRows(2))
		assert.NoError(t, err)
		assert.Equal(t, 2, df.Height())
	})

	t.Run("CustomNullValues", func(t *testing.T) {
		// Create CSV with custom null values
		filename := filepath.Join(tempDir, "nulls.csv")
		content := `name,age,score
Alice,25,95.5
Bob,NA,87.0
Charlie,35,N/A`

		err := os.WriteFile(filename, []byte(content), 0644)
		assert.NoError(t, err)

		// Read with custom null values
		df, err := ReadCSV(filename, WithNullValues([]string{"NA", "N/A"}))
		assert.NoError(t, err)

		ageCol, _ := df.Column("age")
		assert.True(t, ageCol.IsNull(1))

		scoreCol, _ := df.Column("score")
		assert.True(t, scoreCol.IsNull(2))
	})
}

func TestCSVEdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("NonExistentFile", func(t *testing.T) {
		_, err := ReadCSV(filepath.Join(tempDir, "nonexistent.csv"))
		assert.Error(t, err)
	})

	t.Run("EmptyDataFrame", func(t *testing.T) {
		df, err := frame.NewDataFrame()
		assert.NoError(t, err)

		filename := filepath.Join(tempDir, "empty.csv")
		err = WriteCSV(df, filename)
		assert.NoError(t, err)

		// Read back
		result, err := ReadCSV(filename)
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Height())
		assert.Equal(t, 0, result.Width())
	})

	t.Run("InvalidWritePath", func(t *testing.T) {
		df, _ := frame.NewDataFrame(
			series.NewStringSeries("col", []string{"value"}),
		)

		// Try to write to invalid path
		err := WriteCSV(df, "/invalid/path/file.csv")
		assert.Error(t, err)
	})
}