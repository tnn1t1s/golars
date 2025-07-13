package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWriteParquet(t *testing.T) {
	// Create test data
	df, err := frame.NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
		series.NewFloat64Series("score", []float64{95.5, 87.3, 92.1, 78.9, 88.4}),
		series.NewBooleanSeries("active", []bool{true, true, false, true, false}),
	)
	require.NoError(t, err)

	// Create temp file
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test.parquet")

	// Write to parquet
	writer := NewWriter(DefaultWriterOptions())
	err = writer.WriteFile(df, filename)
	require.NoError(t, err)

	// Check file exists
	_, err = os.Stat(filename)
	require.NoError(t, err)

	// Read back
	reader := NewReader(DefaultReaderOptions())
	result, err := reader.ReadFile(filename)
	require.NoError(t, err)

	// Verify data
	assert.Equal(t, df.Height(), result.Height())
	assert.Equal(t, df.Width(), result.Width())

	// Check column names
	assert.Equal(t, df.Columns(), result.Columns())

	// Check data types
	for i, name := range df.Columns() {
		origCol, _ := df.Column(name)
		resultCol, _ := result.Column(name)
		assert.Equal(t, origCol.DataType(), resultCol.DataType(), "column %s type mismatch", name)
		assert.Equal(t, origCol.Len(), resultCol.Len(), "column %s length mismatch", name)
		
		// Check values
		for j := 0; j < origCol.Len(); j++ {
			assert.Equal(t, origCol.Get(j), resultCol.Get(j), "value mismatch at [%d,%d]", j, i)
		}
	}
}

func TestReadParquetWithColumns(t *testing.T) {
	// Create test data with more columns
	df, err := frame.NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
		series.NewFloat64Series("score", []float64{95.5, 87.3, 92.1}),
		series.NewInt64Series("timestamp", []int64{1000, 2000, 3000}),
		series.NewStringSeries("category", []string{"A", "B", "A"}),
	)
	require.NoError(t, err)

	// Write to parquet
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_columns.parquet")
	
	err = WriteParquet(df, filename)
	require.NoError(t, err)

	// Read only specific columns
	reader := NewReader(ReaderOptions{
		Columns: []string{"id", "name", "score"},
	})
	result, err := reader.ReadFile(filename)
	require.NoError(t, err)

	// Verify only requested columns are present
	assert.Equal(t, 3, result.Width())
	assert.Equal(t, []string{"id", "name", "score"}, result.Columns())
	assert.Equal(t, df.Height(), result.Height())
}

func TestReadParquetWithRowLimit(t *testing.T) {
	// Create test data
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i)
	}
	
	df, err := frame.NewDataFrame(
		series.NewInt32Series("value", values),
	)
	require.NoError(t, err)

	// Write to parquet
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_rows.parquet")
	
	err = WriteParquet(df, filename)
	require.NoError(t, err)

	// Read with row limit
	reader := NewReader(ReaderOptions{
		NumRows: 100,
	})
	result, err := reader.ReadFile(filename)
	require.NoError(t, err)

	// Verify row limit
	assert.Equal(t, 100, result.Height())
	
	// Check values
	col, _ := result.Column("value")
	for i := 0; i < 100; i++ {
		assert.Equal(t, int32(i), col.Get(i))
	}
}

func TestParquetWithNulls(t *testing.T) {
	// Create test data with nulls
	df, err := frame.NewDataFrame(
		series.NewSeriesWithValidity("id", []int32{1, 2, 3, 4, 5}, 
			[]bool{false, false, true, false, true}, datatypes.Int32{}),
		series.NewSeriesWithValidity("name", []string{"Alice", "Bob", "", "David", ""}, 
			[]bool{false, false, true, false, true}, datatypes.String{}),
		series.NewSeriesWithValidity("score", []float64{95.5, 87.3, 0, 78.9, 0}, 
			[]bool{false, false, true, false, true}, datatypes.Float64{}),
	)
	require.NoError(t, err)

	// Write and read back
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_nulls.parquet")
	
	err = WriteParquet(df, filename)
	require.NoError(t, err)

	result, err := ReadParquet(filename)
	require.NoError(t, err)

	// Verify nulls are preserved
	for _, name := range df.Columns() {
		origCol, _ := df.Column(name)
		resultCol, _ := result.Column(name)
		
		assert.Equal(t, origCol.NullCount(), resultCol.NullCount(), "null count mismatch for %s", name)
		
		for i := 0; i < origCol.Len(); i++ {
			assert.Equal(t, origCol.IsNull(i), resultCol.IsNull(i), "null mismatch at [%d] for %s", i, name)
			if !origCol.IsNull(i) {
				assert.Equal(t, origCol.Get(i), resultCol.Get(i), "value mismatch at [%d] for %s", i, name)
			}
		}
	}
}

func TestParquetCompressionTypes(t *testing.T) {
	// Create test data
	values := make([]string, 1000)
	for i := range values {
		values[i] = "test string value that should compress well because it repeats"
	}
	
	df, err := frame.NewDataFrame(
		series.NewStringSeries("text", values),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	compressionTypes := []struct {
		name        string
		compression CompressionType
	}{
		{"none", CompressionNone},
		{"snappy", CompressionSnappy},
		{"gzip", CompressionGzip},
		// Note: zstd and lz4 might not be available in all environments
	}

	for _, tc := range compressionTypes {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(tmpDir, "test_"+tc.name+".parquet")
			
			// Write with specific compression
			writer := NewWriter(WriterOptions{
				Compression: tc.compression,
			})
			err := writer.WriteFile(df, filename)
			require.NoError(t, err)

			// Read back
			result, err := ReadParquet(filename)
			require.NoError(t, err)

			// Verify data
			assert.Equal(t, df.Height(), result.Height())
			
			// If not NONE, compressed file should generally be smaller
			// Note: for small files or already compressed data, the difference may be minimal
			if tc.compression != CompressionNone && tc.name != "gzip" {
				noneFile := filepath.Join(tmpDir, "test_none.parquet")
				compressedInfo, _ := os.Stat(filename)
				uncompressedInfo, _ := os.Stat(noneFile)
				
				if uncompressedInfo != nil {
					// Allow small variations due to metadata
					assert.LessOrEqual(t, compressedInfo.Size(), uncompressedInfo.Size()+1000, 
						"compressed file should be smaller or similar")
				}
			}
		})
	}
}

func TestEmptyDataFrame(t *testing.T) {
	// Test empty DataFrame
	df, err := frame.NewDataFrame()
	require.NoError(t, err)

	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "empty.parquet")

	// Should handle empty DataFrame
	err = WriteParquet(df, filename)
	require.NoError(t, err)

	result, err := ReadParquet(filename)
	require.NoError(t, err)

	assert.Equal(t, 0, result.Height())
	assert.Equal(t, 0, result.Width())
}

func TestLargeDataFrame(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large DataFrame test in short mode")
	}

	// Create large DataFrame
	size := 100000
	ids := make([]int64, size)
	values := make([]float64, size)
	categories := make([]string, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		values[i] = float64(i) * 1.5
		categories[i] = []string{"A", "B", "C", "D"}[i%4]
	}

	df, err := frame.NewDataFrame(
		series.NewInt64Series("id", ids),
		series.NewFloat64Series("value", values),
		series.NewStringSeries("category", categories),
	)
	require.NoError(t, err)

	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "large.parquet")

	// Write
	err = WriteParquet(df, filename)
	require.NoError(t, err)

	// Read back
	result, err := ReadParquet(filename)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, size, result.Height())
	assert.Equal(t, 3, result.Width())
	
	// Spot check some values
	idCol, _ := result.Column("id")
	assert.Equal(t, int64(0), idCol.Get(0))
	assert.Equal(t, int64(size-1), idCol.Get(size-1))
}