package csv

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
)

func TestCSVWriter(t *testing.T) {
	t.Run("BasicWrite", func(t *testing.T) {
		// Create test DataFrame
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
			series.NewInt64Series("age", []int64{25, 30, 35}),
			series.NewFloat64Series("score", []float64{95.5, 87.0, 92.3}),
		)
		assert.NoError(t, err)

		// Write to buffer
		var buf bytes.Buffer
		writer := NewWriter(&buf, DefaultWriteOptions())
		err = writer.Write(df)
		assert.NoError(t, err)

		// Check output
		expected := `name,age,score
Alice,25,95.5
Bob,30,87
Charlie,35,92.3
`
		assert.Equal(t, expected, buf.String())
	})

	t.Run("WriteWithNulls", func(t *testing.T) {
		// Create DataFrame with nulls
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
			series.NewSeriesWithValidity("age", []int64{25, 0, 35}, []bool{true, false, true}, datatypes.Int64{}),
			series.NewSeriesWithValidity("score", []float64{95.5, 87.0, 0}, []bool{true, true, false}, datatypes.Float64{}),
		)
		assert.NoError(t, err)

		// Write with custom null value
		var buf bytes.Buffer
		opts := DefaultWriteOptions()
		opts.NullValue = "NULL"
		writer := NewWriter(&buf, opts)
		err = writer.Write(df)
		assert.NoError(t, err)

		expected := `name,age,score
Alice,25,95.5
Bob,NULL,87
Charlie,35,NULL
`
		assert.Equal(t, expected, buf.String())
	})

	t.Run("CustomDelimiter", func(t *testing.T) {
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewInt64Series("age", []int64{25, 30}),
		)
		assert.NoError(t, err)

		var buf bytes.Buffer
		opts := DefaultWriteOptions()
		opts.Delimiter = ';'
		writer := NewWriter(&buf, opts)
		err = writer.Write(df)
		assert.NoError(t, err)

		expected := `name;age
Alice;25
Bob;30
`
		assert.Equal(t, expected, buf.String())
	})

	t.Run("NoHeader", func(t *testing.T) {
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewInt64Series("age", []int64{25, 30}),
		)
		assert.NoError(t, err)

		var buf bytes.Buffer
		opts := DefaultWriteOptions()
		opts.Header = false
		writer := NewWriter(&buf, opts)
		err = writer.Write(df)
		assert.NoError(t, err)

		expected := `Alice,25
Bob,30
`
		assert.Equal(t, expected, buf.String())
	})

	t.Run("FloatFormat", func(t *testing.T) {
		df, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewFloat64Series("score", []float64{95.567, 87.123}),
			series.NewFloat32Series("rating", []float32{4.567, 3.123}),
		)
		assert.NoError(t, err)

		var buf bytes.Buffer
		opts := DefaultWriteOptions()
		opts.FloatFormat = "%.2f"
		writer := NewWriter(&buf, opts)
		err = writer.Write(df)
		assert.NoError(t, err)

		expected := `name,score,rating
Alice,95.57,4.57
Bob,87.12,3.12
`
		assert.Equal(t, expected, buf.String())
	})

	t.Run("EmptyDataFrame", func(t *testing.T) {
		df, err := frame.NewDataFrame()
		assert.NoError(t, err)

		var buf bytes.Buffer
		writer := NewWriter(&buf, DefaultWriteOptions())
		err = writer.Write(df)
		assert.NoError(t, err)
		assert.Equal(t, "", buf.String())
	})

	t.Run("AllTypes", func(t *testing.T) {
		// Test all supported data types
		df, err := frame.NewDataFrame(
			series.NewBooleanSeries("bool", []bool{true, false}),
			series.NewInt8Series("int8", []int8{-128, 127}),
			series.NewInt16Series("int16", []int16{-32768, 32767}),
			series.NewInt32Series("int32", []int32{-2147483648, 2147483647}),
			series.NewInt64Series("int64", []int64{-9223372036854775808, 9223372036854775807}),
			series.NewUInt8Series("uint8", []uint8{0, 255}),
			series.NewUInt16Series("uint16", []uint16{0, 65535}),
			series.NewUInt32Series("uint32", []uint32{0, 4294967295}),
			series.NewUInt64Series("uint64", []uint64{0, 18446744073709551615}),
			series.NewFloat32Series("float32", []float32{1.23, 4.56}),
			series.NewFloat64Series("float64", []float64{7.89, 10.11}),
			series.NewStringSeries("string", []string{"hello", "world"}),
		)
		assert.NoError(t, err)

		var buf bytes.Buffer
		writer := NewWriter(&buf, DefaultWriteOptions())
		err = writer.Write(df)
		assert.NoError(t, err)

		// Just verify it wrote without errors and has correct number of lines
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		assert.Equal(t, 3, len(lines)) // Header + 2 data rows
	})
}

func TestRoundTrip(t *testing.T) {
	// Test that we can write and read back the same data
	t.Run("BasicRoundTrip", func(t *testing.T) {
		// Create original DataFrame
		original, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
			series.NewInt64Series("age", []int64{25, 30, 35}),
			series.NewFloat64Series("score", []float64{95.5, 87.0, 92.3}),
			series.NewBooleanSeries("active", []bool{true, false, true}),
		)
		assert.NoError(t, err)

		// Write to buffer
		var buf bytes.Buffer
		writer := NewWriter(&buf, DefaultWriteOptions())
		err = writer.Write(original)
		assert.NoError(t, err)

		// Read back
		reader := NewReader(&buf, DefaultReadOptions())
		result, err := reader.Read()
		assert.NoError(t, err)

		// Compare dimensions
		assert.Equal(t, original.Height(), result.Height())
		assert.Equal(t, original.Width(), result.Width())
		assert.Equal(t, original.Columns(), result.Columns())

		// Compare values
		for _, colName := range original.Columns() {
			origCol, _ := original.Column(colName)
			resultCol, _ := result.Column(colName)

			for i := 0; i < original.Height(); i++ {
				if origCol.IsNull(i) {
					assert.True(t, resultCol.IsNull(i))
				} else {
					// Note: integers become int64 after round trip
					origVal := origCol.Get(i)
					resultVal := resultCol.Get(i)
					
					switch v := origVal.(type) {
					case int64:
						assert.Equal(t, v, resultVal)
					case float64:
						assert.Equal(t, v, resultVal)
					case bool:
						assert.Equal(t, v, resultVal)
					case string:
						assert.Equal(t, v, resultVal)
					}
				}
			}
		}
	})

	t.Run("RoundTripWithNulls", func(t *testing.T) {
		// Create DataFrame with nulls
		original, err := frame.NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
			series.NewSeriesWithValidity("age", []int64{25, 0, 35}, []bool{true, false, true}, datatypes.Int64{}),
			series.NewSeriesWithValidity("score", []float64{95.5, 87.0, 0}, []bool{true, true, false}, datatypes.Float64{}),
		)
		assert.NoError(t, err)

		// Write with specific null value
		var buf bytes.Buffer
		writeOpts := DefaultWriteOptions()
		writeOpts.NullValue = "NA"
		writer := NewWriter(&buf, writeOpts)
		err = writer.Write(original)
		assert.NoError(t, err)

		// Read back
		readOpts := DefaultReadOptions()
		readOpts.NullValues = []string{"NA"}
		reader := NewReader(&buf, readOpts)
		result, err := reader.Read()
		assert.NoError(t, err)

		// Check nulls are preserved
		ageCol, _ := result.Column("age")
		assert.False(t, ageCol.IsNull(0))
		assert.True(t, ageCol.IsNull(1))
		assert.False(t, ageCol.IsNull(2))

		scoreCol, _ := result.Column("score")
		assert.False(t, scoreCol.IsNull(0))
		assert.False(t, scoreCol.IsNull(1))
		assert.True(t, scoreCol.IsNull(2))
	})
}

func BenchmarkCSVWriter(b *testing.B) {
	// Create large DataFrame
	size := 10000
	names := make([]string, size)
	ages := make([]int64, size)
	scores := make([]float64, size)

	for i := 0; i < size; i++ {
		names[i] = fmt.Sprintf("Person%d", i)
		ages[i] = int64(20 + i%50)
		scores[i] = float64(50 + i%50) + 0.5
	}

	df, _ := frame.NewDataFrame(
		series.NewStringSeries("name", names),
		series.NewInt64Series("age", ages),
		series.NewFloat64Series("score", scores),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := NewWriter(&buf, DefaultWriteOptions())
		_ = writer.Write(df)
	}
}