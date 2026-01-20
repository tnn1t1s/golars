package frame

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/series"
)

func TestStack(t *testing.T) {
	t.Run("Basic stack", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("id", []string{"A", "B"}),
			series.NewInt64Series("x", []int64{1, 2}),
			series.NewInt64Series("y", []int64{3, 4}),
		)
		require.NoError(t, err)

		stacked, err := df.Stack("x", "y")
		require.NoError(t, err)

		// Should have 2 * 2 = 4 rows
		assert.Equal(t, 4, stacked.Height())
		assert.Equal(t, []string{"id", "level_1", "value"}, stacked.Columns())

		// Check ID column is repeated
		idCol, err := stacked.Column("id")
		require.NoError(t, err)
		assert.Equal(t, "A", idCol.Get(0))
		assert.Equal(t, "A", idCol.Get(1))
		assert.Equal(t, "B", idCol.Get(2))
		assert.Equal(t, "B", idCol.Get(3))

		// Check level column
		levelCol, err := stacked.Column("level_1")
		require.NoError(t, err)
		assert.Equal(t, "x", levelCol.Get(0))
		assert.Equal(t, "y", levelCol.Get(1))
		assert.Equal(t, "x", levelCol.Get(2))
		assert.Equal(t, "y", levelCol.Get(3))

		// Check values
		valCol, err := stacked.Column("value")
		require.NoError(t, err)
		assert.Equal(t, int64(1), valCol.Get(0))
		assert.Equal(t, int64(3), valCol.Get(1))
		assert.Equal(t, int64(2), valCol.Get(2))
		assert.Equal(t, int64(4), valCol.Get(3))
	})

	t.Run("Stack single column", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewInt64Series("score", []int64{90, 85}),
		)
		require.NoError(t, err)

		stacked, err := df.Stack("score")
		require.NoError(t, err)

		assert.Equal(t, 2, stacked.Height())
		assert.Equal(t, []string{"name", "level_1", "value"}, stacked.Columns())
	})

	t.Run("Error cases", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
		)
		require.NoError(t, err)

		// No columns specified
		_, err = df.Stack()
		assert.Error(t, err)

		// Invalid column
		_, err = df.Stack("nonexistent")
		assert.Error(t, err)
	})
}

func TestUnstack(t *testing.T) {
	t.Run("Basic unstack", func(t *testing.T) {
		// Create a stacked DataFrame
		df, err := NewDataFrame(
			series.NewStringSeries("id", []string{"A", "A", "B", "B"}),
			series.NewStringSeries("variable", []string{"x", "y", "x", "y"}),
			series.NewInt64Series("value", []int64{1, 3, 2, 4}),
		)
		require.NoError(t, err)

		unstacked, err := df.Unstack("variable", nil)
		require.NoError(t, err)

		assert.Equal(t, 2, unstacked.Height())
		assert.Equal(t, 3, len(unstacked.Columns())) // id, x, y

		// Check values match original
		xCol, err := unstacked.Column("x")
		require.NoError(t, err)
		assert.Equal(t, int64(1), xCol.Get(0))
		assert.Equal(t, int64(2), xCol.Get(1))

		yCol, err := unstacked.Column("y")
		require.NoError(t, err)
		assert.Equal(t, int64(3), yCol.Get(0))
		assert.Equal(t, int64(4), yCol.Get(1))
	})

	t.Run("Unstack with fill value", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("key", []string{"A", "B"}),
			series.NewStringSeries("col", []string{"x", "y"}),
			series.NewInt64Series("val", []int64{10, 20}),
		)
		require.NoError(t, err)

		unstacked, err := df.Unstack("col", int64(0))
		require.NoError(t, err)

		// Should have missing combinations filled with 0
		xCol, err := unstacked.Column("x")
		require.NoError(t, err)
		assert.Equal(t, int64(10), xCol.Get(0))
		assert.Equal(t, int64(0), xCol.Get(1))

		yCol, err := unstacked.Column("y")
		require.NoError(t, err)
		assert.Equal(t, int64(0), yCol.Get(0))
		assert.Equal(t, int64(20), yCol.Get(1))
	})
}

func TestTranspose(t *testing.T) {
	t.Run("Basic transpose", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewInt64Series("age", []int64{25, 30}),
			series.NewFloat64Series("score", []float64{90.5, 85.5}),
		)
		require.NoError(t, err)

		transposed, err := df.Transpose()
		require.NoError(t, err)

		// Should have 3 rows (original columns) and 3 columns (index + 2 original rows)
		assert.Equal(t, 3, transposed.Height())
		assert.Equal(t, 3, len(transposed.Columns())) // index, row_0, row_1

		// Check index column contains original column names
		indexCol, err := transposed.Column("index")
		require.NoError(t, err)
		assert.Equal(t, "name", indexCol.Get(0))
		assert.Equal(t, "age", indexCol.Get(1))
		assert.Equal(t, "score", indexCol.Get(2))

		// Check values (all values are converted to strings in transpose)
		row0, err := transposed.Column("row_0")
		require.NoError(t, err)
		assert.Equal(t, "Alice", row0.Get(0))
		assert.Equal(t, "25", row0.Get(1))
		assert.Equal(t, "90.5", row0.Get(2))

		row1, err := transposed.Column("row_1")
		require.NoError(t, err)
		assert.Equal(t, "Bob", row1.Get(0))
		assert.Equal(t, "30", row1.Get(1))
		assert.Equal(t, "85.5", row1.Get(2))
	})

	t.Run("Transpose empty DataFrame", func(t *testing.T) {
		df, err := NewDataFrame()
		require.NoError(t, err)

		_, err = df.Transpose()
		assert.Error(t, err)
	})

	t.Run("Transpose single column", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("values", []int64{1, 2, 3}),
		)
		require.NoError(t, err)

		transposed, err := df.Transpose()
		require.NoError(t, err)

		assert.Equal(t, 1, transposed.Height())
		assert.Equal(t, 4, len(transposed.Columns())) // index + 3 rows

		indexCol, err := transposed.Column("index")
		require.NoError(t, err)
		assert.Equal(t, "values", indexCol.Get(0))
	})
}
