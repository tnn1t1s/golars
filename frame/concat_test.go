package frame

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcatVertical(t *testing.T) {
	t.Run("Basic vertical concatenation", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2, 3}),
			series.NewFloat64Series("b", []float64{1.1, 2.2, 3.3}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("a", []int64{4, 5, 6}),
			series.NewFloat64Series("b", []float64{4.4, 5.5, 6.6}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{Axis: 0})
		require.NoError(t, err)

		// Check dimensions
		assert.Equal(t, 6, result.Height()) // 3 + 3 rows
		assert.Equal(t, 2, result.Width())  // Same columns

		// Check column a
		colA, err := result.Column("a")
		require.NoError(t, err)
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(2), colA.Get(1))
		assert.Equal(t, int64(3), colA.Get(2))
		assert.Equal(t, int64(4), colA.Get(3))
		assert.Equal(t, int64(5), colA.Get(4))
		assert.Equal(t, int64(6), colA.Get(5))

		// Check column b
		colB, err := result.Column("b")
		require.NoError(t, err)
		assert.Equal(t, 1.1, colB.Get(0))
		assert.Equal(t, 2.2, colB.Get(1))
		assert.Equal(t, 3.3, colB.Get(2))
		assert.Equal(t, 4.4, colB.Get(3))
		assert.Equal(t, 5.5, colB.Get(4))
		assert.Equal(t, 6.6, colB.Get(5))
	})

	t.Run("Vertical concat with different columns - outer join", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
			series.NewInt64Series("b", []int64{3, 4}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("b", []int64{5, 6}),
			series.NewInt64Series("c", []int64{7, 8}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{
			Axis: 0,
			Join: "outer",
		})
		require.NoError(t, err)

		// Check dimensions
		assert.Equal(t, 4, result.Height()) // 2 + 2 rows
		assert.Equal(t, 3, result.Width())  // a, b, c columns

		// Check column a (has nulls for df2)
		colA, err := result.Column("a")
		require.NoError(t, err)
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(2), colA.Get(1))
		assert.True(t, colA.IsNull(2))
		assert.True(t, colA.IsNull(3))

		// Check column b (present in both)
		colB, err := result.Column("b")
		require.NoError(t, err)
		assert.Equal(t, int64(3), colB.Get(0))
		assert.Equal(t, int64(4), colB.Get(1))
		assert.Equal(t, int64(5), colB.Get(2))
		assert.Equal(t, int64(6), colB.Get(3))

		// Check column c (has nulls for df1)
		colC, err := result.Column("c")
		require.NoError(t, err)
		assert.True(t, colC.IsNull(0))
		assert.True(t, colC.IsNull(1))
		assert.Equal(t, int64(7), colC.Get(2))
		assert.Equal(t, int64(8), colC.Get(3))
	})

	t.Run("Vertical concat with different columns - inner join", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
			series.NewInt64Series("b", []int64{3, 4}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("b", []int64{5, 6}),
			series.NewInt64Series("c", []int64{7, 8}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{
			Axis: 0,
			Join: "inner",
		})
		require.NoError(t, err)

		// Check dimensions
		assert.Equal(t, 4, result.Height()) // 2 + 2 rows
		assert.Equal(t, 1, result.Width())  // Only column b

		// Only column b should be present
		assert.True(t, result.HasColumn("b"))
		assert.False(t, result.HasColumn("a"))
		assert.False(t, result.HasColumn("c"))

		// Check column b
		colB, err := result.Column("b")
		require.NoError(t, err)
		assert.Equal(t, int64(3), colB.Get(0))
		assert.Equal(t, int64(4), colB.Get(1))
		assert.Equal(t, int64(5), colB.Get(2))
		assert.Equal(t, int64(6), colB.Get(3))
	})

	t.Run("Concat with sorted columns", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("c", []int64{1}),
			series.NewInt64Series("a", []int64{2}),
			series.NewInt64Series("b", []int64{3}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("b", []int64{4}),
			series.NewInt64Series("c", []int64{5}),
			series.NewInt64Series("a", []int64{6}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{
			Axis: 0,
			Sort: true,
		})
		require.NoError(t, err)

		// Columns should be sorted alphabetically
		cols := result.Columns()
		assert.Equal(t, []string{"a", "b", "c"}, cols)
	})

	t.Run("Empty DataFrames", func(t *testing.T) {
		_, err := Concat([]*DataFrame{}, ConcatOptions{Axis: 0})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no DataFrames provided")
	})

	t.Run("Single DataFrame", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2, 3}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df}, ConcatOptions{Axis: 0})
		require.NoError(t, err)

		// Should return the same DataFrame
		assert.Equal(t, df.Height(), result.Height())
		assert.Equal(t, df.Width(), result.Width())
	})

	t.Run("Type mismatch with verification", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewFloat64Series("a", []float64{3.0, 4.0}),
		)
		require.NoError(t, err)

		_, err = Concat([]*DataFrame{df1, df2}, ConcatOptions{
			Axis:         0,
			VerifySchema: true,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "data type mismatch")
	})
}

func TestConcatHorizontal(t *testing.T) {
	t.Run("Basic horizontal concatenation", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2, 3}),
			series.NewInt64Series("b", []int64{4, 5, 6}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("c", []int64{7, 8, 9}),
			series.NewInt64Series("d", []int64{10, 11, 12}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{Axis: 1})
		require.NoError(t, err)

		// Check dimensions
		assert.Equal(t, 3, result.Height()) // Same rows
		assert.Equal(t, 4, result.Width())  // 2 + 2 columns

		// Check all columns are present
		assert.True(t, result.HasColumn("a"))
		assert.True(t, result.HasColumn("b"))
		assert.True(t, result.HasColumn("c"))
		assert.True(t, result.HasColumn("d"))

		// Check values
		colC, err := result.Column("c")
		require.NoError(t, err)
		assert.Equal(t, int64(7), colC.Get(0))
		assert.Equal(t, int64(8), colC.Get(1))
		assert.Equal(t, int64(9), colC.Get(2))
	})

	t.Run("Horizontal concat with duplicate column names", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
			series.NewInt64Series("b", []int64{3, 4}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("a", []int64{5, 6}),
			series.NewInt64Series("c", []int64{7, 8}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{Axis: 1})
		require.NoError(t, err)

		// Check dimensions
		assert.Equal(t, 2, result.Height())
		assert.Equal(t, 4, result.Width())

		// Check columns - duplicate 'a' should be renamed
		assert.True(t, result.HasColumn("a"))
		assert.True(t, result.HasColumn("a_1"))
		assert.True(t, result.HasColumn("b"))
		assert.True(t, result.HasColumn("c"))
	})

	t.Run("Horizontal concat with different heights", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2, 3}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("b", []int64{4, 5}), // Different height
		)
		require.NoError(t, err)

		_, err = Concat([]*DataFrame{df1, df2}, ConcatOptions{Axis: 1})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "different heights")
	})
}

func TestConcatMultiple(t *testing.T) {
	t.Run("Concat three DataFrames vertically", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("x", []int64{1, 2}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("x", []int64{3, 4}),
		)
		require.NoError(t, err)

		df3, err := NewDataFrame(
			series.NewInt64Series("x", []int64{5, 6}),
		)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2, df3}, ConcatOptions{Axis: 0})
		require.NoError(t, err)

		assert.Equal(t, 6, result.Height())

		col, err := result.Column("x")
		require.NoError(t, err)
		for i := 0; i < 6; i++ {
			assert.Equal(t, int64(i+1), col.Get(i))
		}
	})

	t.Run("Concat with nulls", func(t *testing.T) {
		values1 := []float64{1.0, 0, 3.0}
		validity1 := []bool{true, false, true}
		s1 := series.NewSeriesWithValidity("data", values1, validity1, datatypes.Float64{})

		values2 := []float64{0, 5.0, 0}
		validity2 := []bool{false, true, false}
		s2 := series.NewSeriesWithValidity("data", values2, validity2, datatypes.Float64{})

		df1, err := NewDataFrame(s1)
		require.NoError(t, err)

		df2, err := NewDataFrame(s2)
		require.NoError(t, err)

		result, err := Concat([]*DataFrame{df1, df2}, ConcatOptions{Axis: 0})
		require.NoError(t, err)

		col, err := result.Column("data")
		require.NoError(t, err)

		// Check values and nulls are preserved
		assert.Equal(t, 1.0, col.Get(0))
		assert.True(t, col.IsNull(1))
		assert.Equal(t, 3.0, col.Get(2))
		assert.True(t, col.IsNull(3))
		assert.Equal(t, 5.0, col.Get(4))
		assert.True(t, col.IsNull(5))
	})
}