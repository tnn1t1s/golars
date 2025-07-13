package frame

import (
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCumSum(t *testing.T) {
	t.Run("Basic cumulative sum", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("b", []float64{1.5, 2.5, 3.5, 4.5, 5.5}),
		)
		require.NoError(t, err)

		result, err := df.CumSum(CumulativeOptions{})
		require.NoError(t, err)

		// Check cumsum of column a
		colA, err := result.Column("a")
		require.NoError(t, err)
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(3), colA.Get(1))   // 1+2
		assert.Equal(t, int64(6), colA.Get(2))   // 1+2+3
		assert.Equal(t, int64(10), colA.Get(3))  // 1+2+3+4
		assert.Equal(t, int64(15), colA.Get(4))  // 1+2+3+4+5

		// Check cumsum of column b
		colB, err := result.Column("b")
		require.NoError(t, err)
		assert.Equal(t, 1.5, colB.Get(0))
		assert.Equal(t, 4.0, colB.Get(1))   // 1.5+2.5
		assert.Equal(t, 7.5, colB.Get(2))   // 4+3.5
		assert.Equal(t, 12.0, colB.Get(3))  // 7.5+4.5
		assert.Equal(t, 17.5, colB.Get(4))  // 12+5.5
	})

	t.Run("Cumulative sum with nulls", func(t *testing.T) {
		values := []float64{1, 0, 3, 0, 5}
		validity := []bool{true, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})

		df, err := NewDataFrame(s)
		require.NoError(t, err)

		// With skipNulls=true
		result, err := df.CumSum(CumulativeOptions{SkipNulls: true})
		require.NoError(t, err)

		col, err := result.Column("col")
		require.NoError(t, err)
		assert.Equal(t, 1.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1))  // Skip null, keep previous sum
		assert.Equal(t, 4.0, col.Get(2))  // 1+3
		assert.Equal(t, 4.0, col.Get(3))  // Skip null, keep previous sum
		assert.Equal(t, 9.0, col.Get(4))  // 4+5

		// With skipNulls=false
		result2, err := df.CumSum(CumulativeOptions{SkipNulls: false})
		require.NoError(t, err)

		col2, err := result2.Column("col")
		require.NoError(t, err)
		assert.Equal(t, 1.0, col2.Get(0))
		assert.True(t, col2.IsNull(1))   // Propagate null
		assert.Equal(t, 4.0, col2.Get(2))
		assert.True(t, col2.IsNull(3))   // Propagate null
		assert.Equal(t, 9.0, col2.Get(4))
	})
}

func TestCumProd(t *testing.T) {
	t.Run("Basic cumulative product", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("vals", []int64{2, 3, 4, 5}),
		)
		require.NoError(t, err)

		result, err := df.CumProd(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("vals")
		require.NoError(t, err)
		assert.Equal(t, int64(2), col.Get(0))
		assert.Equal(t, int64(6), col.Get(1))    // 2*3
		assert.Equal(t, int64(24), col.Get(2))   // 6*4
		assert.Equal(t, int64(120), col.Get(3))  // 24*5
	})

	t.Run("Cumulative product with zeros", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{2, 0, 3, 4}),
		)
		require.NoError(t, err)

		result, err := df.CumProd(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("vals")
		require.NoError(t, err)
		assert.Equal(t, 2.0, col.Get(0))
		assert.Equal(t, 0.0, col.Get(1))  // 2*0
		assert.Equal(t, 0.0, col.Get(2))  // 0*3
		assert.Equal(t, 0.0, col.Get(3))  // 0*4
	})
}

func TestCumMax(t *testing.T) {
	t.Run("Basic cumulative maximum", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 2, 5}),
		)
		require.NoError(t, err)

		result, err := df.CumMax(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("vals")
		require.NoError(t, err)
		assert.Equal(t, 3.0, col.Get(0))
		assert.Equal(t, 3.0, col.Get(1))  // max(3,1) = 3
		assert.Equal(t, 4.0, col.Get(2))  // max(3,4) = 4
		assert.Equal(t, 4.0, col.Get(3))  // max(4,2) = 4
		assert.Equal(t, 5.0, col.Get(4))  // max(4,5) = 5
	})

	t.Run("Cumulative maximum with negative values", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("vals", []int64{-5, -3, -10, -1, -2}),
		)
		require.NoError(t, err)

		result, err := df.CumMax(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("vals")
		require.NoError(t, err)
		assert.Equal(t, int64(-5), col.Get(0))
		assert.Equal(t, int64(-3), col.Get(1))   // max(-5,-3) = -3
		assert.Equal(t, int64(-3), col.Get(2))   // max(-3,-10) = -3
		assert.Equal(t, int64(-1), col.Get(3))   // max(-3,-1) = -1
		assert.Equal(t, int64(-1), col.Get(4))   // max(-1,-2) = -1
	})
}

func TestCumMin(t *testing.T) {
	t.Run("Basic cumulative minimum", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 2, 0}),
		)
		require.NoError(t, err)

		result, err := df.CumMin(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("vals")
		require.NoError(t, err)
		assert.Equal(t, 3.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1))  // min(3,1) = 1
		assert.Equal(t, 1.0, col.Get(2))  // min(1,4) = 1
		assert.Equal(t, 1.0, col.Get(3))  // min(1,2) = 1
		assert.Equal(t, 0.0, col.Get(4))  // min(1,0) = 0
	})
}

func TestCumCount(t *testing.T) {
	t.Run("Basic cumulative count", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("vals", []int64{1, 2, 3, 4, 5}),
		)
		require.NoError(t, err)

		result, err := df.CumCount(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("vals")
		require.NoError(t, err)
		assert.Equal(t, int64(1), col.Get(0))
		assert.Equal(t, int64(2), col.Get(1))
		assert.Equal(t, int64(3), col.Get(2))
		assert.Equal(t, int64(4), col.Get(3))
		assert.Equal(t, int64(5), col.Get(4))
	})

	t.Run("Cumulative count with nulls", func(t *testing.T) {
		values := []float64{1, 0, 3, 0, 5}
		validity := []bool{true, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})

		df, err := NewDataFrame(s)
		require.NoError(t, err)

		result, err := df.CumCount(CumulativeOptions{})
		require.NoError(t, err)

		col, err := result.Column("col")
		require.NoError(t, err)
		assert.Equal(t, int64(1), col.Get(0))  // First non-null
		assert.Equal(t, int64(1), col.Get(1))  // Null doesn't increase count
		assert.Equal(t, int64(2), col.Get(2))  // Second non-null
		assert.Equal(t, int64(2), col.Get(3))  // Null doesn't increase count
		assert.Equal(t, int64(3), col.Get(4))  // Third non-null
	})
}

func TestCumulativeWithSpecificColumns(t *testing.T) {
	df, err := NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
		series.NewInt64Series("b", []int64{4, 5, 6}),
		series.NewStringSeries("c", []string{"x", "y", "z"}),
	)
	require.NoError(t, err)

	// Apply cumsum only to column 'a'
	result, err := df.CumSum(CumulativeOptions{
		Columns: []string{"a"},
	})
	require.NoError(t, err)

	// Column 'a' should have cumsum
	colA, err := result.Column("a")
	require.NoError(t, err)
	assert.Equal(t, int64(1), colA.Get(0))
	assert.Equal(t, int64(3), colA.Get(1))
	assert.Equal(t, int64(6), colA.Get(2))

	// Column 'b' should be unchanged
	colB, err := result.Column("b")
	require.NoError(t, err)
	assert.Equal(t, int64(4), colB.Get(0))
	assert.Equal(t, int64(5), colB.Get(1))
	assert.Equal(t, int64(6), colB.Get(2))

	// Column 'c' should be unchanged
	colC, err := result.Column("c")
	require.NoError(t, err)
	assert.Equal(t, "x", colC.Get(0))
	assert.Equal(t, "y", colC.Get(1))
	assert.Equal(t, "z", colC.Get(2))
}