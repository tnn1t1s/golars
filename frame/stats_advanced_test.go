package frame

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func TestMode(t *testing.T) {
	t.Run("Basic mode calculation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("col1", []int64{1, 2, 2, 3, 3, 3, 4}),
			series.NewFloat64Series("col2", []float64{1.5, 1.5, 2.5, 2.5, 2.5, 3.5, 3.5}),
		)
		require.NoError(t, err)

		mode, err := df.Mode(0, true, true)
		require.NoError(t, err)

		// Check that mode returns the most frequent values
		col1Mode, err := mode.Column("col1")
		require.NoError(t, err)
		assert.Equal(t, int64(3), col1Mode.Get(0)) // 3 appears 3 times

		col2Mode, err := mode.Column("col2")
		require.NoError(t, err)
		assert.Equal(t, 2.5, col2Mode.Get(0)) // 2.5 appears 3 times
	})

	t.Run("Mode with null values", func(t *testing.T) {
		values := []float64{1, 2, 0, 2, 0, 3}
		validity := []bool{true, true, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})

		df, err := NewDataFrame(s)
		require.NoError(t, err)

		// Mode with dropNaN=true
		mode, err := df.Mode(0, true, true)
		require.NoError(t, err)

		col, err := mode.Column("col")
		require.NoError(t, err)
		assert.Equal(t, 2.0, col.Get(0)) // 2 appears twice (most frequent non-null)
	})

	t.Run("Mode with strings", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("words", []string{"cat", "dog", "cat", "bird", "cat", "dog"}),
		)
		require.NoError(t, err)

		mode, err := df.Mode(0, false, true)
		require.NoError(t, err)

		col, err := mode.Column("words")
		require.NoError(t, err)
		assert.Equal(t, "cat", col.Get(0)) // "cat" appears 3 times
	})
}

func TestSkew(t *testing.T) {
	t.Run("Basic skewness calculation", func(t *testing.T) {
		// Create a right-skewed distribution
		df, err := NewDataFrame(
			series.NewFloat64Series("right_skew", []float64{1, 2, 2, 3, 3, 3, 4, 5, 10}),
			series.NewFloat64Series("symmetric", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}),
		)
		require.NoError(t, err)

		skew, err := df.Skew(0, true)
		require.NoError(t, err)

		// Right skewed should have positive skewness
		rightSkew, err := skew.Column("right_skew")
		require.NoError(t, err)
		assert.Greater(t, rightSkew.Get(0).(float64), 0.0)

		// Symmetric should have skewness close to 0
		symmetric, err := skew.Column("symmetric")
		require.NoError(t, err)
		assert.InDelta(t, 0.0, symmetric.Get(0).(float64), 0.1)
	})

	t.Run("Skewness with insufficient data", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("col", []float64{1, 2}), // Only 2 values
		)
		require.NoError(t, err)

		skew, err := df.Skew(0, true)
		require.NoError(t, err)

		col, err := skew.Column("col")
		require.NoError(t, err)
		assert.True(t, math.IsNaN(col.Get(0).(float64)))
	})
}

func TestKurtosis(t *testing.T) {
	t.Run("Basic kurtosis calculation", func(t *testing.T) {
		// Create distributions with different kurtosis
		df, err := NewDataFrame(
			// Uniform-like distribution (platykurtic, negative excess kurtosis)
			series.NewFloat64Series("uniform", []float64{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}),
			// Normal-like distribution (mesokurtic, excess kurtosis ≈ 0)
			series.NewFloat64Series("normal", []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1}),
		)
		require.NoError(t, err)

		kurt, err := df.Kurtosis(0, true)
		require.NoError(t, err)

		// Uniform should have negative excess kurtosis
		uniform, err := kurt.Column("uniform")
		require.NoError(t, err)
		assert.Less(t, uniform.Get(0).(float64), 0.0)
	})

	t.Run("Kurtosis with insufficient data", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("col", []float64{1, 2, 3}), // Only 3 values
		)
		require.NoError(t, err)

		kurt, err := df.Kurtosis(0, true)
		require.NoError(t, err)

		col, err := kurt.Column("col")
		require.NoError(t, err)
		assert.True(t, math.IsNaN(col.Get(0).(float64)))
	})
}

func TestValueCounts(t *testing.T) {
	t.Run("Basic value counts", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("colors", []string{"red", "blue", "red", "green", "blue", "red"}),
		)
		require.NoError(t, err)

		counts, err := df.ValueCounts([]string{"colors"}, false, true, false, true)
		require.NoError(t, err)

		// Should be sorted by count descending
		colors, err := counts.Column("colors")
		require.NoError(t, err)
		countCol, err := counts.Column("count")
		require.NoError(t, err)

		// "red" appears 3 times (most frequent)
		assert.Equal(t, "red", colors.Get(0))
		assert.Equal(t, 3.0, countCol.Get(0))

		// "blue" appears 2 times
		assert.Equal(t, "blue", colors.Get(1))
		assert.Equal(t, 2.0, countCol.Get(1))

		// "green" appears 1 time
		assert.Equal(t, "green", colors.Get(2))
		assert.Equal(t, 1.0, countCol.Get(2))
	})

	t.Run("Normalized value counts", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("numbers", []int64{1, 2, 2, 3, 3, 3}),
		)
		require.NoError(t, err)

		counts, err := df.ValueCounts([]string{"numbers"}, true, true, false, true)
		require.NoError(t, err)

		countCol, err := counts.Column("count")
		require.NoError(t, err)

		// Check normalized frequencies
		assert.InDelta(t, 0.5, countCol.Get(0), 0.001)   // 3 appears 3/6 times
		assert.InDelta(t, 0.333, countCol.Get(1), 0.001) // 2 appears 2/6 times
		assert.InDelta(t, 0.167, countCol.Get(2), 0.001) // 1 appears 1/6 times
	})

	t.Run("Value counts with nulls", func(t *testing.T) {
		values := []int64{1, 2, 0, 2, 0, 3}
		validity := []bool{true, true, false, true, false, true}
		s := series.NewSeriesWithValidity("nums", values, validity, datatypes.Int64{})

		df, err := NewDataFrame(s)
		require.NoError(t, err)

		// With dropNaN=true
		counts, err := df.ValueCounts([]string{"nums"}, false, true, false, true)
		require.NoError(t, err)

		// Should only count non-null values
		assert.Equal(t, 3, counts.Height()) // 1, 2, 3 (no nulls)
	})
}

func TestNUnique(t *testing.T) {
	t.Run("Basic n_unique", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("col1", []int64{1, 2, 2, 3, 3, 3}),
			series.NewStringSeries("col2", []string{"a", "b", "a", "c", "b", "d"}),
		)
		require.NoError(t, err)

		nunique, err := df.NUnique(0, true)
		require.NoError(t, err)

		// col1 has 3 unique values: 1, 2, 3
		col1, err := nunique.Column("col1")
		require.NoError(t, err)
		assert.Equal(t, int64(3), col1.Get(0))

		// col2 has 4 unique values: a, b, c, d
		col2, err := nunique.Column("col2")
		require.NoError(t, err)
		assert.Equal(t, int64(4), col2.Get(0))
	})

	t.Run("N_unique with nulls", func(t *testing.T) {
		values := []float64{1, 2, 0, 2, 0, 3}
		validity := []bool{true, true, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})

		df, err := NewDataFrame(s)
		require.NoError(t, err)

		// With dropNaN=true
		nunique, err := df.NUnique(0, true)
		require.NoError(t, err)

		col, err := nunique.Column("col")
		require.NoError(t, err)
		assert.Equal(t, int64(3), col.Get(0)) // 1, 2, 3 (nulls not counted)

		// With dropNaN=false
		nunique2, err := df.NUnique(0, false)
		require.NoError(t, err)

		col2, err := nunique2.Column("col")
		require.NoError(t, err)
		assert.Equal(t, int64(4), col2.Get(0)) // 1, 2, 3, null
	})
}

func TestRank(t *testing.T) {
	t.Run("Basic rank - average method", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 1, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "average",
			Ascending: true,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Values: [3, 1, 4, 1, 5]
		// Sorted: [1, 1, 3, 4, 5]
		// Ranks:  [1.5, 1.5, 3, 4, 5]
		// Result: [3, 1.5, 4, 1.5, 5]
		assert.Equal(t, 3.0, col.Get(0))
		assert.Equal(t, 1.5, col.Get(1))
		assert.Equal(t, 4.0, col.Get(2))
		assert.Equal(t, 1.5, col.Get(3))
		assert.Equal(t, 5.0, col.Get(4))
	})

	t.Run("Rank - min method", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 1, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "min",
			Ascending: true,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Min rank for ties
		assert.Equal(t, 3.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1)) // Min rank for tie
		assert.Equal(t, 4.0, col.Get(2))
		assert.Equal(t, 1.0, col.Get(3)) // Min rank for tie
		assert.Equal(t, 5.0, col.Get(4))
	})

	t.Run("Rank - max method", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 1, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "max",
			Ascending: true,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Max rank for ties
		assert.Equal(t, 3.0, col.Get(0))
		assert.Equal(t, 2.0, col.Get(1)) // Max rank for tie
		assert.Equal(t, 4.0, col.Get(2))
		assert.Equal(t, 2.0, col.Get(3)) // Max rank for tie
		assert.Equal(t, 5.0, col.Get(4))
	})

	t.Run("Rank - dense method", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 1, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "dense",
			Ascending: true,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Dense ranking (no gaps)
		// Unique values: 1, 3, 4, 5 -> ranks 1, 2, 3, 4
		assert.Equal(t, 2.0, col.Get(0)) // 3 -> rank 2
		assert.Equal(t, 1.0, col.Get(1)) // 1 -> rank 1
		assert.Equal(t, 3.0, col.Get(2)) // 4 -> rank 3
		assert.Equal(t, 1.0, col.Get(3)) // 1 -> rank 1
		assert.Equal(t, 4.0, col.Get(4)) // 5 -> rank 4
	})

	t.Run("Rank - ordinal method", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 1, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "ordinal",
			Ascending: true,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Ordinal ranking (unique ranks, first come first served)
		assert.Equal(t, 3.0, col.Get(0)) // 3rd smallest
		assert.Equal(t, 1.0, col.Get(1)) // 1st smallest (first 1)
		assert.Equal(t, 4.0, col.Get(2)) // 4th smallest
		assert.Equal(t, 2.0, col.Get(3)) // 2nd smallest (second 1)
		assert.Equal(t, 5.0, col.Get(4)) // 5th smallest
	})

	t.Run("Rank - descending", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{3, 1, 4, 1, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "average",
			Ascending: false,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Descending order
		assert.Equal(t, 3.0, col.Get(0)) // 3 is 3rd largest
		assert.Equal(t, 4.5, col.Get(1)) // 1 is tied for 4th/5th
		assert.Equal(t, 2.0, col.Get(2)) // 4 is 2nd largest
		assert.Equal(t, 4.5, col.Get(3)) // 1 is tied for 4th/5th
		assert.Equal(t, 1.0, col.Get(4)) // 5 is largest
	})

	t.Run("Rank with nulls", func(t *testing.T) {
		values := []float64{3, 0, 4, 0, 5}
		validity := []bool{true, false, true, false, true}
		s := series.NewSeriesWithValidity("vals", values, validity, datatypes.Float64{})

		df, err := NewDataFrame(s)
		require.NoError(t, err)

		// NaOption = "keep" (default)
		ranked, err := df.Rank(RankOptions{
			Method:    "average",
			Ascending: true,
			NaOption:  "keep",
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		assert.Equal(t, 1.0, col.Get(0)) // 3 is smallest
		assert.True(t, col.IsNull(1))    // Null kept as null
		assert.Equal(t, 2.0, col.Get(2)) // 4 is 2nd
		assert.True(t, col.IsNull(3))    // Null kept as null
		assert.Equal(t, 3.0, col.Get(4)) // 5 is 3rd
	})

	t.Run("Rank - percentile ranks", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("vals", []float64{1, 2, 3, 4, 5}),
		)
		require.NoError(t, err)

		ranked, err := df.Rank(RankOptions{
			Method:    "average",
			Ascending: true,
			Pct:       true,
		})
		require.NoError(t, err)

		col, err := ranked.Column("vals")
		require.NoError(t, err)

		// Percentile ranks
		assert.Equal(t, 0.2, col.Get(0)) // 1/5
		assert.Equal(t, 0.4, col.Get(1)) // 2/5
		assert.Equal(t, 0.6, col.Get(2)) // 3/5
		assert.Equal(t, 0.8, col.Get(3)) // 4/5
		assert.Equal(t, 1.0, col.Get(4)) // 5/5
	})
}
