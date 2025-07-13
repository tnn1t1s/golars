package frame

import (
	"testing"

	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeAsof(t *testing.T) {
	t.Run("Basic backward merge", func(t *testing.T) {
		// Left DataFrame - trades
		trades, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 5, 10, 15, 20}),
			series.NewStringSeries("ticker", []string{"A", "A", "A", "A", "A"}),
			series.NewFloat64Series("price", []float64{100, 101, 102, 103, 104}),
		)
		require.NoError(t, err)

		// Right DataFrame - quotes
		quotes, err := NewDataFrame(
			series.NewInt64Series("time", []int64{0, 3, 7, 12, 18}),
			series.NewStringSeries("ticker", []string{"A", "A", "A", "A", "A"}),
			series.NewFloat64Series("bid", []float64{99.5, 100.5, 101.5, 102.5, 103.5}),
			series.NewFloat64Series("ask", []float64{100.5, 101.5, 102.5, 103.5, 104.5}),
		)
		require.NoError(t, err)

		// Merge trades with quotes
		result, err := trades.MergeAsof(quotes, MergeAsofOptions{
			On:        "time",
			Direction: "backward",
		})
		require.NoError(t, err)

		// Check dimensions
		assert.Equal(t, 5, result.Height()) // Same as left
		assert.Equal(t, 6, result.Width())  // time, ticker, price, ticker_y, bid, ask

		// Check bid values (should be from the last quote before each trade)
		bid, err := result.Column("bid")
		require.NoError(t, err)
		assert.Equal(t, 99.5, bid.Get(0))   // Trade at 1, quote at 0
		assert.Equal(t, 100.5, bid.Get(1))  // Trade at 5, quote at 3
		assert.Equal(t, 101.5, bid.Get(2))  // Trade at 10, quote at 7
		assert.Equal(t, 102.5, bid.Get(3))  // Trade at 15, quote at 12
		assert.Equal(t, 103.5, bid.Get(4))  // Trade at 20, quote at 18
	})

	t.Run("Forward merge", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 5, 10}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{2, 6, 8, 12}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0}),
		)
		require.NoError(t, err)

		result, err := left.MergeAsof(right, MergeAsofOptions{
			On:        "time",
			Direction: "forward",
		})
		require.NoError(t, err)

		// Check forward matches
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.Equal(t, 1.0, value.Get(0))  // Time 1 -> next is 2
		assert.Equal(t, 2.0, value.Get(1))  // Time 5 -> next is 6
		assert.Equal(t, 4.0, value.Get(2))  // Time 10 -> next is 12
	})

	t.Run("Nearest merge", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{3, 7, 11}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 5, 10, 15}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0}),
		)
		require.NoError(t, err)

		result, err := left.MergeAsof(right, MergeAsofOptions{
			On:        "time",
			Direction: "nearest",
		})
		require.NoError(t, err)

		// Check nearest matches
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.Equal(t, 1.0, value.Get(0))  // Time 3 -> nearest is 1 (distance 2)
		assert.Equal(t, 2.0, value.Get(1))  // Time 7 -> nearest is 5 (distance 2)
		assert.Equal(t, 3.0, value.Get(2))  // Time 11 -> nearest is 10 (distance 1)
	})

	t.Run("Merge with tolerance", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 15, 25}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 10, 20}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		result, err := left.MergeAsof(right, MergeAsofOptions{
			On:        "time",
			Direction: "backward",
			Tolerance: 3.0, // Max distance of 3
		})
		require.NoError(t, err)

		// Check matches with tolerance
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.True(t, value.IsNull(0))   // Time 5, nearest backward is 1 (distance 4 > tolerance 3)
		assert.True(t, value.IsNull(1))   // Time 15, nearest backward is 10 (distance 5 > tolerance 3)
		assert.True(t, value.IsNull(2))   // Time 25, nearest backward is 20 (distance 5 > tolerance 3)
	})

	t.Run("Merge without exact matches", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{2, 4, 6}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame  
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{2, 4, 6}), // Exact same times
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		allowExact := false
		result, err := left.MergeAsof(right, MergeAsofOptions{
			On:         "time",
			Direction:  "backward",
			AllowExact: &allowExact, // Don't allow exact matches
		})
		require.NoError(t, err)

		// With exact matches disabled:
		// - Time 2: No backward match (nothing before 2)
		// - Time 4: Matches time 2 (value 1.0)
		// - Time 6: Matches time 4 (value 2.0)
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.True(t, value.IsNull(0))    // No backward match for time 2
		assert.Equal(t, 1.0, value.Get(1))  // Time 4 matches time 2 (value 1.0)
		assert.Equal(t, 2.0, value.Get(2))  // Time 6 matches time 4 (value 2.0)
	})

	t.Run("Different merge column names", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("trade_time", []int64{5, 10, 15}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("quote_time", []int64{3, 8, 13}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		result, err := left.MergeAsof(right, MergeAsofOptions{
			Left_on:   "trade_time",
			Right_on:  "quote_time",
			Direction: "backward",
		})
		require.NoError(t, err)

		// Check matches
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.Equal(t, 1.0, value.Get(0))  // Trade at 5, quote at 3
		assert.Equal(t, 2.0, value.Get(1))  // Trade at 10, quote at 8
		assert.Equal(t, 3.0, value.Get(2))  // Trade at 15, quote at 13
	})

	t.Run("No matches", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 2, 3}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame - all times are after left
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{10, 11, 12}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		result, err := left.MergeAsof(right, MergeAsofOptions{
			On:        "time",
			Direction: "backward",
		})
		require.NoError(t, err)

		// All values should be null (no backward matches)
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.True(t, value.IsNull(0))
		assert.True(t, value.IsNull(1))
		assert.True(t, value.IsNull(2))
	})

	t.Run("Unsorted column error", func(t *testing.T) {
		// Left DataFrame - unsorted
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 2, 8}), // Not sorted!
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 3, 5}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		_, err = left.MergeAsof(right, MergeAsofOptions{
			On: "time",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be sorted")
	})
}