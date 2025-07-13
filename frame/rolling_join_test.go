package frame

import (
	"testing"

	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRollingJoin(t *testing.T) {
	t.Run("Basic backward rolling join", func(t *testing.T) {
		// Left DataFrame - events
		events, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 10, 15, 20, 25}),
			series.NewStringSeries("event", []string{"A", "B", "C", "D", "E"}),
		)
		require.NoError(t, err)

		// Right DataFrame - measurements
		measurements, err := NewDataFrame(
			series.NewInt64Series("time", []int64{3, 7, 12, 18, 22, 27}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
		)
		require.NoError(t, err)

		// Join with window size of 5
		result, err := events.RollingJoin(measurements, RollingJoinOptions{
			On:         "time",
			WindowSize: 5,
			Direction:  "backward",
		})
		require.NoError(t, err)

		// Event at time 5: window [0,5], matches measurements at 3
		// Event at time 10: window [5,10], matches measurements at 7
		// Event at time 15: window [10,15], matches measurements at 12
		// Event at time 20: window [15,20], matches measurements at 18
		// Event at time 25: window [20,25], matches measurements at 22

		assert.Equal(t, 5, result.Height())
		
		// Check that each event matched one measurement
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.Equal(t, 1.0, value.Get(0))
		assert.Equal(t, 2.0, value.Get(1))
		assert.Equal(t, 3.0, value.Get(2))
		assert.Equal(t, 4.0, value.Get(3))
		assert.Equal(t, 5.0, value.Get(4))
	})

	t.Run("Forward rolling join", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 10, 15}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{7, 12, 17, 20}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:         "time",
			WindowSize: 5,
			Direction:  "forward",
		})
		require.NoError(t, err)

		// Time 5: window [5,10], matches 7
		// Time 10: window [10,15], matches 12
		// Time 15: window [15,20], matches 17, 20

		// Should have 4 rows (1 + 1 + 2)
		assert.Equal(t, 4, result.Height())
	})

	t.Run("Rolling join with multiple matches", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{10, 20}),
			series.NewStringSeries("id", []string{"A", "B"}),
		)
		require.NoError(t, err)

		// Right DataFrame with multiple values in windows
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{8, 9, 10, 18, 19, 20, 21}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:            "time",
			WindowSize:    3,
			Direction:     "backward",
			ClosedInterval: "both", // Include both endpoints
		})
		require.NoError(t, err)

		// Time 10: window [7,10], matches 8, 9, 10 (3 matches)
		// Time 20: window [17,20], matches 18, 19, 20 (3 matches)
		// Total: 6 rows

		assert.Equal(t, 6, result.Height())
		
		// Check id column is replicated correctly
		id, err := result.Column("id")
		require.NoError(t, err)
		assert.Equal(t, "A", id.Get(0))
		assert.Equal(t, "A", id.Get(1))
		assert.Equal(t, "A", id.Get(2))
		assert.Equal(t, "B", id.Get(3))
		assert.Equal(t, "B", id.Get(4))
		assert.Equal(t, "B", id.Get(5))
	})

	t.Run("Rolling join with min_periods", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 10, 15}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{4, 11, 12}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:         "time",
			WindowSize: 5,
			MinPeriods: 2, // Require at least 2 matches
			Direction:  "backward",
		})
		require.NoError(t, err)

		// Time 5: window [0,5], only 1 match (4) - not enough
		// Time 10: window [5,10], no matches - not enough
		// Time 15: window [10,15], 2 matches (11, 12) - enough

		// First two should have null values, third should have 2 matches
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.True(t, value.IsNull(0))  // Time 5: not enough matches
		assert.True(t, value.IsNull(1))  // Time 10: not enough matches
		assert.Equal(t, 2.0, value.Get(2)) // Time 15: first match at 11
		assert.Equal(t, 3.0, value.Get(3)) // Time 15: second match at 12
	})

	t.Run("Center window", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{10, 20, 30}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 10, 15, 20, 25, 30, 35}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:             "time",
			WindowSize:     10,
			Center:         true,
			Direction:      "backward", // ignored when center is true
			ClosedInterval: "both", // Include both endpoints
		})
		require.NoError(t, err)

		// Time 10: centered window [5,15], matches 5, 10, 15
		// Time 20: centered window [15,25], matches 15, 20, 25
		// Time 30: centered window [25,35], matches 25, 30, 35

		assert.Equal(t, 9, result.Height()) // 3 + 3 + 3 matches
	})

	t.Run("Both direction", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{20}),
			series.NewStringSeries("id", []string{"A"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{15, 18, 20, 22, 25}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:             "time",
			WindowSize:     5,
			Direction:      "both",
			ClosedInterval: "both", // Include both endpoints
		})
		require.NoError(t, err)

		// Time 20: window [15,25], matches all 5 values
		assert.Equal(t, 5, result.Height())
		
		value, err := result.Column("value")
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			assert.Equal(t, float64(i+1), value.Get(i))
		}
	})

	t.Run("Closed interval options", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{10}),
			series.NewStringSeries("id", []string{"A"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 10, 15}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		// Test "neither" - excludes both endpoints
		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:             "time",
			WindowSize:     5,
			Direction:      "backward",
			ClosedInterval: "neither",
		})
		require.NoError(t, err)

		// Window (5,10) - no matches (5 and 10 are excluded)
		assert.Equal(t, 1, result.Height())
		value, _ := result.Column("value")
		assert.True(t, value.IsNull(0))

		// Test "left" - includes left endpoint only
		result, err = left.RollingJoin(right, RollingJoinOptions{
			On:             "time",
			WindowSize:     5,
			Direction:      "backward",
			ClosedInterval: "left",
		})
		require.NoError(t, err)

		// Window [5,10) - matches 5 only
		assert.Equal(t, 1, result.Height())
		value, _ = result.Column("value")
		assert.Equal(t, 1.0, value.Get(0))
	})

	t.Run("No matches", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("time", []int64{5, 10, 15}),
			series.NewStringSeries("id", []string{"A", "B", "C"}),
		)
		require.NoError(t, err)

		// Right DataFrame - all values outside windows
		right, err := NewDataFrame(
			series.NewInt64Series("time", []int64{50, 60, 70}),
			series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			On:         "time",
			WindowSize: 10,
			Direction:  "backward",
		})
		require.NoError(t, err)

		// All values should be null
		assert.Equal(t, 3, result.Height())
		value, err := result.Column("value")
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			assert.True(t, value.IsNull(i))
		}
	})

	t.Run("Different column names", func(t *testing.T) {
		// Left DataFrame
		left, err := NewDataFrame(
			series.NewInt64Series("event_time", []int64{10, 20}),
			series.NewStringSeries("id", []string{"A", "B"}),
		)
		require.NoError(t, err)

		// Right DataFrame
		right, err := NewDataFrame(
			series.NewInt64Series("measure_time", []int64{8, 18}),
			series.NewFloat64Series("value", []float64{1.0, 2.0}),
		)
		require.NoError(t, err)

		result, err := left.RollingJoin(right, RollingJoinOptions{
			Left_on:    "event_time",
			Right_on:   "measure_time",
			WindowSize: 5,
			Direction:  "backward",
		})
		require.NoError(t, err)

		assert.Equal(t, 2, result.Height())
		value, err := result.Column("value")
		require.NoError(t, err)
		assert.Equal(t, 1.0, value.Get(0))
		assert.Equal(t, 2.0, value.Get(1))
	})

	t.Run("Invalid parameters", func(t *testing.T) {
		df1, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 2, 3}),
		)
		require.NoError(t, err)

		df2, err := NewDataFrame(
			series.NewInt64Series("time", []int64{1, 2, 3}),
		)
		require.NoError(t, err)

		// Negative window size
		_, err = df1.RollingJoin(df2, RollingJoinOptions{
			On:         "time",
			WindowSize: -5,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "window size must be positive")

		// Invalid direction
		_, err = df1.RollingJoin(df2, RollingJoinOptions{
			On:         "time",
			WindowSize: 5,
			Direction:  "invalid",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "direction must be")

		// Invalid closed interval
		_, err = df1.RollingJoin(df2, RollingJoinOptions{
			On:             "time",
			WindowSize:     5,
			ClosedInterval: "invalid",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "closed_interval must be")
	})
}