package datetime

import (
	"testing"
	"time"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateTimeArithmeticSeries(t *testing.T) {
	times := []time.Time{
		time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		time.Date(2024, 7, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2023, 12, 25, 14, 15, 30, 0, time.UTC),
	}

	s := NewDateTimeSeries("timestamps", times)
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Add duration", func(t *testing.T) {
		// Add 1 day
		result := dts.Add(Days(1))
		assert.Equal(t, "timestamps_plus_1 day", result.Name())
		assert.Equal(t, 3, result.Len())
		
		// Check first value
		ts := result.Get(0).(int64)
		dt := DateTime{timestamp: ts, timezone: time.UTC}
		expected := time.Date(2024, 1, 16, 10, 30, 45, 0, time.UTC)
		assert.Equal(t, expected, dt.Time())

		// Add 2 hours
		result2 := dts.Add(Hours(2))
		ts2 := result2.Get(0).(int64)
		dt2 := DateTime{timestamp: ts2, timezone: time.UTC}
		expected2 := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)
		assert.Equal(t, expected2, dt2.Time())

		// Add 1 month
		result3 := dts.Add(Months(1))
		ts3 := result3.Get(0).(int64)
		dt3 := DateTime{timestamp: ts3, timezone: time.UTC}
		expected3 := time.Date(2024, 2, 15, 10, 30, 45, 0, time.UTC)
		assert.Equal(t, expected3, dt3.Time())
	})

	t.Run("Subtract duration", func(t *testing.T) {
		// Subtract 1 week
		result := dts.Sub(Weeks(1))
		ts := result.Get(0).(int64)
		dt := DateTime{timestamp: ts, timezone: time.UTC}
		expected := time.Date(2024, 1, 8, 10, 30, 45, 0, time.UTC)
		assert.Equal(t, expected, dt.Time())
	})

	t.Run("Diff between series", func(t *testing.T) {
		times2 := []time.Time{
			time.Date(2024, 1, 14, 10, 30, 45, 0, time.UTC), // 1 day before
			time.Date(2024, 7, 30, 23, 59, 59, 0, time.UTC), // 1 day before
			time.Date(2023, 12, 24, 14, 15, 30, 0, time.UTC), // 1 day before
		}
		s2 := NewDateTimeSeries("other", times2)
		
		diff, err := dts.Diff(s2)
		require.NoError(t, err)
		assert.Equal(t, "timestamps_diff_other", diff.Name())
		assert.IsType(t, datatypes.Duration{}, diff.DataType())
		
		// First difference should be 1 day in nanoseconds
		nsDiff := diff.Get(0).(int64)
		assert.Equal(t, int64(24*time.Hour), nsDiff)
	})

	t.Run("Add business days", func(t *testing.T) {
		// Create a date that's a Thursday
		thursday := time.Date(2024, 1, 18, 0, 0, 0, 0, time.UTC)
		s := NewDateTimeSeries("dates", []time.Time{thursday})
		dts, err := DtSeries(s)
		require.NoError(t, err)

		// Add 1 business day (should be Friday)
		result := dts.AddBusinessDays(1)
		ts := result.Get(0).(int64)
		dt := DateTime{timestamp: ts, timezone: time.UTC}
		assert.Equal(t, time.Friday, dt.Time().Weekday())
		assert.Equal(t, 19, dt.Day())

		// Add 2 business days (should skip weekend to Monday)
		result2 := dts.AddBusinessDays(2)
		ts2 := result2.Get(0).(int64)
		dt2 := DateTime{timestamp: ts2, timezone: time.UTC}
		assert.Equal(t, time.Monday, dt2.Time().Weekday())
		assert.Equal(t, 22, dt2.Day())
	})
}

func TestDateArithmeticSeries(t *testing.T) {
	dates := []time.Time{
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 7, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
	}

	s := NewDateSeries("dates", dates)
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Add duration to date", func(t *testing.T) {
		// Add 10 days
		result := dts.Add(Days(10))
		assert.Equal(t, "dates_plus_10 days", result.Name())
		
		days := result.Get(0).(int32)
		date := Date{days: days}
		assert.Equal(t, 25, date.Day())
		assert.Equal(t, time.January, date.Month())
	})

	t.Run("Add months to date", func(t *testing.T) {
		// Add 2 months
		result := dts.Add(Months(2))
		
		days := result.Get(0).(int32)
		date := Date{days: days}
		assert.Equal(t, 15, date.Day())
		assert.Equal(t, time.March, date.Month())
	})

	t.Run("Date diff", func(t *testing.T) {
		dates2 := []time.Time{
			time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC), // 5 days before
			time.Date(2024, 7, 20, 0, 0, 0, 0, time.UTC), // 11 days before
			time.Date(2023, 12, 20, 0, 0, 0, 0, time.UTC), // 5 days before
		}
		s2 := NewDateSeries("other", dates2)
		
		diff, err := dts.Diff(s2)
		require.NoError(t, err)
		assert.Equal(t, datatypes.Int32{}, diff.DataType())
		
		// Check differences
		assert.Equal(t, int32(5), diff.Get(0))
		assert.Equal(t, int32(11), diff.Get(1))
		assert.Equal(t, int32(5), diff.Get(2))
	})
}

func TestDateTimeArithmeticExpr(t *testing.T) {
	col := expr.Col("timestamp")
	dtExpr := DtExpr(col)

	t.Run("Add expression", func(t *testing.T) {
		addExpr := dtExpr.Add(Days(7))
		assert.Equal(t, "col(timestamp) + 7 days", addExpr.String())
		assert.Equal(t, "timestamp_plus_7 days", addExpr.Name())
		assert.False(t, addExpr.IsColumn())
	})

	t.Run("Subtract expression", func(t *testing.T) {
		subExpr := dtExpr.Sub(Hours(12))
		assert.Equal(t, "col(timestamp) - 12 hours", subExpr.String())
		assert.Equal(t, "timestamp_minus_12 hours", subExpr.Name())
	})

	t.Run("Diff expression", func(t *testing.T) {
		other := expr.Col("other_timestamp")
		diffExpr := dtExpr.Diff(other)
		assert.Equal(t, "col(timestamp) - col(other_timestamp)", diffExpr.String())
		assert.Equal(t, "timestamp_diff_other_timestamp", diffExpr.Name())
		
		// Should return Duration type for datetime diff
		assert.Equal(t, datatypes.Duration{Unit: datatypes.Nanoseconds}, diffExpr.DataType())
	})

	t.Run("Chained operations", func(t *testing.T) {
		// Add a month then floor to day
		expr := dtExpr.Add(Months(1)).(*DateTimeAddExpr)
		floorExpr := DtExpr(expr).Floor(Day)
		assert.Contains(t, floorExpr.String(), "floor(D)")
	})
}

func TestNullHandlingArithmetic(t *testing.T) {
	// Create series with nulls
	values := []string{"2024-01-15", "", "2024-07-31"}
	s, err := NewDateTimeSeriesFromStrings("dates", values, "")
	require.NoError(t, err)
	
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Add with nulls", func(t *testing.T) {
		result := dts.Add(Days(1))
		assert.Equal(t, 3, result.Len())
		assert.False(t, result.IsNull(0))
		assert.True(t, result.IsNull(1)) // Should preserve null
		assert.False(t, result.IsNull(2))
	})

	t.Run("Diff with nulls", func(t *testing.T) {
		values2 := []string{"2024-01-14", "2024-07-30", ""}
		s2, err := NewDateTimeSeriesFromStrings("other", values2, "")
		require.NoError(t, err)
		
		diff, err := dts.Diff(s2)
		require.NoError(t, err)
		
		assert.False(t, diff.IsNull(0))
		assert.True(t, diff.IsNull(1))  // null in first series
		assert.True(t, diff.IsNull(2))  // null in second series
	})
}

func TestBusinessDays(t *testing.T) {
	t.Run("Add positive business days", func(t *testing.T) {
		// Start on Monday
		monday := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		s := NewDateSeries("dates", []time.Time{monday})
		dts, err := DtSeries(s)
		require.NoError(t, err)

		// Add 5 business days (should be next Monday)
		result := dts.AddBusinessDays(5)
		days := result.Get(0).(int32)
		date := Date{days: days}
		assert.Equal(t, time.Monday, date.Time().Weekday())
		assert.Equal(t, 22, date.Day())
	})

	t.Run("Add negative business days", func(t *testing.T) {
		// Start on Friday
		friday := time.Date(2024, 1, 19, 0, 0, 0, 0, time.UTC)
		s := NewDateSeries("dates", []time.Time{friday})
		dts, err := DtSeries(s)
		require.NoError(t, err)

		// Subtract 1 business day (should be Thursday)
		result := dts.AddBusinessDays(-1)
		days := result.Get(0).(int32)
		date := Date{days: days}
		assert.Equal(t, time.Thursday, date.Time().Weekday())
		assert.Equal(t, 18, date.Day())

		// Subtract 2 business days (should be Wednesday)
		result2 := dts.AddBusinessDays(-2)
		days2 := result2.Get(0).(int32)
		date2 := Date{days: days2}
		assert.Equal(t, time.Wednesday, date2.Time().Weekday())
		assert.Equal(t, 17, date2.Day())
	})
}