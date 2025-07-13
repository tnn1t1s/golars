package datetime

import (
	"testing"
	"time"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDateTimeSeries(t *testing.T) {
	times := []time.Time{
		time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		time.Date(2024, 7, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
	}

	s := NewDateTimeSeries("timestamps", times)
	
	assert.Equal(t, "timestamps", s.Name())
	assert.Equal(t, 3, s.Len())
	assert.IsType(t, datatypes.Datetime{}, s.DataType())
}

func TestNewDateTimeSeriesFromStrings(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		format  string
		wantLen int
		wantErr bool
	}{
		{
			name:    "ISO format",
			values:  []string{"2024-01-15T10:30:45Z", "2024-07-31T23:59:59Z", "2023-12-25T00:00:00Z"},
			format:  "",
			wantLen: 3,
		},
		{
			name:    "Custom format",
			values:  []string{"2024-01-15 10:30:45", "2024-07-31 23:59:59", "2023-12-25 00:00:00"},
			format:  "%Y-%m-%d %H:%M:%S",
			wantLen: 3,
		},
		{
			name:    "With nulls",
			values:  []string{"2024-01-15", "", "invalid", "2024-07-31"},
			format:  "",
			wantLen: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewDateTimeSeriesFromStrings("test", tt.values, tt.format)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			
			require.NoError(t, err)
			assert.Equal(t, tt.wantLen, s.Len())
		})
	}
}

func TestDateTimeOperations(t *testing.T) {
	times := []time.Time{
		time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		time.Date(2024, 7, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2023, 12, 25, 14, 15, 30, 0, time.UTC),
	}

	s := NewDateTimeSeries("timestamps", times)
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Year", func(t *testing.T) {
		year := dts.Year()
		assert.Equal(t, "timestamps.year", year.Name())
		assert.Equal(t, int32(2024), year.Get(0))
		assert.Equal(t, int32(2024), year.Get(1))
		assert.Equal(t, int32(2023), year.Get(2))
	})

	t.Run("Month", func(t *testing.T) {
		month := dts.Month()
		assert.Equal(t, "timestamps.month", month.Name())
		assert.Equal(t, int32(1), month.Get(0))
		assert.Equal(t, int32(7), month.Get(1))
		assert.Equal(t, int32(12), month.Get(2))
	})

	t.Run("Day", func(t *testing.T) {
		day := dts.Day()
		assert.Equal(t, "timestamps.day", day.Name())
		assert.Equal(t, int32(15), day.Get(0))
		assert.Equal(t, int32(31), day.Get(1))
		assert.Equal(t, int32(25), day.Get(2))
	})

	t.Run("Hour", func(t *testing.T) {
		hour := dts.Hour()
		assert.Equal(t, "timestamps.hour", hour.Name())
		assert.Equal(t, int32(10), hour.Get(0))
		assert.Equal(t, int32(23), hour.Get(1))
		assert.Equal(t, int32(14), hour.Get(2))
	})

	t.Run("DayOfWeek", func(t *testing.T) {
		dow := dts.DayOfWeek()
		assert.Equal(t, "timestamps.dayofweek", dow.Name())
		assert.Equal(t, int32(1), dow.Get(0)) // Monday
		assert.Equal(t, int32(3), dow.Get(1)) // Wednesday
		assert.Equal(t, int32(1), dow.Get(2)) // Monday
	})

	t.Run("Quarter", func(t *testing.T) {
		quarter := dts.Quarter()
		assert.Equal(t, "timestamps.quarter", quarter.Name())
		assert.Equal(t, int32(1), quarter.Get(0))
		assert.Equal(t, int32(3), quarter.Get(1))
		assert.Equal(t, int32(4), quarter.Get(2))
	})

	t.Run("IsLeapYear", func(t *testing.T) {
		isLeap := dts.IsLeapYear()
		assert.Equal(t, "timestamps.is_leap_year", isLeap.Name())
		assert.Equal(t, true, isLeap.Get(0))  // 2024 is leap
		assert.Equal(t, true, isLeap.Get(1))  // 2024 is leap
		assert.Equal(t, false, isLeap.Get(2)) // 2023 is not leap
	})
}

func TestDateSeries(t *testing.T) {
	times := []time.Time{
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 7, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
	}

	s := NewDateSeries("dates", times)
	
	assert.Equal(t, "dates", s.Name())
	assert.Equal(t, 3, s.Len())
	assert.IsType(t, datatypes.Date{}, s.DataType())

	// Test operations on Date series
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Year from Date", func(t *testing.T) {
		year := dts.Year()
		assert.Equal(t, int32(2024), year.Get(0))
		assert.Equal(t, int32(2024), year.Get(1))
		assert.Equal(t, int32(2023), year.Get(2))
	})

	t.Run("Month from Date", func(t *testing.T) {
		month := dts.Month()
		assert.Equal(t, int32(1), month.Get(0))
		assert.Equal(t, int32(7), month.Get(1))
		assert.Equal(t, int32(12), month.Get(2))
	})
}

func TestTimeSeries(t *testing.T) {
	hours := []int{10, 23, 14}
	minutes := []int{30, 59, 15}
	seconds := []int{45, 59, 30}

	s, err := NewTimeSeries("times", hours, minutes, seconds)
	require.NoError(t, err)
	
	assert.Equal(t, "times", s.Name())
	assert.Equal(t, 3, s.Len())
	assert.IsType(t, datatypes.Time{}, s.DataType())

	// Test operations on Time series
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Hour from Time", func(t *testing.T) {
		hour := dts.Hour()
		assert.Equal(t, int32(10), hour.Get(0))
		assert.Equal(t, int32(23), hour.Get(1))
		assert.Equal(t, int32(14), hour.Get(2))
	})

	t.Run("Minute from Time", func(t *testing.T) {
		minute := dts.Minute()
		assert.Equal(t, int32(30), minute.Get(0))
		assert.Equal(t, int32(59), minute.Get(1))
		assert.Equal(t, int32(15), minute.Get(2))
	})
}

func TestFormat(t *testing.T) {
	times := []time.Time{
		time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		time.Date(2024, 7, 31, 23, 59, 59, 0, time.UTC),
	}

	s := NewDateTimeSeries("timestamps", times)
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("Default format", func(t *testing.T) {
		formatted := dts.Format("")
		assert.Equal(t, "timestamps.formatted", formatted.Name())
		// Should be ISO format
		str0 := formatted.Get(0).(string)
		assert.Contains(t, str0, "2024-01-15")
		assert.Contains(t, str0, "10:30:45")
	})

	t.Run("Custom format", func(t *testing.T) {
		formatted := dts.Format("%Y-%m-%d")
		assert.Equal(t, "2024-01-15", formatted.Get(0))
		assert.Equal(t, "2024-07-31", formatted.Get(1))
	})
}

func TestNullHandling(t *testing.T) {
	// Create series with some null values
	values := []string{"2024-01-15", "", "2024-07-31", "invalid"}
	s, err := NewDateTimeSeriesFromStrings("dates", values, "")
	require.NoError(t, err)

	assert.Equal(t, 4, s.Len())
	assert.False(t, s.IsNull(0)) // Valid date
	assert.True(t, s.IsNull(1))  // Empty string
	assert.False(t, s.IsNull(2)) // Valid date
	assert.True(t, s.IsNull(3))  // Invalid date

	// Test that operations handle nulls correctly
	dts, err := DtSeries(s)
	require.NoError(t, err)

	year := dts.Year()
	assert.Equal(t, int32(2024), year.Get(0))
	assert.True(t, year.IsNull(1))
	assert.Equal(t, int32(2024), year.Get(2))
	assert.True(t, year.IsNull(3))
}