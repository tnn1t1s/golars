package datetime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateRange(t *testing.T) {
	t.Run("Daily range", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRange(start, end, Days(1))
		require.NoError(t, err)
		
		// Should have 7 dates (inclusive)
		assert.Equal(t, 7, len(dates))
		
		// Check dates are correct
		for i := 0; i < 7; i++ {
			expected := start.AddDate(0, 0, i)
			assert.Equal(t, expected.Day(), dates[i].Day())
		}
	})
	
	t.Run("Weekly range", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRange(start, end, Weeks(1))
		require.NoError(t, err)
		
		// Should have 5 dates
		assert.Equal(t, 5, len(dates))
		
		// Check weekly increments
		for i := 1; i < len(dates); i++ {
			diff := dates[i].Time().Sub(dates[i-1].Time())
			assert.Equal(t, 7*24*time.Hour, diff)
		}
	})
	
	t.Run("Monthly range", func(t *testing.T) {
		start := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRange(start, end, Months(1))
		require.NoError(t, err)
		
		// Should have 6 dates
		assert.Equal(t, 6, len(dates))
		
		// Check months increment correctly
		for i := 0; i < len(dates); i++ {
			assert.Equal(t, time.Month(i+1), dates[i].Month())
			assert.Equal(t, 15, dates[i].Day())
		}
	})
	
	t.Run("Hourly range", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 1, 23, 0, 0, 0, time.UTC)
		
		dates, err := DateRange(start, end, Hours(1))
		require.NoError(t, err)
		
		// Should have 24 dates
		assert.Equal(t, 24, len(dates))
		
		// Check hourly increments
		for i := 0; i < len(dates); i++ {
			assert.Equal(t, i, dates[i].Hour())
		}
	})
	
	t.Run("Empty range", func(t *testing.T) {
		start := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRange(start, end, Days(1))
		require.NoError(t, err)
		
		// Should be empty when end < start
		assert.Equal(t, 0, len(dates))
	})
}

func TestDateRangeWithCount(t *testing.T) {
	t.Run("Daily count", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRangeWithCount(start, Days(1), 10)
		require.NoError(t, err)
		
		assert.Equal(t, 10, len(dates))
		
		// Check dates
		for i := 0; i < 10; i++ {
			assert.Equal(t, i+1, dates[i].Day())
		}
	})
	
	t.Run("Weekly count", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRangeWithCount(start, Weeks(1), 4)
		require.NoError(t, err)
		
		assert.Equal(t, 4, len(dates))
		
		// Check weekly increments
		expectedDays := []int{1, 8, 15, 22}
		for i, date := range dates {
			assert.Equal(t, expectedDays[i], date.Day())
		}
	})
	
	t.Run("Negative count", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRangeWithCount(start, Days(1), -5)
		require.NoError(t, err)
		
		// Should be empty with negative count
		assert.Equal(t, 0, len(dates))
	})
	
	t.Run("Zero count", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		
		dates, err := DateRangeWithCount(start, Days(1), 0)
		require.NoError(t, err)
		
		assert.Equal(t, 0, len(dates))
	})
}


func TestDateRangeBuilder(t *testing.T) {
	t.Run("Builder with all options", func(t *testing.T) {
		start := NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
		end := NewDateTime(time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC))
		
		builder := NewDateRangeBuilder().
			Start(start).
			End(end).
			Frequency(Days(1)).
			Inclusive("both")
		
		dates, err := builder.Build()
		require.NoError(t, err)
		
		assert.Equal(t, 7, len(dates))
	})
	
	t.Run("Builder with count", func(t *testing.T) {
		start := NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
		
		builder := NewDateRangeBuilder().
			Start(start).
			Frequency(Days(2)).
			Count(5)
		
		dates, err := builder.Build()
		require.NoError(t, err)
		
		assert.Equal(t, 5, len(dates))
		
		// Check 2-day increments
		for i := 1; i < len(dates); i++ {
			diff := dates[i].Time().Sub(dates[i-1].Time())
			assert.Equal(t, 2*24*time.Hour, diff)
		}
	})
	
	t.Run("Builder errors", func(t *testing.T) {
		// Missing start
		builder := NewDateRangeBuilder().
			End(NewDateTime(time.Now())).
			Frequency(Days(1))
		
		_, err := builder.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "start date")
		
		// Missing frequency
		builder = NewDateRangeBuilder().
			Start(NewDateTime(time.Now())).
			End(NewDateTime(time.Now().Add(24 * time.Hour)))
		
		_, err = builder.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "frequency")
		
		// Missing end and count
		builder = NewDateRangeBuilder().
			Start(NewDateTime(time.Now())).
			Frequency(Days(1))
		
		_, err = builder.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "end date or count")
	})
}

func TestDateRangeEdgeCases(t *testing.T) {
	t.Run("Large range", func(t *testing.T) {
		start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		
		// Yearly range
		dates, err := DateRange(start, end, Years(1))
		require.NoError(t, err)
		
		assert.Equal(t, 6, len(dates)) // 2020-2025 inclusive
	})
	
	t.Run("Nanosecond precision", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := start.Add(10 * time.Nanosecond)
		
		dates, err := DateRange(start, end, Duration{nanoseconds: 1})
		require.NoError(t, err)
		
		assert.Equal(t, 11, len(dates)) // 0-10 nanoseconds inclusive
	})
}