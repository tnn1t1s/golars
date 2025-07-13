package datetime

import (
	"testing"
	"time"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrameIntegrationWithDateTime(t *testing.T) {
	// Create test data
	dates := []time.Time{
		time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		time.Date(2024, 2, 20, 14, 15, 30, 0, time.UTC),
		time.Date(2024, 3, 25, 18, 45, 00, 0, time.UTC),
		time.Date(2024, 4, 30, 22, 00, 15, 0, time.UTC),
	}
	
	values := []float64{100.5, 200.75, 150.25, 300.0}
	categories := []string{"A", "B", "A", "B"}
	
	// Create series
	dateSeries := NewDateTimeSeries("timestamp", dates)
	valueSeries := series.NewSeries("value", values, datatypes.Float64{})
	categorySeries := series.NewSeries("category", categories, datatypes.String{})
	
	// Create DataFrame
	df, err := frame.NewDataFrame(dateSeries, valueSeries, categorySeries)
	require.NoError(t, err)
	
	t.Run("Basic DataFrame with DateTime", func(t *testing.T) {
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, 4, df.Height())
		assert.Equal(t, []string{"timestamp", "value", "category"}, df.Columns())
		
		// Get timestamp column
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		assert.NotNil(t, tsCol)
		assert.Equal(t, "timestamp", tsCol.Name())
	})
	
	t.Run("Extract datetime components as new series", func(t *testing.T) {
		// Get the datetime series
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		// Use DtSeries to access datetime operations
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Extract components
		yearSeries := dts.Year()
		monthSeries := dts.Month()
		daySeries := dts.Day()
		
		// Create new dataframe with components
		df2, err := frame.NewDataFrame(tsCol, yearSeries, monthSeries, daySeries, valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 5, df2.Width())
		assert.Equal(t, []string{"timestamp", "timestamp.year", "timestamp.month", "timestamp.day", "value"}, df2.Columns())
		
		// Check year values
		yearCol, err := df2.Column("timestamp.year")
		require.NoError(t, err)
		assert.Equal(t, int32(2024), yearCol.Get(0))
		assert.Equal(t, int32(2024), yearCol.Get(1))
		
		// Check month values
		monthCol, err := df2.Column("timestamp.month")
		require.NoError(t, err)
		assert.Equal(t, int32(1), monthCol.Get(0))
		assert.Equal(t, int32(2), monthCol.Get(1))
		assert.Equal(t, int32(3), monthCol.Get(2))
		assert.Equal(t, int32(4), monthCol.Get(3))
	})
	
	t.Run("DateTime arithmetic in DataFrame", func(t *testing.T) {
		// Get the datetime series
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Add 7 days
		futureTS := dts.Add(Days(7))
		
		// Create new dataframe with original and future timestamps
		df2, err := frame.NewDataFrame(tsCol, futureTS, valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 3, df2.Width())
		assert.Equal(t, []string{"timestamp", "timestamp_plus_7 days", "value"}, df2.Columns())
		
		// Verify the difference is 7 days
		origCol, err := df2.Column("timestamp")
		require.NoError(t, err)
		futureCol, err := df2.Column("timestamp_plus_7 days")
		require.NoError(t, err)
		
		for i := 0; i < df2.Height(); i++ {
			orig := DateTime{timestamp: origCol.Get(i).(int64), timezone: time.UTC}
			future := DateTime{timestamp: futureCol.Get(i).(int64), timezone: time.UTC}
			
			diff := future.Time().Sub(orig.Time())
			assert.Equal(t, 7*24*time.Hour, diff)
		}
	})
	
	t.Run("Format datetime in DataFrame", func(t *testing.T) {
		// Get the datetime series
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Format as date strings
		dateStrings := dts.Format("%Y-%m-%d")
		timeStrings := dts.Format("%H:%M")
		
		// Create new dataframe with formatted strings
		df2, err := frame.NewDataFrame(dateStrings, timeStrings, valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 3, df2.Width())
		
		// Check formatted values
		dateCol, err := df2.Column("timestamp.formatted")
		require.NoError(t, err)
		assert.Equal(t, "2024-01-15", dateCol.Get(0))
		assert.Equal(t, "2024-02-20", dateCol.Get(1))
		assert.Equal(t, "2024-03-25", dateCol.Get(2))
		assert.Equal(t, "2024-04-30", dateCol.Get(3))
	})
	
	t.Run("Business day calculations", func(t *testing.T) {
		// Get the datetime series
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Add 5 business days
		businessTS := dts.AddBusinessDays(5)
		
		// Create new dataframe
		df2, err := frame.NewDataFrame(tsCol, businessTS)
		require.NoError(t, err)
		
		assert.Equal(t, 2, df2.Width())
		
		// Verify business days were added correctly
		origCol, err := df2.Column("timestamp")
		require.NoError(t, err)
		businessCol, err := df2.Column("timestamp_plus_5_business_days")
		require.NoError(t, err)
		
		for i := 0; i < df2.Height(); i++ {
			orig := DateTime{timestamp: origCol.Get(i).(int64), timezone: time.UTC}
			business := DateTime{timestamp: businessCol.Get(i).(int64), timezone: time.UTC}
			
			// The difference should be at least 5 days (could be more with weekends)
			daysDiff := int(business.Time().Sub(orig.Time()).Hours() / 24)
			assert.GreaterOrEqual(t, daysDiff, 5)
		}
	})
}

func TestDataFrameWithDateStrings(t *testing.T) {
	// Create DataFrame with string dates
	dateStrings := []string{
		"2024-01-15 10:30:45",
		"2024-02-20 14:15:30",
		"invalid date",
		"2024-04-30 22:00:15",
	}
	
	values := []float64{100.5, 200.75, 150.25, 300.0}
	
	stringSeries := series.NewSeries("date_str", dateStrings, datatypes.String{})
	valueSeries := series.NewSeries("value", values, datatypes.Float64{})
	
	_, err := frame.NewDataFrame(stringSeries, valueSeries)
	require.NoError(t, err)
	
	t.Run("Parse datetime strings", func(t *testing.T) {
		// Parse the string series to datetime
		parsedSeries, err := NewDateTimeSeriesFromStrings("parsed_timestamp", dateStrings, "%Y-%m-%d %H:%M:%S")
		require.NoError(t, err)
		
		// Create new dataframe with parsed dates
		df2, err := frame.NewDataFrame(stringSeries, parsedSeries, valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 3, df2.Width())
		
		// Check that invalid date resulted in null
		parsedCol, err := df2.Column("parsed_timestamp")
		require.NoError(t, err)
		
		assert.False(t, parsedCol.IsNull(0))
		assert.False(t, parsedCol.IsNull(1))
		assert.True(t, parsedCol.IsNull(2)) // Invalid date
		assert.False(t, parsedCol.IsNull(3))
		
		// Extract year from parsed dates
		dts, err := DtSeries(parsedCol)
		require.NoError(t, err)
		
		yearSeries := dts.Year()
		assert.Equal(t, int32(2024), yearSeries.Get(0))
		assert.Equal(t, int32(2024), yearSeries.Get(1))
		assert.True(t, yearSeries.IsNull(2))
		assert.Equal(t, int32(2024), yearSeries.Get(3))
	})
}

func TestTimeSeriesDataFrameAnalysis(t *testing.T) {
	// Create hourly time series data for one week
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	times := make([]time.Time, 24*7) // One week of hourly data
	values := make([]float64, 24*7)
	
	for i := range times {
		times[i] = startTime.Add(time.Duration(i) * time.Hour)
		// Simulate some pattern: higher values during day hours
		hour := times[i].Hour()
		if hour >= 9 && hour <= 17 {
			values[i] = 100 + float64(i%24)*2
		} else {
			values[i] = 50 + float64(i%24)
		}
	}
	
	timeSeries := NewDateTimeSeries("timestamp", times)
	valueSeries := series.NewSeries("value", values, datatypes.Float64{})
	
	df, err := frame.NewDataFrame(timeSeries, valueSeries)
	require.NoError(t, err)
	
	t.Run("Extract hour for analysis", func(t *testing.T) {
		// Get timestamp column
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Extract hour and day of week
		hourSeries := dts.Hour()
		dowSeries := dts.DayOfWeek()
		
		// Create analysis dataframe
		analysisDF, err := frame.NewDataFrame(timeSeries, hourSeries, dowSeries, valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 4, analysisDF.Width())
		assert.Equal(t, 168, analysisDF.Height()) // 24 * 7 hours
		
		// Verify hour extraction
		hourCol, err := analysisDF.Column("timestamp.hour")
		require.NoError(t, err)
		
		// First 24 values should be 0-23
		for i := 0; i < 24; i++ {
			assert.Equal(t, int32(i), hourCol.Get(i))
		}
	})
	
	t.Run("Filter business hours", func(t *testing.T) {
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Check weekends
		isWeekendSeries := dts.IsWeekend()
		
		// Count weekend entries (should be 48 = 2 days * 24 hours)
		weekendCount := 0
		for i := 0; i < isWeekendSeries.Len(); i++ {
			if isWeekendSeries.Get(i).(bool) {
				weekendCount++
			}
		}
		
		// The count depends on which day of week we started
		// But we should have at least some weekend hours
		assert.Greater(t, weekendCount, 0)
		assert.LessOrEqual(t, weekendCount, 48)
	})
	
	t.Run("Round to day for daily aggregation", func(t *testing.T) {
		tsCol, err := df.Column("timestamp")
		require.NoError(t, err)
		
		dts, err := DtSeries(tsCol)
		require.NoError(t, err)
		
		// Floor timestamps to day
		dailyTS := dts.Floor(Day)
		
		// Create dataframe with daily timestamps
		dailyDF, err := frame.NewDataFrame(dailyTS, valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 2, dailyDF.Width())
		
		// Verify flooring worked - all timestamps at same day should be identical
		dayCol, err := dailyDF.Column("timestamp.floor_D")
		require.NoError(t, err)
		
		// First 24 values should all be the same day
		firstDay := dayCol.Get(0).(int64)
		for i := 0; i < 24; i++ {
			assert.Equal(t, firstDay, dayCol.Get(i).(int64))
		}
		
		// Next 24 should be the next day
		secondDay := dayCol.Get(24).(int64)
		for i := 24; i < 48; i++ {
			assert.Equal(t, secondDay, dayCol.Get(i).(int64))
		}
	})
}