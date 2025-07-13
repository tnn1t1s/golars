package datetime

import (
	"testing"
	"time"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFrequency(t *testing.T) {
	tests := []struct {
		name     string
		freq     string
		wantDur  int64
		wantUnit TimeUnit
		wantErr  bool
	}{
		{"1 day", "1D", 1, Day, false},
		{"2 hours", "2H", 2, Hour, false},
		{"30 minutes", "30min", 30, Minute, false},
		{"5 seconds", "5s", 5, Second, false},
		{"1 week", "1W", 1, Week, false},
		{"Default day", "D", 1, Day, false},
		{"Default hour", "H", 1, Hour, false},
		{"Invalid", "xyz", 0, Nanosecond, true},
		{"Empty", "", 0, Nanosecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dur, unit, err := parseFrequency(tt.freq)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDur, dur)
				assert.Equal(t, tt.wantUnit, unit)
			}
		})
	}
}

func TestResampleHourly(t *testing.T) {
	// Create hourly data with some values
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	times := make([]time.Time, 24)
	values := make([]float64, 24)
	
	for i := 0; i < 24; i++ {
		times[i] = startTime.Add(time.Duration(i) * time.Hour)
		values[i] = float64(i + 1) // Values 1-24
	}
	
	timeSeries := NewDateTimeSeries("timestamp", times)
	valueSeries := series.NewSeries("value", values, datatypes.Float64{})
	
	dts, err := DtSeries(timeSeries)
	require.NoError(t, err)

	t.Run("Resample to 6H", func(t *testing.T) {
		rule := NewResampleRule("6H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		// Sum aggregation
		sumResult, err := resampler.Sum(valueSeries)
		require.NoError(t, err)
		
		// Should have 4 bins (0-6, 6-12, 12-18, 18-24)
		assert.Equal(t, 4, sumResult.Len())
		
		// Check sums: 1+2+3+4+5+6=21, 7+8+9+10+11+12=57, etc.
		assert.Equal(t, 21.0, sumResult.Get(0))
		assert.Equal(t, 57.0, sumResult.Get(1))
		assert.Equal(t, 93.0, sumResult.Get(2))
		assert.Equal(t, 129.0, sumResult.Get(3))
		
		// Mean aggregation
		meanResult, err := resampler.Mean(valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 4, meanResult.Len())
		assert.Equal(t, 3.5, meanResult.Get(0))  // (1+2+3+4+5+6)/6
		assert.Equal(t, 9.5, meanResult.Get(1))  // (7+8+9+10+11+12)/6
		assert.Equal(t, 15.5, meanResult.Get(2)) // (13+14+15+16+17+18)/6
		assert.Equal(t, 21.5, meanResult.Get(3)) // (19+20+21+22+23+24)/6
		
		// Count aggregation
		countResult, err := resampler.Count()
		require.NoError(t, err)
		
		assert.Equal(t, 4, countResult.Len())
		assert.Equal(t, uint32(6), countResult.Get(0))
		assert.Equal(t, uint32(6), countResult.Get(1))
		assert.Equal(t, uint32(6), countResult.Get(2))
		assert.Equal(t, uint32(6), countResult.Get(3))
	})

	t.Run("Resample to 1D", func(t *testing.T) {
		rule := NewResampleRule("1D")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		// Should have 1 bin for the whole day
		sumResult, err := resampler.Sum(valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 1, sumResult.Len())
		assert.Equal(t, 300.0, sumResult.Get(0)) // Sum of 1-24 = 300
		
		meanResult, err := resampler.Mean(valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 12.5, meanResult.Get(0)) // Average of 1-24
	})

	t.Run("Min/Max aggregation", func(t *testing.T) {
		rule := NewResampleRule("12H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		minResult, err := resampler.Aggregate("min", valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 2, minResult.Len())
		assert.Equal(t, 1.0, minResult.Get(0))  // Min of first 12 hours
		assert.Equal(t, 13.0, minResult.Get(1)) // Min of second 12 hours
		
		maxResult, err := resampler.Aggregate("max", valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 12.0, maxResult.Get(0)) // Max of first 12 hours
		assert.Equal(t, 24.0, maxResult.Get(1)) // Max of second 12 hours
	})

	t.Run("First/Last aggregation", func(t *testing.T) {
		rule := NewResampleRule("8H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		firstResult, err := resampler.Aggregate("first", valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 3, firstResult.Len())
		assert.Equal(t, 1.0, firstResult.Get(0))  // First value in 0-8
		assert.Equal(t, 9.0, firstResult.Get(1))  // First value in 8-16
		assert.Equal(t, 17.0, firstResult.Get(2)) // First value in 16-24
		
		lastResult, err := resampler.Aggregate("last", valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 8.0, lastResult.Get(0))  // Last value in 0-8
		assert.Equal(t, 16.0, lastResult.Get(1)) // Last value in 8-16
		assert.Equal(t, 24.0, lastResult.Get(2)) // Last value in 16-24
	})
}

func TestResampleWithNulls(t *testing.T) {
	// Create data with some null values
	times := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 4, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 5, 0, 0, 0, time.UTC),
	}
	
	values := []float64{1.0, 2.0, 0.0, 4.0, 0.0, 6.0}
	validity := []bool{true, true, false, true, false, true} // nulls at index 2,4
	
	timeSeries := NewDateTimeSeries("timestamp", times)
	valueSeries := series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{})
	
	dts, err := DtSeries(timeSeries)
	require.NoError(t, err)

	t.Run("Sum with nulls", func(t *testing.T) {
		rule := NewResampleRule("3H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		sumResult, err := resampler.Sum(valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 2, sumResult.Len())
		assert.Equal(t, 3.0, sumResult.Get(0))  // 1+2 (null ignored)
		assert.Equal(t, 10.0, sumResult.Get(1)) // 4+6 (null ignored)
	})

	t.Run("Mean with nulls", func(t *testing.T) {
		rule := NewResampleRule("3H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		meanResult, err := resampler.Mean(valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 2, meanResult.Len())
		assert.Equal(t, 1.5, meanResult.Get(0)) // (1+2)/2
		assert.Equal(t, 5.0, meanResult.Get(1)) // (4+6)/2
	})

	t.Run("Count includes null timestamps", func(t *testing.T) {
		rule := NewResampleRule("3H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		countResult, err := resampler.Count()
		require.NoError(t, err)
		
		assert.Equal(t, 2, countResult.Len())
		assert.Equal(t, uint32(3), countResult.Get(0)) // 3 timestamps in first bin
		assert.Equal(t, uint32(3), countResult.Get(1)) // 3 timestamps in second bin
	})
}

func TestResampleDifferentFrequencies(t *testing.T) {
	// Create a week of hourly data
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	hours := 24 * 7 // One week
	times := make([]time.Time, hours)
	values := make([]float64, hours)
	
	for i := 0; i < hours; i++ {
		times[i] = startTime.Add(time.Duration(i) * time.Hour)
		values[i] = float64(i)
	}
	
	timeSeries := NewDateTimeSeries("timestamp", times)
	// valueSeries will be used in aggregation tests
	_ = series.NewSeries("value", values, datatypes.Float64{})
	
	dts, err := DtSeries(timeSeries)
	require.NoError(t, err)

	t.Run("Resample to days", func(t *testing.T) {
		rule := NewResampleRule("1D")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		countResult, err := resampler.Count()
		require.NoError(t, err)
		
		assert.Equal(t, 7, countResult.Len()) // 7 days
		for i := 0; i < 7; i++ {
			assert.Equal(t, uint32(24), countResult.Get(i)) // 24 hours per day
		}
	})

	t.Run("Resample to week", func(t *testing.T) {
		rule := NewResampleRule("1W")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		countResult, err := resampler.Count()
		require.NoError(t, err)
		
		assert.Equal(t, 1, countResult.Len()) // 1 week
		assert.Equal(t, uint32(168), countResult.Get(0)) // 24*7 hours
	})

	t.Run("Resample to 30 minutes", func(t *testing.T) {
		// Just test first day
		dayTimes := times[:24]
		dayValues := values[:24]
		
		dayTimeSeries := NewDateTimeSeries("timestamp", dayTimes)
		// dayValueSeries will be used in future tests
		_ = series.NewSeries("value", dayValues, datatypes.Float64{})
		
		dayDts, err := DtSeries(dayTimeSeries)
		require.NoError(t, err)
		
		rule := NewResampleRule("30min")
		resampler, err := dayDts.Resample(rule)
		require.NoError(t, err)
		
		countResult, err := resampler.Count()
		require.NoError(t, err)
		
		// Each hour has 2 30-minute bins, but our data is hourly
		// So each value falls in one bin, with empty bins in between
		// We should see values in every other bin
		expectedBins := 0
		for i := 0; i < countResult.Len(); i++ {
			if countResult.Get(i).(uint32) > 0 {
				expectedBins++
			}
		}
		assert.Equal(t, 24, expectedBins) // 24 hours of data
	})
}

func TestResampleEdgeCases(t *testing.T) {
	t.Run("Empty series", func(t *testing.T) {
		emptyTimes := []time.Time{}
		timeSeries := NewDateTimeSeries("timestamp", emptyTimes)
		
		dts, err := DtSeries(timeSeries)
		require.NoError(t, err)
		
		rule := NewResampleRule("1H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		countResult, err := resampler.Count()
		require.NoError(t, err)
		
		assert.Equal(t, 0, countResult.Len())
	})

	t.Run("Single value", func(t *testing.T) {
		times := []time.Time{time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)}
		values := []float64{42.0}
		
		timeSeries := NewDateTimeSeries("timestamp", times)
		valueSeries := series.NewSeries("value", values, datatypes.Float64{})
		
		dts, err := DtSeries(timeSeries)
		require.NoError(t, err)
		
		rule := NewResampleRule("1H")
		resampler, err := dts.Resample(rule)
		require.NoError(t, err)
		
		sumResult, err := resampler.Sum(valueSeries)
		require.NoError(t, err)
		
		assert.Equal(t, 1, sumResult.Len())
		assert.Equal(t, 42.0, sumResult.Get(0))
	})

	t.Run("Invalid frequency", func(t *testing.T) {
		times := []time.Time{time.Now()}
		timeSeries := NewDateTimeSeries("timestamp", times)
		
		dts, err := DtSeries(timeSeries)
		require.NoError(t, err)
		
		rule := NewResampleRule("invalid")
		_, err = dts.Resample(rule)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid frequency")
	})

	t.Run("Nil rule", func(t *testing.T) {
		times := []time.Time{time.Now()}
		timeSeries := NewDateTimeSeries("timestamp", times)
		
		dts, err := DtSeries(timeSeries)
		require.NoError(t, err)
		
		_, err = dts.Resample(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rule cannot be nil")
	})

	t.Run("Non-datetime series", func(t *testing.T) {
		// Try to resample a non-datetime series
		intSeries := series.NewSeries("ints", []int32{1, 2, 3}, datatypes.Int32{})
		
		dts := &DateTimeSeries{s: intSeries}
		
		rule := NewResampleRule("1H")
		_, err := dts.Resample(rule)
		assert.NoError(t, err) // Should handle gracefully
	})
}