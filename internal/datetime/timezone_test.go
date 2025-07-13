package datetime

import (
	"testing"
	"time"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadTimezone(t *testing.T) {
	tests := []struct {
		name    string
		tzName  string
		wantErr bool
	}{
		{"UTC", "UTC", false},
		{"GMT", "GMT", false},
		{"Local", "Local", false},
		{"EST alias", "EST", false},
		{"PST alias", "PST", false},
		{"America/New_York", "America/New_York", false},
		{"Europe/London", "Europe/London", false},
		{"Asia/Tokyo", "Asia/Tokyo", false},
		{"Invalid", "Invalid/Timezone", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tz, err := LoadTimezone(tt.tzName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, tz)
			}
		})
	}
}

func TestDateTimeTimezone(t *testing.T) {
	// Create a datetime in UTC
	utcTime := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)
	dt := NewDateTime(utcTime)

	t.Run("ConvertTimezone", func(t *testing.T) {
		// Convert to EST (UTC-5 in June)
		est, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		
		dtEST := dt.ConvertTimezone(est)
		
		// The timestamp should be the same, but when displayed it should show EST time
		assert.Equal(t, dt.timestamp, dtEST.timestamp)
		assert.Equal(t, est, dtEST.timezone)
		
		// When converted to time.Time, it should show the correct local time
		estTime := dtEST.Time()
		assert.Equal(t, 10, estTime.Hour()) // 14:30 UTC = 10:30 EST (summer time)
		assert.Equal(t, 30, estTime.Minute())
	})

	t.Run("WithTimezone", func(t *testing.T) {
		// WithTimezone changes timezone without converting
		est, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		
		dtEST := dt.WithTimezone(est)
		
		// The timestamp should be the same
		assert.Equal(t, dt.timestamp, dtEST.timestamp)
		assert.Equal(t, est, dtEST.timezone)
		
		// When displayed, it interprets the timestamp as EST
		estTime := dtEST.Time()
		// The timestamp represents 14:30 UTC, which is 10:30 EST (UTC-4 in June)
		assert.Equal(t, 10, estTime.Hour())
	})

	t.Run("InTimezone", func(t *testing.T) {
		dtTokyo, err := dt.InTimezone("Asia/Tokyo")
		require.NoError(t, err)
		
		// Tokyo is UTC+9
		tokyoTime := dtTokyo.Time()
		assert.Equal(t, 23, tokyoTime.Hour()) // 14:30 UTC = 23:30 JST
		assert.Equal(t, 30, tokyoTime.Minute())
	})

	t.Run("ToUTC", func(t *testing.T) {
		// Start with a non-UTC datetime
		est, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		
		estTime := time.Date(2024, 6, 15, 10, 30, 0, 0, est)
		dtEST := NewDateTime(estTime)
		
		// Convert to UTC
		dtUTC := dtEST.ToUTC()
		utcTime := dtUTC.Time()
		
		assert.Equal(t, time.UTC, dtUTC.timezone)
		assert.Equal(t, 14, utcTime.Hour()) // 10:30 EST = 14:30 UTC (summer time)
		assert.Equal(t, 30, utcTime.Minute())
	})

	t.Run("ToLocal", func(t *testing.T) {
		dtLocal := dt.ToLocal()
		assert.Equal(t, time.Local, dtLocal.timezone)
	})
}

func TestDateTimeSeriesTimezone(t *testing.T) {
	// Create datetime series in UTC
	times := []time.Time{
		time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
		time.Date(2024, 12, 15, 12, 0, 0, 0, time.UTC),
	}
	
	s := NewDateTimeSeries("timestamps", times)
	dts, err := DtSeries(s)
	require.NoError(t, err)

	t.Run("ConvertTimezone", func(t *testing.T) {
		// Convert to America/New_York
		nyS, err := dts.ConvertTimezone("America/New_York")
		require.NoError(t, err)
		
		assert.Equal(t, "timestamps_tz_America/New_York", nyS.Name())
		assert.Equal(t, 3, nyS.Len())
		
		// Check timezone in datatype
		dt := nyS.DataType().(datatypes.Datetime)
		assert.Contains(t, dt.TimeZone.String(), "America/New_York")
		
		// Check that values are correct
		// January is EST (UTC-5), June is EDT (UTC-4), December is EST (UTC-5)
		ts0 := nyS.Get(0).(int64)
		dt0 := DateTime{timestamp: ts0, timezone: time.UTC}
		assert.Equal(t, 12, dt0.Hour()) // Still 12 in timestamp
		
		// The actual conversion happens when interpreting the timestamp
		ny, _ := time.LoadLocation("America/New_York")
		dt0NY := DateTime{timestamp: ts0, timezone: ny}
		assert.Equal(t, 7, dt0NY.Time().Hour()) // 12 UTC = 7 EST in January
	})

	t.Run("ToUTC", func(t *testing.T) {
		utcS := dts.ToUTC()
		assert.Equal(t, "timestamps_tz_UTC", utcS.Name())
		
		dt := utcS.DataType().(datatypes.Datetime)
		assert.Equal(t, "UTC", dt.TimeZone.String())
	})

	t.Run("ToLocal", func(t *testing.T) {
		localS := dts.ToLocal()
		assert.Equal(t, "timestamps_tz_Local", localS.Name())
	})

	t.Run("Localize", func(t *testing.T) {
		// Localize interprets naive timestamps as being in the specified timezone
		localizedS, err := dts.Localize("Europe/London")
		require.NoError(t, err)
		
		assert.Equal(t, "timestamps_localized_Europe/London", localizedS.Name())
		
		// Values should be the same (no conversion)
		for i := 0; i < s.Len(); i++ {
			assert.Equal(t, s.Get(i), localizedS.Get(i))
		}
		
		// But timezone metadata should be updated
		dt := localizedS.DataType().(datatypes.Datetime)
		assert.Contains(t, dt.TimeZone.String(), "Europe/London")
	})

	t.Run("GetTimezone", func(t *testing.T) {
		// Default should be UTC
		tz := dts.GetTimezone()
		assert.Equal(t, "UTC", tz)
		
		// After conversion
		nyS, err := dts.ConvertTimezone("America/New_York")
		require.NoError(t, err)
		
		nyDts, err := DtSeries(nyS)
		require.NoError(t, err)
		
		nyTz := nyDts.GetTimezone()
		assert.Contains(t, nyTz, "America/New_York")
	})

	t.Run("NullHandling", func(t *testing.T) {
		// Create series with nulls
		values := []string{"2024-01-15 12:00:00", "", "2024-12-15 12:00:00"}
		s, err := NewDateTimeSeriesFromStrings("dates", values, "")
		require.NoError(t, err)
		
		dts, err := DtSeries(s)
		require.NoError(t, err)
		
		// Convert timezone
		converted, err := dts.ConvertTimezone("Asia/Tokyo")
		require.NoError(t, err)
		
		assert.Equal(t, 3, converted.Len())
		assert.False(t, converted.IsNull(0))
		assert.True(t, converted.IsNull(1)) // Null preserved
		assert.False(t, converted.IsNull(2))
	})
}

func TestTimezoneExpressions(t *testing.T) {
	col := expr.Col("timestamp")
	dtExpr := DtExpr(col)

	t.Run("ConvertTimezone expression", func(t *testing.T) {
		tzExpr := dtExpr.ConvertTimezone("America/New_York")
		assert.Equal(t, `col(timestamp).dt.convert_timezone("America/New_York")`, tzExpr.String())
		assert.Equal(t, "timestamp_tz_America/New_York", tzExpr.Name())
		
		dt := tzExpr.DataType().(datatypes.Datetime)
		assert.Equal(t, "America/New_York", dt.TimeZone.String())
	})

	t.Run("ToUTC expression", func(t *testing.T) {
		utcExpr := dtExpr.ToUTC()
		assert.Equal(t, `col(timestamp).dt.convert_timezone("UTC")`, utcExpr.String())
		assert.Equal(t, "timestamp_tz_UTC", utcExpr.Name())
	})

	t.Run("ToLocal expression", func(t *testing.T) {
		localExpr := dtExpr.ToLocal()
		assert.Equal(t, `col(timestamp).dt.convert_timezone("Local")`, localExpr.String())
		assert.Equal(t, "timestamp_tz_Local", localExpr.Name())
	})
}

func TestTimezoneEdgeCases(t *testing.T) {
	t.Run("DST transition", func(t *testing.T) {
		// Test around DST transition
		ny, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		
		// March 10, 2024 2:00 AM EST -> 3:00 AM EDT
		beforeDST := time.Date(2024, 3, 10, 1, 30, 0, 0, ny)
		afterDST := time.Date(2024, 3, 10, 3, 30, 0, 0, ny)
		
		dtBefore := NewDateTime(beforeDST)
		dtAfter := NewDateTime(afterDST)
		
		// Convert to UTC
		utcBefore := dtBefore.ToUTC()
		utcAfter := dtAfter.ToUTC()
		
		// Check the difference is 1 hour (not 2)
		diff := utcAfter.timestamp - utcBefore.timestamp
		assert.Equal(t, int64(time.Hour), diff)
	})

	t.Run("Invalid timezone", func(t *testing.T) {
		times := []time.Time{time.Now()}
		s := NewDateTimeSeries("test", times)
		dts, err := DtSeries(s)
		require.NoError(t, err)
		
		_, err = dts.ConvertTimezone("Invalid/Timezone")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load timezone")
	})
}