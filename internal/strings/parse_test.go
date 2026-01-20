package strings

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func TestStringToInteger(t *testing.T) {
	t.Run("Basic integer parsing", func(t *testing.T) {
		s := series.NewStringSeries("nums", []string{"123", "456", "-789", "0"})
		ops := NewStringOps(s)

		intSeries, err := ops.ToInteger()
		require.NoError(t, err)
		assert.Equal(t, 4, intSeries.Len())
		// The type will be Int16 since we have negative values
		assert.Equal(t, datatypes.Int16{}, intSeries.DataType())
		assert.Equal(t, int16(123), intSeries.Get(0))
		assert.Equal(t, int16(456), intSeries.Get(1))
		assert.Equal(t, int16(-789), intSeries.Get(2))
		assert.Equal(t, int16(0), intSeries.Get(3))
	})

	t.Run("With base", func(t *testing.T) {
		s := series.NewStringSeries("hex", []string{"FF", "10", "A0"})
		ops := NewStringOps(s)

		intSeries, err := ops.ToInteger(16)
		require.NoError(t, err)
		assert.Equal(t, 3, intSeries.Len())
		assert.Equal(t, uint8(255), intSeries.Get(0))
		assert.Equal(t, uint8(16), intSeries.Get(1))
		assert.Equal(t, uint8(160), intSeries.Get(2))
	})

	t.Run("With invalid values", func(t *testing.T) {
		s := series.NewStringSeries("mixed", []string{"123", "abc", "456", ""})
		ops := NewStringOps(s)

		intSeries, err := ops.ToInteger()
		require.NoError(t, err)
		assert.Equal(t, 4, intSeries.Len())
		assert.False(t, intSeries.IsNull(0))
		assert.True(t, intSeries.IsNull(1)) // "abc" is invalid
		assert.False(t, intSeries.IsNull(2))
		assert.True(t, intSeries.IsNull(3)) // empty string
		// Since we only have positive values 123 and 456, type will be UInt16
		assert.Equal(t, datatypes.UInt16{}, intSeries.DataType())
		assert.Equal(t, uint16(123), intSeries.Get(0))
		assert.Equal(t, uint16(456), intSeries.Get(2))
	})

	t.Run("Large integers", func(t *testing.T) {
		s := series.NewStringSeries("large", []string{"2147483647", "-2147483648", "9223372036854775807"})
		ops := NewStringOps(s)

		intSeries, err := ops.ToInteger()
		require.NoError(t, err)
		assert.Equal(t, 3, intSeries.Len())
		assert.Equal(t, datatypes.Int64{}, intSeries.DataType())
		assert.Equal(t, int64(2147483647), intSeries.Get(0))
		assert.Equal(t, int64(-2147483648), intSeries.Get(1))
		assert.Equal(t, int64(9223372036854775807), intSeries.Get(2))
	})
}

func TestStringToFloat(t *testing.T) {
	t.Run("Basic float parsing", func(t *testing.T) {
		s := series.NewStringSeries("floats", []string{"123.45", "-67.89", "0.0", "1e10"})
		ops := NewStringOps(s)

		floatSeries, err := ops.ToFloat()
		require.NoError(t, err)
		assert.Equal(t, 4, floatSeries.Len())
		assert.Equal(t, 123.45, floatSeries.Get(0))
		assert.Equal(t, -67.89, floatSeries.Get(1))
		assert.Equal(t, 0.0, floatSeries.Get(2))
		assert.Equal(t, 1e10, floatSeries.Get(3))
	})

	t.Run("Special values", func(t *testing.T) {
		s := series.NewStringSeries("special", []string{"inf", "-inf", "nan", "NaN", "Infinity"})
		ops := NewStringOps(s)

		floatSeries, err := ops.ToFloat()
		require.NoError(t, err)
		assert.Equal(t, 5, floatSeries.Len())
		assert.True(t, math.IsInf(floatSeries.Get(0).(float64), 1))
		assert.True(t, math.IsInf(floatSeries.Get(1).(float64), -1))
		assert.True(t, math.IsNaN(floatSeries.Get(2).(float64)))
		assert.True(t, math.IsNaN(floatSeries.Get(3).(float64)))
		assert.True(t, math.IsInf(floatSeries.Get(4).(float64), 1))
	})

	t.Run("With invalid values", func(t *testing.T) {
		s := series.NewStringSeries("mixed", []string{"123.45", "abc", "67.89", ""})
		ops := NewStringOps(s)

		floatSeries, err := ops.ToFloat()
		require.NoError(t, err)
		assert.Equal(t, 4, floatSeries.Len())
		assert.False(t, floatSeries.IsNull(0))
		assert.True(t, floatSeries.IsNull(1))
		assert.False(t, floatSeries.IsNull(2))
		assert.True(t, floatSeries.IsNull(3))
		assert.Equal(t, 123.45, floatSeries.Get(0))
		assert.Equal(t, 67.89, floatSeries.Get(2))
	})
}

func TestStringToBoolean(t *testing.T) {
	t.Run("Various boolean formats", func(t *testing.T) {
		s := series.NewStringSeries("bools", []string{
			"true", "false", "True", "False",
			"t", "f", "T", "F",
			"yes", "no", "Yes", "No",
			"y", "n", "Y", "N",
			"1", "0", "on", "off",
		})
		ops := NewStringOps(s)

		boolSeries, err := ops.ToBoolean()
		require.NoError(t, err)
		assert.Equal(t, 20, boolSeries.Len())

		// True values
		trueIndices := []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}
		for _, i := range trueIndices {
			assert.Equal(t, true, boolSeries.Get(i), "Index %d should be true", i)
		}

		// False values
		falseIndices := []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19}
		for _, i := range falseIndices {
			assert.Equal(t, false, boolSeries.Get(i), "Index %d should be false", i)
		}
	})

	t.Run("Invalid boolean values", func(t *testing.T) {
		s := series.NewStringSeries("invalid", []string{"true", "maybe", "false", "unknown"})
		ops := NewStringOps(s)

		boolSeries, err := ops.ToBoolean()
		require.NoError(t, err)
		assert.Equal(t, 4, boolSeries.Len())
		assert.False(t, boolSeries.IsNull(0))
		assert.True(t, boolSeries.IsNull(1))
		assert.False(t, boolSeries.IsNull(2))
		assert.True(t, boolSeries.IsNull(3))
		assert.Equal(t, true, boolSeries.Get(0))
		assert.Equal(t, false, boolSeries.Get(2))
	})
}

func TestStringToDateTime(t *testing.T) {
	t.Run("Various datetime formats", func(t *testing.T) {
		s := series.NewStringSeries("dates", []string{
			"2024-01-15T10:30:00Z",
			"2024-01-15 10:30:00",
			"2024-01-15",
			"01/15/2024",
			"15-Jan-2024",
		})
		ops := NewStringOps(s)

		dtSeries, err := ops.ToDateTime()
		require.NoError(t, err)
		assert.Equal(t, 5, dtSeries.Len())
		assert.Equal(t, datatypes.Datetime{Unit: datatypes.Nanoseconds}, dtSeries.DataType())

		// All should parse successfully
		for i := 0; i < 5; i++ {
			assert.False(t, dtSeries.IsNull(i))
		}

		// Check first datetime
		ts := dtSeries.Get(0).(int64)
		dt := time.Unix(0, ts).UTC()
		assert.Equal(t, 2024, dt.Year())
		assert.Equal(t, time.January, dt.Month())
		assert.Equal(t, 15, dt.Day())
		assert.Equal(t, 10, dt.Hour())
		assert.Equal(t, 30, dt.Minute())
	})

	t.Run("With custom format", func(t *testing.T) {
		s := series.NewStringSeries("custom", []string{"15/01/2024", "20/02/2024", "25/03/2024"})
		ops := NewStringOps(s)

		dtSeries, err := ops.ToDateTime("%d/%m/%Y")
		require.NoError(t, err)
		assert.Equal(t, 3, dtSeries.Len())

		// Check dates
		for i, expected := range []struct{ day, month int }{
			{15, 1}, {20, 2}, {25, 3},
		} {
			ts := dtSeries.Get(i).(int64)
			dt := time.Unix(0, ts).UTC()
			assert.Equal(t, 2024, dt.Year())
			assert.Equal(t, expected.month, int(dt.Month()))
			assert.Equal(t, expected.day, dt.Day())
		}
	})

	t.Run("Invalid datetime values", func(t *testing.T) {
		s := series.NewStringSeries("invalid", []string{"2024-01-15", "not-a-date", "2024-13-45", ""})
		ops := NewStringOps(s)

		dtSeries, err := ops.ToDateTime()
		require.NoError(t, err)
		assert.Equal(t, 4, dtSeries.Len())
		assert.False(t, dtSeries.IsNull(0))
		assert.True(t, dtSeries.IsNull(1))
		assert.True(t, dtSeries.IsNull(2))
		assert.True(t, dtSeries.IsNull(3))
	})
}

func TestStringToDate(t *testing.T) {
	t.Run("Date parsing", func(t *testing.T) {
		s := series.NewStringSeries("dates", []string{"2024-01-15", "2024-02-20", "2024-03-25"})
		ops := NewStringOps(s)

		dateSeries, err := ops.ToDate()
		require.NoError(t, err)
		assert.Equal(t, 3, dateSeries.Len())
		assert.Equal(t, datatypes.Date{}, dateSeries.DataType())

		// Check dates (days since epoch)
		// 2024-01-15 is 19737 days since 1970-01-01
		assert.Equal(t, int32(19737), dateSeries.Get(0))
	})
}

func TestStringToTime(t *testing.T) {
	t.Run("Time parsing", func(t *testing.T) {
		s := series.NewStringSeries("times", []string{"10:30:45", "14:15:00", "23:59:59"})
		ops := NewStringOps(s)

		timeSeries, err := ops.ToTime()
		require.NoError(t, err)
		assert.Equal(t, 3, timeSeries.Len())
		assert.Equal(t, datatypes.Time{}, timeSeries.DataType())

		// Check times (nanoseconds since midnight)
		// 10:30:45 = 10*3600 + 30*60 + 45 = 37845 seconds = 37845000000000 nanoseconds
		assert.Equal(t, int64(37845000000000), timeSeries.Get(0))
		assert.Equal(t, int64(51300000000000), timeSeries.Get(1))
		assert.Equal(t, int64(86399000000000), timeSeries.Get(2))
	})

	t.Run("12-hour format", func(t *testing.T) {
		s := series.NewStringSeries("times", []string{"10:30:45 AM", "2:15 PM", "11:59:59 PM"})
		ops := NewStringOps(s)

		timeSeries, err := ops.ToTime()
		require.NoError(t, err)
		assert.Equal(t, 3, timeSeries.Len())

		// 2:15 PM = 14:15:00
		assert.Equal(t, int64(51300000000000), timeSeries.Get(1))
		// 11:59:59 PM = 23:59:59
		assert.Equal(t, int64(86399000000000), timeSeries.Get(2))
	})
}

func TestStringValidation(t *testing.T) {
	t.Run("IsNumeric", func(t *testing.T) {
		s := series.NewStringSeries("mixed", []string{"123", "123.45", "abc", "1e10", ""})
		ops := NewStringOps(s)

		isNum := ops.IsNumericStr()
		assert.Equal(t, 5, isNum.Len())
		assert.Equal(t, true, isNum.Get(0))
		assert.Equal(t, true, isNum.Get(1))
		assert.Equal(t, false, isNum.Get(2))
		assert.Equal(t, true, isNum.Get(3))
		assert.Equal(t, false, isNum.Get(4))
	})

	t.Run("IsAlpha", func(t *testing.T) {
		s := series.NewStringSeries("mixed", []string{"abc", "ABC", "abc123", "123", "", "hello world"})
		ops := NewStringOps(s)

		isAlpha := ops.IsAlphaStr()
		assert.Equal(t, 6, isAlpha.Len())
		assert.Equal(t, true, isAlpha.Get(0))
		assert.Equal(t, true, isAlpha.Get(1))
		assert.Equal(t, false, isAlpha.Get(2)) // has numbers
		assert.Equal(t, false, isAlpha.Get(3)) // only numbers
		assert.Equal(t, false, isAlpha.Get(4)) // empty
		assert.Equal(t, false, isAlpha.Get(5)) // has space
	})

	t.Run("IsAlphanumeric", func(t *testing.T) {
		s := series.NewStringSeries("mixed", []string{"abc123", "ABC", "hello world", "test_123", ""})
		ops := NewStringOps(s)

		isAlnum := ops.IsAlphanumericStr()
		assert.Equal(t, 5, isAlnum.Len())
		assert.Equal(t, true, isAlnum.Get(0))
		assert.Equal(t, true, isAlnum.Get(1))
		assert.Equal(t, false, isAlnum.Get(2)) // has space
		assert.Equal(t, false, isAlnum.Get(3)) // has underscore
		assert.Equal(t, false, isAlnum.Get(4)) // empty
	})
}
