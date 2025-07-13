package datetime

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/expr"
	"github.com/stretchr/testify/assert"
)

func TestDateTimeExpr(t *testing.T) {
	col := expr.Col("timestamp")
	dtExpr := DtExpr(col)

	t.Run("Component extraction", func(t *testing.T) {
		yearExpr := dtExpr.Year()
		assert.Equal(t, "col(timestamp).dt.year()", yearExpr.String())
		assert.Equal(t, datatypes.Int32{}, yearExpr.DataType())
		assert.Equal(t, "timestamp_year", yearExpr.Name())

		monthExpr := dtExpr.Month()
		assert.Equal(t, "col(timestamp).dt.month()", monthExpr.String())
		assert.Equal(t, datatypes.Int32{}, monthExpr.DataType())

		dayExpr := dtExpr.Day()
		assert.Equal(t, "col(timestamp).dt.day()", dayExpr.String())
		assert.Equal(t, datatypes.Int32{}, dayExpr.DataType())

		hourExpr := dtExpr.Hour()
		assert.Equal(t, "col(timestamp).dt.hour()", hourExpr.String())
		assert.Equal(t, datatypes.Int32{}, hourExpr.DataType())

		minuteExpr := dtExpr.Minute()
		assert.Equal(t, "col(timestamp).dt.minute()", minuteExpr.String())
		assert.Equal(t, datatypes.Int32{}, minuteExpr.DataType())

		secondExpr := dtExpr.Second()
		assert.Equal(t, "col(timestamp).dt.second()", secondExpr.String())
		assert.Equal(t, datatypes.Int32{}, secondExpr.DataType())

		nanosecondExpr := dtExpr.Nanosecond()
		assert.Equal(t, "col(timestamp).dt.nanosecond()", nanosecondExpr.String())
		assert.Equal(t, datatypes.Int64{}, nanosecondExpr.DataType())
	})

	t.Run("Additional components", func(t *testing.T) {
		dowExpr := dtExpr.DayOfWeek()
		assert.Equal(t, "col(timestamp).dt.dayofweek()", dowExpr.String())
		assert.Equal(t, datatypes.Int32{}, dowExpr.DataType())

		doyExpr := dtExpr.DayOfYear()
		assert.Equal(t, "col(timestamp).dt.dayofyear()", doyExpr.String())
		assert.Equal(t, datatypes.Int32{}, doyExpr.DataType())

		quarterExpr := dtExpr.Quarter()
		assert.Equal(t, "col(timestamp).dt.quarter()", quarterExpr.String())
		assert.Equal(t, datatypes.Int32{}, quarterExpr.DataType())

		weekExpr := dtExpr.WeekOfYear()
		assert.Equal(t, "col(timestamp).dt.weekofyear()", weekExpr.String())
		assert.Equal(t, datatypes.Int32{}, weekExpr.DataType())
	})

	t.Run("Formatting", func(t *testing.T) {
		formatExpr := dtExpr.Format("%Y-%m-%d")
		assert.Equal(t, `col(timestamp).dt.format("%Y-%m-%d")`, formatExpr.String())
		assert.Equal(t, datatypes.String{}, formatExpr.DataType())
		assert.Equal(t, "timestamp_formatted", formatExpr.Name())
	})

	t.Run("Rounding operations", func(t *testing.T) {
		floorExpr := dtExpr.Floor(Day)
		assert.Equal(t, "col(timestamp).dt.floor(D)", floorExpr.String())

		ceilExpr := dtExpr.Ceil(Hour)
		assert.Equal(t, "col(timestamp).dt.ceil(h)", ceilExpr.String())

		roundExpr := dtExpr.Round(Minute)
		assert.Equal(t, "col(timestamp).dt.round(min)", roundExpr.String())

		truncateExpr := dtExpr.Truncate(Second)
		assert.Equal(t, "col(timestamp).dt.floor(s)", truncateExpr.String())
	})

	t.Run("Aliasing", func(t *testing.T) {
		yearExpr := dtExpr.Year().Alias("year_column")
		assert.Equal(t, "year_column", yearExpr.Name())
		assert.Equal(t, "year_column", yearExpr.String())
	})
}

func TestStringToDateTimeExpr(t *testing.T) {
	col := expr.Col("date_string")

	t.Run("StrToDateTime", func(t *testing.T) {
		dtExpr := StrToDateTime(col, "%Y-%m-%d %H:%M:%S")
		assert.Equal(t, `str_to_datetime(col(date_string), "%Y-%m-%d %H:%M:%S")`, dtExpr.String())
		assert.Equal(t, datatypes.Datetime{Unit: datatypes.Nanoseconds}, dtExpr.DataType())
		assert.Equal(t, "date_string_datetime", dtExpr.Name())

		// Without format
		dtExpr2 := StrToDateTime(col, "")
		assert.Equal(t, "str_to_datetime(col(date_string))", dtExpr2.String())
	})

	t.Run("StrToDate", func(t *testing.T) {
		dateExpr := StrToDate(col, "%Y-%m-%d")
		assert.Equal(t, `str_to_date(col(date_string), "%Y-%m-%d")`, dateExpr.String())
		assert.Equal(t, datatypes.Date{}, dateExpr.DataType())
		assert.Equal(t, "date_string_date", dateExpr.Name())
	})

	t.Run("StrToTime", func(t *testing.T) {
		timeExpr := StrToTime(col, "%H:%M:%S")
		assert.Equal(t, `str_to_time(col(date_string), "%H:%M:%S")`, timeExpr.String())
		assert.Equal(t, datatypes.Time{}, timeExpr.DataType())
		assert.Equal(t, "date_string_time", timeExpr.Name())
	})
}

func TestDateTimeExprIntegration(t *testing.T) {
	// Test chaining operations
	col := expr.Col("timestamp")
	
	// Extract year and alias it
	yearExpr := DtExpr(col).Year().Alias("year")
	assert.Equal(t, "year", yearExpr.Name())
	assert.Equal(t, datatypes.Int32{}, yearExpr.DataType())

	// Format and alias
	formattedExpr := DtExpr(col).Format("%Y-%m-%d").Alias("date_only")
	assert.Equal(t, "date_only", formattedExpr.Name())
	assert.Equal(t, datatypes.String{}, formattedExpr.DataType())

	// Round to day
	dayStartExpr := DtExpr(col).Floor(Day).Alias("day_start")
	assert.Equal(t, "day_start", dayStartExpr.Name())
}

func TestExprIsColumn(t *testing.T) {
	col := expr.Col("timestamp")
	
	// All datetime expressions should return false for IsColumn()
	assert.False(t, DtExpr(col).Year().IsColumn())
	assert.False(t, DtExpr(col).Format("%Y-%m-%d").IsColumn())
	assert.False(t, DtExpr(col).Floor(Day).IsColumn())
	assert.False(t, StrToDateTime(col, "").IsColumn())
}