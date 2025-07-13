package datetime

import (
	"fmt"
	"time"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// DateTimeSeries wraps a Series with DateTime operations
type DateTimeSeries struct {
	s series.Series
}

// NewDateTimeSeries creates a new DateTime series from time.Time values
func NewDateTimeSeries(name string, values []time.Time) series.Series {
	timestamps := make([]int64, len(values))
	for i, t := range values {
		timestamps[i] = t.UnixNano()
	}
	
	dt := datatypes.Datetime{
		Unit:     datatypes.Nanoseconds,
		TimeZone: time.UTC,
	}
	
	return series.NewSeries(name, timestamps, dt)
}

// NewDateTimeSeriesFromStrings creates a DateTime series from string values
func NewDateTimeSeriesFromStrings(name string, values []string, format string) (series.Series, error) {
	timestamps := make([]int64, len(values))
	validity := make([]bool, len(values))
	
	for i, v := range values {
		if v == "" {
			validity[i] = false
			continue
		}
		
		var dt DateTime
		var err error
		
		if format == "" {
			dt, err = ParseDateTime(v)
		} else {
			dt, err = ParseDateTimeWithFormat(v, format)
		}
		
		if err != nil {
			validity[i] = false
		} else {
			timestamps[i] = dt.timestamp
			validity[i] = true
		}
	}
	
	dataType := datatypes.Datetime{
		Unit:     datatypes.Nanoseconds,
		TimeZone: time.UTC,
	}
	
	return series.NewSeriesWithValidity(name, timestamps, validity, dataType), nil
}

// NewDateTimeSeriesFromEpoch creates a DateTime series from epoch values
func NewDateTimeSeriesFromEpoch(name string, values []int64, unit TimeUnit) series.Series {
	timestamps := make([]int64, len(values))
	
	for i, v := range values {
		dt := ParseDateTimeFromEpoch(v, unit)
		timestamps[i] = dt.timestamp
	}
	
	dataType := datatypes.Datetime{
		Unit:     datatypes.Nanoseconds,
		TimeZone: time.UTC,
	}
	
	return series.NewSeries(name, timestamps, dataType)
}

// NewDateSeries creates a new Date series from time.Time values
func NewDateSeries(name string, values []time.Time) series.Series {
	days := make([]int32, len(values))
	for i, t := range values {
		date := NewDateFromTime(t)
		days[i] = date.days
	}
	
	return series.NewSeries(name, days, datatypes.Date{})
}

// NewDateSeriesFromStrings creates a Date series from string values
func NewDateSeriesFromStrings(name string, values []string, format string) (series.Series, error) {
	days := make([]int32, len(values))
	validity := make([]bool, len(values))
	
	for i, v := range values {
		if v == "" {
			validity[i] = false
			continue
		}
		
		var date Date
		var err error
		
		if format == "" {
			date, err = ParseDate(v)
		} else {
			date, err = ParseDateWithFormat(v, format)
		}
		
		if err != nil {
			validity[i] = false
		} else {
			days[i] = date.days
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, days, validity, datatypes.Date{}), nil
}

// NewTimeSeries creates a new Time series
func NewTimeSeries(name string, hours, minutes, seconds []int) (series.Series, error) {
	if len(hours) != len(minutes) || len(hours) != len(seconds) {
		return nil, fmt.Errorf("hours, minutes, and seconds must have the same length")
	}
	
	nanoseconds := make([]int64, len(hours))
	for i := range hours {
		t := NewTime(hours[i], minutes[i], seconds[i], 0)
		nanoseconds[i] = t.nanoseconds
	}
	
	return series.NewSeries(name, nanoseconds, datatypes.Time{}), nil
}

// DtSeries returns DateTime operations for a Series
func DtSeries(s series.Series) (*DateTimeSeries, error) {
	switch dt := s.DataType().(type) {
	case datatypes.Datetime:
		return &DateTimeSeries{s: s}, nil
	case datatypes.Date:
		// Convert Date to DateTime for operations
		return &DateTimeSeries{s: s}, nil
	case datatypes.Time:
		// Time series can use some DateTime operations
		return &DateTimeSeries{s: s}, nil
	default:
		return nil, fmt.Errorf("series has non-temporal type %s", dt)
	}
}

// Year extracts the year component
func (dts *DateTimeSeries) Year() series.Series {
	name := fmt.Sprintf("%s.year", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Year())
		})
	case datatypes.Date:
		return extractComponentFromDate(dts.s, name, func(days int32) int32 {
			date := Date{days: days}
			return int32(date.Year())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// Month extracts the month component
func (dts *DateTimeSeries) Month() series.Series {
	name := fmt.Sprintf("%s.month", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Month())
		})
	case datatypes.Date:
		return extractComponentFromDate(dts.s, name, func(days int32) int32 {
			date := Date{days: days}
			return int32(date.Month())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// Day extracts the day component
func (dts *DateTimeSeries) Day() series.Series {
	name := fmt.Sprintf("%s.day", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Day())
		})
	case datatypes.Date:
		return extractComponentFromDate(dts.s, name, func(days int32) int32 {
			date := Date{days: days}
			return int32(date.Day())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// Hour extracts the hour component
func (dts *DateTimeSeries) Hour() series.Series {
	name := fmt.Sprintf("%s.hour", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Hour())
		})
	case datatypes.Time:
		return extractComponentFromTime(dts.s, name, func(ns int64) int32 {
			t := Time{nanoseconds: ns}
			return int32(t.Hour())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// Minute extracts the minute component
func (dts *DateTimeSeries) Minute() series.Series {
	name := fmt.Sprintf("%s.minute", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Minute())
		})
	case datatypes.Time:
		return extractComponentFromTime(dts.s, name, func(ns int64) int32 {
			t := Time{nanoseconds: ns}
			return int32(t.Minute())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// Second extracts the second component
func (dts *DateTimeSeries) Second() series.Series {
	name := fmt.Sprintf("%s.second", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Second())
		})
	case datatypes.Time:
		return extractComponentFromTime(dts.s, name, func(ns int64) int32 {
			t := Time{nanoseconds: ns}
			return int32(t.Second())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// DayOfWeek extracts the day of week (0=Sunday, 6=Saturday)
func (dts *DateTimeSeries) DayOfWeek() series.Series {
	name := fmt.Sprintf("%s.dayofweek", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.DayOfWeek())
		})
	case datatypes.Date:
		return extractComponentFromDate(dts.s, name, func(days int32) int32 {
			date := Date{days: days}
			return int32(date.Time().Weekday())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// DayOfYear extracts the day of year
func (dts *DateTimeSeries) DayOfYear() series.Series {
	name := fmt.Sprintf("%s.dayofyear", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.DayOfYear())
		})
	case datatypes.Date:
		return extractComponentFromDate(dts.s, name, func(days int32) int32 {
			date := Date{days: days}
			return int32(date.Time().YearDay())
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// Quarter extracts the quarter (1-4)
func (dts *DateTimeSeries) Quarter() series.Series {
	name := fmt.Sprintf("%s.quarter", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractComponent(dts.s, name, func(ts int64) int32 {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return int32(dt.Quarter())
		})
	case datatypes.Date:
		return extractComponentFromDate(dts.s, name, func(days int32) int32 {
			date := Date{days: days}
			month := date.Month()
			return int32((int(month)-1)/3 + 1)
		})
	default:
		return series.NewSeries(name, []int32{}, datatypes.Int32{})
	}
}

// IsLeapYear returns a boolean series indicating leap years
func (dts *DateTimeSeries) IsLeapYear() series.Series {
	name := fmt.Sprintf("%s.is_leap_year", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractBoolComponent(dts.s, name, func(ts int64) bool {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return dt.IsLeapYear()
		})
	case datatypes.Date:
		return extractBoolComponentFromDate(dts.s, name, func(days int32) bool {
			date := Date{days: days}
			year := date.Year()
			return year%4 == 0 && (year%100 != 0 || year%400 == 0)
		})
	default:
		return series.NewSeries(name, []bool{}, datatypes.Boolean{})
	}
}

// Format formats the DateTime values according to the given layout
func (dts *DateTimeSeries) Format(layout string) series.Series {
	name := fmt.Sprintf("%s.formatted", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractStringComponent(dts.s, name, func(ts int64) string {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			if layout == "" {
				return dt.ISOFormat()
			}
			return FormatWithPolarsStyle(dt, layout)
		})
	case datatypes.Date:
		return extractStringComponentFromDate(dts.s, name, func(days int32) string {
			date := Date{days: days}
			if layout == "" {
				return date.ISOFormat()
			}
			return date.Format(convertPolarsToGoFormat(layout))
		})
	default:
		return series.NewSeries(name, []string{}, datatypes.String{})
	}
}

// IsWeekend returns a boolean series indicating whether each datetime is a weekend
func (dts *DateTimeSeries) IsWeekend() series.Series {
	name := fmt.Sprintf("%s.is_weekend", dts.s.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return extractBoolComponent(dts.s, name, func(ts int64) bool {
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			return dt.IsWeekend()
		})
	case datatypes.Date:
		return extractBoolComponentFromDate(dts.s, name, func(days int32) bool {
			date := Date{days: days}
			return date.IsWeekend()
		})
	default:
		return series.NewSeries(name, []bool{}, datatypes.Boolean{})
	}
}

// Floor rounds down to the specified time unit
func (dts *DateTimeSeries) Floor(unit TimeUnit) series.Series {
	name := fmt.Sprintf("%s.floor_%s", dts.s.Name(), unit.String())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		length := dts.s.Len()
		values := make([]int64, length)
		validity := make([]bool, length)
		
		for i := 0; i < length; i++ {
			if dts.s.IsNull(i) {
				validity[i] = false
			} else {
				ts := dts.s.Get(i).(int64)
				dt := DateTime{timestamp: ts, timezone: time.UTC}
				floored := dt.Floor(unit)
				values[i] = floored.timestamp
				validity[i] = true
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, dts.s.DataType())
		
	case datatypes.Date:
		// For dates, only certain units make sense
		if unit < Day {
			// Can't floor to units smaller than day for Date type
			return dts.s
		}
		
		length := dts.s.Len()
		values := make([]int32, length)
		validity := make([]bool, length)
		
		for i := 0; i < length; i++ {
			if dts.s.IsNull(i) {
				validity[i] = false
			} else {
				days := dts.s.Get(i).(int32)
				date := Date{days: days}
				dt := DateTime{timestamp: date.Time().UnixNano(), timezone: time.UTC}
				floored := dt.Floor(unit)
				newDate := NewDateFromTime(floored.Time())
				values[i] = newDate.days
				validity[i] = true
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Date{})
		
	default:
		return series.NewSeries(name, []int64{}, dts.s.DataType())
	}
}

// Helper functions for extracting components

func extractComponent(s series.Series, name string, extractor func(int64) int32) series.Series {
	length := s.Len()
	values := make([]int32, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			ts := s.Get(i).(int64)
			values[i] = extractor(ts)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

func extractComponentFromDate(s series.Series, name string, extractor func(int32) int32) series.Series {
	length := s.Len()
	values := make([]int32, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			days := s.Get(i).(int32)
			values[i] = extractor(days)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

func extractComponentFromTime(s series.Series, name string, extractor func(int64) int32) series.Series {
	length := s.Len()
	values := make([]int32, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			ns := s.Get(i).(int64)
			values[i] = extractor(ns)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

func extractBoolComponent(s series.Series, name string, extractor func(int64) bool) series.Series {
	length := s.Len()
	values := make([]bool, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			ts := s.Get(i).(int64)
			values[i] = extractor(ts)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Boolean{})
}

func extractBoolComponentFromDate(s series.Series, name string, extractor func(int32) bool) series.Series {
	length := s.Len()
	values := make([]bool, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			days := s.Get(i).(int32)
			values[i] = extractor(days)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Boolean{})
}

func extractStringComponent(s series.Series, name string, extractor func(int64) string) series.Series {
	length := s.Len()
	values := make([]string, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			ts := s.Get(i).(int64)
			values[i] = extractor(ts)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.String{})
}

func extractStringComponentFromDate(s series.Series, name string, extractor func(int32) string) series.Series {
	length := s.Len()
	values := make([]string, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			days := s.Get(i).(int32)
			values[i] = extractor(days)
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.String{})
}