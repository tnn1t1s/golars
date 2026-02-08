package datetime

import (
	"fmt"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"time"
)

// DateTimeSeries wraps a Series with DateTime operations
type DateTimeSeries struct {
	s series.Series
}

// NewDateTimeSeries creates a new DateTime series from time.Time values
func NewDateTimeSeries(name string, values []time.Time) series.Series {
	timestamps := make([]int64, len(values))
	for i, v := range values {
		timestamps[i] = v.UnixNano()
	}
	return series.NewSeries(name, timestamps, datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: time.UTC})
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
		if format != "" {
			dt, err = ParseDateTimeWithFormat(v, format)
		} else {
			dt, err = ParseDateTime(v)
		}
		if err != nil {
			validity[i] = false
			continue
		}
		timestamps[i] = dt.timestamp
		validity[i] = true
	}

	return series.NewSeriesWithValidity(name, timestamps, validity, datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: time.UTC}), nil
}

// NewDateTimeSeriesFromEpoch creates a DateTime series from epoch values
func NewDateTimeSeriesFromEpoch(name string, values []int64, unit TimeUnit) series.Series {
	timestamps := make([]int64, len(values))
	for i, v := range values {
		dt := ParseDateTimeFromEpoch(v, unit)
		timestamps[i] = dt.timestamp
	}
	return series.NewSeries(name, timestamps, datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: time.UTC})
}

// NewDateSeries creates a new Date series from time.Time values
func NewDateSeries(name string, values []time.Time) series.Series {
	days := make([]int32, len(values))
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, v := range values {
		d := int32(v.Sub(epoch).Hours() / 24)
		days[i] = d
	}
	return series.NewSeries(name, days, datatypes.Date{})
}

// NewDateSeriesFromStrings creates a Date series from string values
func NewDateSeriesFromStrings(name string, values []string, format string) (series.Series, error) {
	days := make([]int32, len(values))
	validity := make([]bool, len(values))
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	for i, v := range values {
		if v == "" {
			validity[i] = false
			continue
		}
		var d Date
		var err error
		if format != "" {
			d, err = ParseDateWithFormat(v, format)
		} else {
			d, err = ParseDate(v)
		}
		if err != nil {
			validity[i] = false
			continue
		}
		t := d.Time()
		days[i] = int32(t.Sub(epoch).Hours() / 24)
		validity[i] = true
	}

	return series.NewSeriesWithValidity(name, days, validity, datatypes.Date{}), nil
}

// NewTimeSeries creates a new Time series
func NewTimeSeries(name string, hours, minutes, seconds []int) (series.Series, error) {
	if len(hours) != len(minutes) || len(hours) != len(seconds) {
		return nil, fmt.Errorf("hours, minutes, and seconds must have the same length")
	}
	nanos := make([]int64, len(hours))
	for i := range hours {
		t := NewTime(hours[i], minutes[i], seconds[i], 0)
		nanos[i] = t.nanoseconds
	}
	return series.NewSeries(name, nanos, datatypes.Time{}), nil
}

// DtSeries returns DateTime operations for a Series
func DtSeries(s series.Series) (*DateTimeSeries, error) {
	dt := s.DataType()
	switch dt.(type) {
	case datatypes.Datetime:
		return &DateTimeSeries{s: s}, nil
	case datatypes.Date:
		return &DateTimeSeries{s: s}, nil
	case datatypes.Time:
		return &DateTimeSeries{s: s}, nil
	default:
		return nil, fmt.Errorf("series %q has type %s, not a temporal type", s.Name(), dt.String())
	}
}

// Year extracts the year component
func (dts *DateTimeSeries) Year() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractComponentFromDate(dts.s, dts.s.Name()+".year", func(days int32) int32 {
			d := Date{days: days}
			return int32(d.Year())
		})
	case datatypes.Time:
		return series.NewSeries(dts.s.Name()+".year", make([]int32, dts.s.Len()), datatypes.Int32{})
	default:
		return extractComponent(dts.s, dts.s.Name()+".year", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32(t.Year())
		})
	}
}

// Month extracts the month component
func (dts *DateTimeSeries) Month() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractComponentFromDate(dts.s, dts.s.Name()+".month", func(days int32) int32 {
			d := Date{days: days}
			return int32(d.Month())
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".month", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32(t.Month())
		})
	}
}

// Day extracts the day component
func (dts *DateTimeSeries) Day() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractComponentFromDate(dts.s, dts.s.Name()+".day", func(days int32) int32 {
			d := Date{days: days}
			return int32(d.Day())
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".day", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32(t.Day())
		})
	}
}

// Hour extracts the hour component
func (dts *DateTimeSeries) Hour() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Time:
		return extractComponentFromTime(dts.s, dts.s.Name()+".hour", func(ns int64) int32 {
			t := Time{nanoseconds: ns}
			return int32(t.Hour())
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".hour", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32(t.Hour())
		})
	}
}

// Minute extracts the minute component
func (dts *DateTimeSeries) Minute() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Time:
		return extractComponentFromTime(dts.s, dts.s.Name()+".minute", func(ns int64) int32 {
			t := Time{nanoseconds: ns}
			return int32(t.Minute())
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".minute", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32(t.Minute())
		})
	}
}

// Second extracts the second component
func (dts *DateTimeSeries) Second() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Time:
		return extractComponentFromTime(dts.s, dts.s.Name()+".second", func(ns int64) int32 {
			t := Time{nanoseconds: ns}
			return int32(t.Second())
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".second", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32(t.Second())
		})
	}
}

// DayOfWeek extracts the day of week (Monday=1, ..., Sunday=7)
func (dts *DateTimeSeries) DayOfWeek() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractComponentFromDate(dts.s, dts.s.Name()+".dayofweek", func(days int32) int32 {
			d := Date{days: days}
			wd := d.Time().Weekday()
			if wd == time.Sunday {
				return 7
			}
			return int32(wd)
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".dayofweek", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			wd := t.Weekday()
			if wd == time.Sunday {
				return 7
			}
			return int32(wd)
		})
	}
}

// DayOfYear extracts the day of year
func (dts *DateTimeSeries) DayOfYear() series.Series {
	return extractComponent(dts.s, dts.s.Name()+".dayofyear", func(ts int64) int32 {
		t := time.Unix(0, ts).UTC()
		return int32(t.YearDay())
	})
}

// Quarter extracts the quarter (1-4)
func (dts *DateTimeSeries) Quarter() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractComponentFromDate(dts.s, dts.s.Name()+".quarter", func(days int32) int32 {
			d := Date{days: days}
			return int32((int(d.Month()) - 1) / 3 + 1)
		})
	default:
		return extractComponent(dts.s, dts.s.Name()+".quarter", func(ts int64) int32 {
			t := time.Unix(0, ts).UTC()
			return int32((int(t.Month()) - 1) / 3 + 1)
		})
	}
}

// IsLeapYear returns a boolean series indicating leap years
func (dts *DateTimeSeries) IsLeapYear() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractBoolComponentFromDate(dts.s, dts.s.Name()+".is_leap_year", func(days int32) bool {
			d := Date{days: days}
			y := d.Year()
			return y%4 == 0 && (y%100 != 0 || y%400 == 0)
		})
	default:
		return extractBoolComponent(dts.s, dts.s.Name()+".is_leap_year", func(ts int64) bool {
			t := time.Unix(0, ts).UTC()
			y := t.Year()
			return y%4 == 0 && (y%100 != 0 || y%400 == 0)
		})
	}
}

// Format formats the DateTime values according to the given layout
func (dts *DateTimeSeries) Format(layout string) series.Series {
	goLayout := layout
	if goLayout == "" {
		goLayout = time.RFC3339Nano
	} else {
		goLayout = convertPolarsToGoFormat(goLayout)
	}

	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractStringComponentFromDate(dts.s, dts.s.Name()+".formatted", func(days int32) string {
			d := Date{days: days}
			return d.Time().Format(goLayout)
		})
	default:
		return extractStringComponent(dts.s, dts.s.Name()+".formatted", func(ts int64) string {
			t := time.Unix(0, ts).UTC()
			return t.Format(goLayout)
		})
	}
}

// IsWeekend returns a boolean series indicating whether each datetime is a weekend
func (dts *DateTimeSeries) IsWeekend() series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return extractBoolComponentFromDate(dts.s, dts.s.Name()+".is_weekend", func(days int32) bool {
			d := Date{days: days}
			wd := d.Time().Weekday()
			return wd == time.Saturday || wd == time.Sunday
		})
	default:
		return extractBoolComponent(dts.s, dts.s.Name()+".is_weekend", func(ts int64) bool {
			t := time.Unix(0, ts).UTC()
			wd := t.Weekday()
			return wd == time.Saturday || wd == time.Sunday
		})
	}
}

// Floor rounds down to the specified time unit
func (dts *DateTimeSeries) Floor(unit TimeUnit) series.Series {
	name := fmt.Sprintf("%s.floor_%s", dts.s.Name(), unit.String())
	n := dts.s.Len()

	switch dts.s.DataType().(type) {
	case datatypes.Date:
		values := make([]int32, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if dts.s.IsNull(i) {
				validity[i] = false
				continue
			}
			days := dts.s.Get(i).(int32)
			d := Date{days: days}
			dt := NewDateTime(d.Time())
			floored := dt.Floor(unit)
			// Convert back to days
			epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			values[i] = int32(floored.Time().Sub(epoch).Hours() / 24)
			validity[i] = true
		}
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Date{})
	default:
		values := make([]int64, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if dts.s.IsNull(i) {
				validity[i] = false
				continue
			}
			ts := dts.s.Get(i).(int64)
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			floored := dt.Floor(unit)
			values[i] = floored.timestamp
			validity[i] = true
		}
		return series.NewSeriesWithValidity(name, values, validity, dts.s.DataType())
	}
}

// Helper functions for extracting components

func extractComponent(s series.Series, name string, extractor func(int64) int32) series.Series {
	n := s.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		ts := s.Get(i).(int64)
		values[i] = extractor(ts)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

func extractComponentFromDate(s series.Series, name string, extractor func(int32) int32) series.Series {
	n := s.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		days := s.Get(i).(int32)
		values[i] = extractor(days)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

func extractComponentFromTime(s series.Series, name string, extractor func(int64) int32) series.Series {
	n := s.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		ns := s.Get(i).(int64)
		values[i] = extractor(ns)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

func extractBoolComponent(s series.Series, name string, extractor func(int64) bool) series.Series {
	n := s.Len()
	values := make([]bool, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		ts := s.Get(i).(int64)
		values[i] = extractor(ts)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Boolean{})
}

func extractBoolComponentFromDate(s series.Series, name string, extractor func(int32) bool) series.Series {
	n := s.Len()
	values := make([]bool, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		days := s.Get(i).(int32)
		values[i] = extractor(days)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Boolean{})
}

func extractStringComponent(s series.Series, name string, extractor func(int64) string) series.Series {
	n := s.Len()
	values := make([]string, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		ts := s.Get(i).(int64)
		values[i] = extractor(ts)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.String{})
}

func extractStringComponentFromDate(s series.Series, name string, extractor func(int32) string) series.Series {
	n := s.Len()
	values := make([]string, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		days := s.Get(i).(int32)
		values[i] = extractor(days)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.String{})
}
