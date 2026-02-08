package datetime

import (
	_ "fmt"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"time"
)

// DateTimeSeries wraps a Series with DateTime operations
type DateTimeSeries struct {
	s series.Series
}

// NewDateTimeSeries creates a new DateTime series from time.Time values
func NewDateTimeSeries(name string, values []time.Time) series.Series {
	panic("not implemented")

}

// NewDateTimeSeriesFromStrings creates a DateTime series from string values
func NewDateTimeSeriesFromStrings(name string, values []string, format string) (series.Series, error) {
	panic("not implemented")

}

// NewDateTimeSeriesFromEpoch creates a DateTime series from epoch values
func NewDateTimeSeriesFromEpoch(name string, values []int64, unit TimeUnit) series.Series {
	panic("not implemented")

}

// NewDateSeries creates a new Date series from time.Time values
func NewDateSeries(name string, values []time.Time) series.Series {
	panic("not implemented")

}

// NewDateSeriesFromStrings creates a Date series from string values
func NewDateSeriesFromStrings(name string, values []string, format string) (series.Series, error) {
	panic("not implemented")

}

// NewTimeSeries creates a new Time series
func NewTimeSeries(name string, hours, minutes, seconds []int) (series.Series, error) {
	panic("not implemented")

}

// DtSeries returns DateTime operations for a Series
func DtSeries(s series.Series) (*DateTimeSeries, error) {
	panic("not implemented")

	// Convert Date to DateTime for operations

	// Time series can use some DateTime operations

}

// Year extracts the year component
func (dts *DateTimeSeries) Year() series.Series {
	panic("not implemented")

}

// Month extracts the month component
func (dts *DateTimeSeries) Month() series.Series {
	panic("not implemented")

}

// Day extracts the day component
func (dts *DateTimeSeries) Day() series.Series {
	panic("not implemented")

}

// Hour extracts the hour component
func (dts *DateTimeSeries) Hour() series.Series {
	panic("not implemented")

}

// Minute extracts the minute component
func (dts *DateTimeSeries) Minute() series.Series {
	panic("not implemented")

}

// Second extracts the second component
func (dts *DateTimeSeries) Second() series.Series {
	panic("not implemented")

}

// DayOfWeek extracts the day of week (0=Sunday, 6=Saturday)
func (dts *DateTimeSeries) DayOfWeek() series.Series {
	panic("not implemented")

}

// DayOfYear extracts the day of year
func (dts *DateTimeSeries) DayOfYear() series.Series {
	panic("not implemented")

}

// Quarter extracts the quarter (1-4)
func (dts *DateTimeSeries) Quarter() series.Series {
	panic("not implemented")

}

// IsLeapYear returns a boolean series indicating leap years
func (dts *DateTimeSeries) IsLeapYear() series.Series {
	panic("not implemented")

}

// Format formats the DateTime values according to the given layout
func (dts *DateTimeSeries) Format(layout string) series.Series {
	panic("not implemented")

}

// IsWeekend returns a boolean series indicating whether each datetime is a weekend
func (dts *DateTimeSeries) IsWeekend() series.Series {
	panic("not implemented")

}

// Floor rounds down to the specified time unit
func (dts *DateTimeSeries) Floor(unit TimeUnit) series.Series {
	panic("not implemented")

	// For dates, only certain units make sense

	// Can't floor to units smaller than day for Date type

}

// Helper functions for extracting components

func extractComponent(s series.Series, name string, extractor func(int64) int32) series.Series {
	panic("not implemented")

}

func extractComponentFromDate(s series.Series, name string, extractor func(int32) int32) series.Series {
	panic("not implemented")

}

func extractComponentFromTime(s series.Series, name string, extractor func(int64) int32) series.Series {
	panic("not implemented")

}

func extractBoolComponent(s series.Series, name string, extractor func(int64) bool) series.Series {
	panic("not implemented")

}

func extractBoolComponentFromDate(s series.Series, name string, extractor func(int32) bool) series.Series {
	panic("not implemented")

}

func extractStringComponent(s series.Series, name string, extractor func(int64) string) series.Series {
	panic("not implemented")

}

func extractStringComponentFromDate(s series.Series, name string, extractor func(int32) string) series.Series {
	panic("not implemented")

}
