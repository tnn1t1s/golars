package datetime

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// TimeUnit represents the precision of temporal data
type TimeUnit int

const (
	Nanosecond TimeUnit = iota
	Microsecond
	Millisecond
	Second
	Minute
	Hour
	Day
	Week
	Month
	Quarter
	Year
)

// String returns the string representation of TimeUnit
func (tu TimeUnit) String() string {
	switch tu {
	case Nanosecond:
		return "ns"
	case Microsecond:
		return "us"
	case Millisecond:
		return "ms"
	case Second:
		return "s"
	case Minute:
		return "min"
	case Hour:
		return "h"
	case Day:
		return "D"
	case Week:
		return "W"
	case Month:
		return "M"
	case Quarter:
		return "Q"
	case Year:
		return "Y"
	default:
		return "unknown"
	}
}

// ToArrowTimeUnit converts TimeUnit to Arrow's TimeUnit
func (tu TimeUnit) ToArrowTimeUnit() arrow.TimeUnit {
	switch tu {
	case Nanosecond:
		return arrow.Nanosecond
	case Microsecond:
		return arrow.Microsecond
	case Millisecond:
		return arrow.Millisecond
	case Second:
		return arrow.Second
	default:
		return arrow.Nanosecond
	}
}

// Duration represents a time duration with support for calendar units
type Duration struct {
	months      int32
	days        int32
	nanoseconds int64
}

// NewDuration creates a new duration
func NewDuration(months, days int32, nanoseconds int64) Duration {
	return Duration{
		months:      months,
		days:        days,
		nanoseconds: nanoseconds,
	}
}

// Days creates a duration of n days
func Days(n int) Duration {
	return Duration{days: int32(n)}
}

// Hours creates a duration of n hours
func Hours(n int) Duration {
	return Duration{nanoseconds: int64(n) * int64(time.Hour)}
}

// Minutes creates a duration of n minutes
func Minutes(n int) Duration {
	return Duration{nanoseconds: int64(n) * int64(time.Minute)}
}

// Seconds creates a duration of n seconds
func Seconds(n int) Duration {
	return Duration{nanoseconds: int64(n) * int64(time.Second)}
}

// Milliseconds creates a duration of n milliseconds
func Milliseconds(n int) Duration {
	return Duration{nanoseconds: int64(n) * int64(time.Millisecond)}
}

// Microseconds creates a duration of n microseconds
func Microseconds(n int) Duration {
	return Duration{nanoseconds: int64(n) * int64(time.Microsecond)}
}

// Nanoseconds creates a duration of n nanoseconds
func Nanoseconds(n int64) Duration {
	return Duration{nanoseconds: n}
}

// Weeks creates a duration of n weeks
func Weeks(n int) Duration {
	return Duration{days: int32(n * 7)}
}

// Months creates a duration of n months
func Months(n int) Duration {
	return Duration{months: int32(n)}
}

// Years creates a duration of n years
func Years(n int) Duration {
	return Duration{months: int32(n * 12)}
}

// Add adds two durations
func (d Duration) Add(other Duration) Duration {
	return Duration{
		months:      d.months + other.months,
		days:        d.days + other.days,
		nanoseconds: d.nanoseconds + other.nanoseconds,
	}
}

// Negate returns the negative of the duration
func (d Duration) Negate() Duration {
	return Duration{
		months:      -d.months,
		days:        -d.days,
		nanoseconds: -d.nanoseconds,
	}
}

// IsZero returns true if the duration is zero
func (d Duration) IsZero() bool {
	return d.months == 0 && d.days == 0 && d.nanoseconds == 0
}

// DateTime represents a point in time with timezone information
type DateTime struct {
	timestamp int64          // nanoseconds since Unix epoch
	timezone  *time.Location // nil means UTC
}

// NewDateTime creates a new DateTime
func NewDateTime(t time.Time) DateTime {
	return DateTime{
		timestamp: t.UnixNano(),
		timezone:  t.Location(),
	}
}

// Time returns the Go time.Time representation
func (dt DateTime) Time() time.Time {
	t := time.Unix(0, dt.timestamp)
	if dt.timezone != nil {
		t = t.In(dt.timezone)
	}
	return t
}

// ToTimezone converts the DateTime to a different timezone (different instant)
func (dt DateTime) ToTimezone(tz *time.Location) DateTime {
	t := dt.Time()
	converted := t.In(tz)
	return NewDateTime(converted)
}

// Add adds a duration to the DateTime
func (dt DateTime) Add(d Duration) DateTime {
	t := dt.Time()

	// Add months first
	if d.months != 0 {
		t = t.AddDate(0, int(d.months), 0)
	}

	// Then add days
	if d.days != 0 {
		t = t.AddDate(0, 0, int(d.days))
	}

	// Finally add nanoseconds
	if d.nanoseconds != 0 {
		t = t.Add(time.Duration(d.nanoseconds))
	}

	return NewDateTime(t)
}

// Sub subtracts a duration from the DateTime
func (dt DateTime) Sub(d Duration) DateTime {
	return dt.Add(d.Negate())
}

// Date represents a calendar date without time
type Date struct {
	days int32 // days since Unix epoch (1970-01-01)
}

// NewDate creates a new Date
func NewDate(year int, month time.Month, day int) Date {
	t := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int32(t.Sub(epoch).Hours() / 24)
	return Date{days: days}
}

// NewDateFromTime creates a Date from a time.Time
func NewDateFromTime(t time.Time) Date {
	year, month, day := t.Date()
	return NewDate(year, month, day)
}

// Time returns the time.Time representation at midnight UTC
func (d Date) Time() time.Time {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	return epoch.AddDate(0, 0, int(d.days))
}

// Year returns the year
func (d Date) Year() int {
	return d.Time().Year()
}

// Month returns the month
func (d Date) Month() time.Month {
	return d.Time().Month()
}

// Day returns the day of month
func (d Date) Day() int {
	return d.Time().Day()
}

// Add adds days to the date
func (d Date) Add(days int) Date {
	return Date{days: d.days + int32(days)}
}

// Time represents time of day without date
type Time struct {
	nanoseconds int64 // nanoseconds since midnight
}

// NewTime creates a new Time
func NewTime(hour, minute, second, nanosecond int) Time {
	ns := int64(hour)*int64(time.Hour) +
		int64(minute)*int64(time.Minute) +
		int64(second)*int64(time.Second) +
		int64(nanosecond)
	return Time{nanoseconds: ns}
}

// Hour returns the hour component
func (t Time) Hour() int {
	return int(t.nanoseconds / int64(time.Hour))
}

// Minute returns the minute component
func (t Time) Minute() int {
	remaining := t.nanoseconds % int64(time.Hour)
	return int(remaining / int64(time.Minute))
}

// Second returns the second component
func (t Time) Second() int {
	remaining := t.nanoseconds % int64(time.Minute)
	return int(remaining / int64(time.Second))
}

// Nanosecond returns the nanosecond component
func (t Time) Nanosecond() int {
	return int(t.nanoseconds % int64(time.Second))
}
