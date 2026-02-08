package datetime

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var tzCache = &sync.Map{}

func getTimezone(name string) (*time.Location, error) {
	if cached, ok := tzCache.Load(name); ok {
		return cached.(*time.Location), nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	tzCache.Store(name, loc)
	return loc, nil
}

var commonFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05.999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05.999999",
	"2006-01-02T15:04:05.999",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"2006/01/02",
	"01/02/2006",
	"02-Jan-2006",
	"02-Jan-2006 15:04:05",
	"Jan 02, 2006",
	"January 02, 2006",
	"2006-01-02 15:04:05 MST",
	"2006-01-02 15:04:05 -0700",
}

func ParseDateTime(value string) (DateTime, error) {
	if value == "" {
		return DateTime{}, fmt.Errorf("empty string")
	}
	for _, format := range commonFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			return NewDateTime(t), nil
		}
	}
	return DateTime{}, fmt.Errorf("unable to parse datetime: %q", value)
}

func ParseDateTimeWithFormat(value string, format string) (DateTime, error) {
	if value == "" {
		return DateTime{}, fmt.Errorf("empty string")
	}
	goFormat := convertToGoTimeFormat(format)
	t, err := time.Parse(goFormat, value)
	if err != nil {
		return DateTime{}, fmt.Errorf("unable to parse %q with format %q: %w", value, format, err)
	}
	return NewDateTime(t), nil
}

func ParseDateTimeWithTimezone(value string, tz *time.Location) (DateTime, error) {
	dt, err := ParseDateTime(value)
	if err != nil {
		return DateTime{}, err
	}
	// Re-interpret the parsed time in the given timezone
	t := dt.Time()
	localized := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), tz)
	return NewDateTime(localized), nil
}

func ParseDate(value string) (Date, error) {
	if value == "" {
		return Date{}, fmt.Errorf("empty string")
	}
	dt, err := ParseDateTime(value)
	if err != nil {
		return Date{}, err
	}
	t := dt.Time()
	return NewDate(t.Year(), t.Month(), t.Day()), nil
}

func ParseDateWithFormat(value string, format string) (Date, error) {
	if value == "" {
		return Date{}, fmt.Errorf("empty string")
	}
	dt, err := ParseDateTimeWithFormat(value, format)
	if err != nil {
		return Date{}, err
	}
	t := dt.Time()
	return NewDate(t.Year(), t.Month(), t.Day()), nil
}

func ParseTime(value string) (Time, error) {
	if value == "" {
		return Time{}, fmt.Errorf("empty string")
	}

	// Try various time formats
	timeFormats := []string{
		"15:04:05.999999999",
		"15:04:05.999999",
		"15:04:05.999",
		"15:04:05",
		"15:04",
		"3:04:05 PM",
		"3:04:05 AM",
		"03:04:05 PM",
		"03:04:05 AM",
	}

	for _, format := range timeFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			return NewTime(t.Hour(), t.Minute(), t.Second(), t.Nanosecond()), nil
		}
	}

	return Time{}, fmt.Errorf("unable to parse time: %q", value)
}

func ParseTimeWithFormat(value string, format string) (Time, error) {
	if value == "" {
		return Time{}, fmt.Errorf("empty string")
	}
	goFormat := convertToGoTimeFormat(format)
	t, err := time.Parse(goFormat, value)
	if err != nil {
		return Time{}, fmt.Errorf("unable to parse time %q with format %q: %w", value, format, err)
	}
	return NewTime(t.Hour(), t.Minute(), t.Second(), t.Nanosecond()), nil
}

func ParseDateTimeFromEpoch(epoch int64, unit TimeUnit) DateTime {
	var t time.Time
	switch unit {
	case Second:
		t = time.Unix(epoch, 0).UTC()
	case Millisecond:
		t = time.Unix(0, epoch*int64(time.Millisecond)).UTC()
	case Microsecond:
		t = time.Unix(0, epoch*int64(time.Microsecond)).UTC()
	case Nanosecond:
		t = time.Unix(0, epoch).UTC()
	default:
		t = time.Unix(epoch, 0).UTC()
	}
	return NewDateTime(t)
}

func convertToGoTimeFormat(format string) string {
	return convertPolarsToGoFormat(format)
}

func ParseDuration(s string) (Duration, error) {
	if s == "" {
		return Duration{}, fmt.Errorf("empty string")
	}
	return parsePeriodDuration(s)
}

func parsePeriodDuration(s string) (Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Duration{}, fmt.Errorf("empty duration string")
	}

	// Split on spaces for combined durations like "1y 2mo 3d"
	parts := strings.Fields(s)
	var total Duration

	for _, part := range parts {
		d, err := parseSingleDuration(part)
		if err != nil {
			return Duration{}, err
		}
		total = total.Add(d)
	}
	return total, nil
}

func parseSingleDuration(s string) (Duration, error) {
	// Find where the number ends and unit begins
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '-') {
		i++
	}

	numStr := s[:i]
	unitStr := s[i:]

	var n int64
	var err error
	if numStr == "" {
		n = 1
	} else {
		n, err = strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return Duration{}, fmt.Errorf("invalid duration number: %q", numStr)
		}
	}

	switch unitStr {
	case "ns":
		return Nanoseconds(n), nil
	case "us", "µs":
		return Microseconds(int(n)), nil
	case "ms":
		return Milliseconds(int(n)), nil
	case "s":
		return Seconds(int(n)), nil
	case "m", "min":
		return Minutes(int(n)), nil
	case "h":
		return Hours(int(n)), nil
	case "d":
		return Days(int(n)), nil
	case "w":
		return Weeks(int(n)), nil
	case "mo":
		return Months(int(n)), nil
	case "y":
		return Years(int(n)), nil
	default:
		return Duration{}, fmt.Errorf("unknown duration unit: %q", unitStr)
	}
}
