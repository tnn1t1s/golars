package datetime

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
)

var tzCache = &sync.Map{}

func getTimezone(name string) (*time.Location, error) {
	if cached, ok := tzCache.Load(name); ok {
		return cached.(*time.Location), nil
	}

	tz, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %q: %w", name, err)
	}

	tzCache.Store(name, tz)
	return tz, nil
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
	value = strings.TrimSpace(value)
	if value == "" {
		return DateTime{}, fmt.Errorf("empty datetime string")
	}

	for _, format := range commonFormats {
		if t, err := time.Parse(format, value); err == nil {
			return NewDateTime(t), nil
		}
	}

	t, err := dateparse.ParseAny(value)
	if err != nil {
		return DateTime{}, fmt.Errorf("unable to parse datetime %q: %w", value, err)
	}

	return NewDateTime(t), nil
}

func ParseDateTimeWithFormat(value string, format string) (DateTime, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return DateTime{}, fmt.Errorf("empty datetime string")
	}

	goFormat := convertToGoTimeFormat(format)
	t, err := time.Parse(goFormat, value)
	if err != nil {
		return DateTime{}, fmt.Errorf("unable to parse datetime %q with format %q: %w", value, format, err)
	}

	return NewDateTime(t), nil
}

func ParseDateTimeWithTimezone(value string, tz *time.Location) (DateTime, error) {
	dt, err := ParseDateTime(value)
	if err != nil {
		return DateTime{}, err
	}

	if tz != nil {
		dt.timezone = tz
	}
	return dt, nil
}

func ParseDate(value string) (Date, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Date{}, fmt.Errorf("empty date string")
	}

	dateFormats := []string{
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"02-Jan-2006",
		"Jan 02, 2006",
		"January 02, 2006",
	}

	for _, format := range dateFormats {
		if t, err := time.Parse(format, value); err == nil {
			return NewDateFromTime(t), nil
		}
	}

	t, err := dateparse.ParseAny(value)
	if err != nil {
		return Date{}, fmt.Errorf("unable to parse date %q: %w", value, err)
	}

	return NewDateFromTime(t), nil
}

func ParseDateWithFormat(value string, format string) (Date, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Date{}, fmt.Errorf("empty date string")
	}

	goFormat := convertToGoTimeFormat(format)
	t, err := time.Parse(goFormat, value)
	if err != nil {
		return Date{}, fmt.Errorf("unable to parse date %q with format %q: %w", value, format, err)
	}

	return NewDateFromTime(t), nil
}

func ParseTime(value string) (Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Time{}, fmt.Errorf("empty time string")
	}

	timeFormats := []string{
		"15:04:05.999999999",
		"15:04:05.999999",
		"15:04:05.999",
		"15:04:05",
		"15:04",
		"3:04:05 PM",
		"3:04 PM",
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, value); err == nil {
			return NewTime(t.Hour(), t.Minute(), t.Second(), t.Nanosecond()), nil
		}
	}

	return Time{}, fmt.Errorf("unable to parse time %q", value)
}

func ParseTimeWithFormat(value string, format string) (Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Time{}, fmt.Errorf("empty time string")
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
	case Nanosecond:
		t = time.Unix(0, epoch).UTC()
	case Microsecond:
		t = time.Unix(0, epoch*1000).UTC()
	case Millisecond:
		t = time.Unix(0, epoch*1_000_000).UTC()
	case Second:
		t = time.Unix(epoch, 0).UTC()
	default:
		t = time.Unix(0, epoch).UTC()
	}
	return NewDateTime(t)
}

func convertToGoTimeFormat(format string) string {
	replacements := map[string]string{
		"%Y": "2006",
		"%y": "06",
		"%m": "01",
		"%b": "Jan",
		"%B": "January",
		"%d": "02",
		"%e": "_2",
		"%H": "15",
		"%I": "03",
		"%M": "04",
		"%S": "05",
		"%f": "999999",
		"%p": "PM",
		"%z": "-0700",
		"%Z": "MST",
		"%j": "002",
		"%U": "",
		"%W": "",
		"%w": "1",
		"%a": "Mon",
		"%A": "Monday",
	}

	goFormat := format
	for pattern, replacement := range replacements {
		goFormat = strings.ReplaceAll(goFormat, pattern, replacement)
	}

	return goFormat
}

func ParseDuration(s string) (Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Duration{}, fmt.Errorf("empty duration string")
	}

	if strings.Contains(s, "mo") || strings.Contains(s, "y") {
		return parsePeriodDuration(s)
	}

	goDuration, err := time.ParseDuration(s)
	if err != nil {
		return parsePeriodDuration(s)
	}

	return Duration{nanoseconds: int64(goDuration)}, nil
}

func parsePeriodDuration(s string) (Duration, error) {
	s = strings.ToLower(s)
	
	var months, days int32
	var nanoseconds int64

	parts := strings.Fields(s)
	for _, part := range parts {
		if len(part) < 2 {
			continue
		}

		numStr := ""
		unit := ""
		for i, ch := range part {
			if ch >= '0' && ch <= '9' || ch == '-' || ch == '.' {
				numStr += string(ch)
			} else {
				unit = part[i:]
				break
			}
		}

		if numStr == "" || unit == "" {
			continue
		}

		num, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			continue
		}

		switch unit {
		case "y", "yr", "year", "years":
			months += int32(num * 12)
		case "mo", "mon", "month", "months":
			months += int32(num)
		case "w", "wk", "week", "weeks":
			days += int32(num * 7)
		case "d", "day", "days":
			days += int32(num)
		case "h", "hr", "hour", "hours":
			nanoseconds += int64(num * float64(time.Hour))
		case "m", "min", "minute", "minutes":
			nanoseconds += int64(num * float64(time.Minute))
		case "s", "sec", "second", "seconds":
			nanoseconds += int64(num * float64(time.Second))
		case "ms", "millisecond", "milliseconds":
			nanoseconds += int64(num * float64(time.Millisecond))
		case "us", "µs", "microsecond", "microseconds":
			nanoseconds += int64(num * float64(time.Microsecond))
		case "ns", "nanosecond", "nanoseconds":
			nanoseconds += int64(num)
		}
	}

	if months == 0 && days == 0 && nanoseconds == 0 {
		return Duration{}, fmt.Errorf("unable to parse duration %q", s)
	}

	return Duration{
		months:      months,
		days:        days,
		nanoseconds: nanoseconds,
	}, nil
}