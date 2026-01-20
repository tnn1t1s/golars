package datetime

import (
	"fmt"
	"strings"
	"time"
)

func (dt DateTime) Format(layout string) string {
	return dt.Time().Format(layout)
}

func (dt DateTime) ISOFormat() string {
	return dt.Time().Format(time.RFC3339Nano)
}

func (dt DateTime) String() string {
	return dt.ISOFormat()
}

func (d Date) Format(layout string) string {
	return d.Time().Format(layout)
}

func (d Date) ISOFormat() string {
	return d.Time().Format("2006-01-02")
}

func (d Date) String() string {
	return d.ISOFormat()
}

func (t Time) Format(layout string) string {
	baseTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	fullTime := baseTime.Add(time.Duration(t.nanoseconds))
	return fullTime.Format(layout)
}

func (t Time) ISOFormat() string {
	hours := t.Hour()
	minutes := t.Minute()
	seconds := t.Second()
	nanos := t.Nanosecond()

	if nanos == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}

	return fmt.Sprintf("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanos)
}

func (t Time) String() string {
	return t.ISOFormat()
}

func (tu TimeUnit) ToDuration() time.Duration {
	switch tu {
	case Nanosecond:
		return time.Nanosecond
	case Microsecond:
		return time.Microsecond
	case Millisecond:
		return time.Millisecond
	case Second:
		return time.Second
	case Minute:
		return time.Minute
	case Hour:
		return time.Hour
	case Day:
		return 24 * time.Hour
	case Week:
		return 7 * 24 * time.Hour
	default:
		return time.Duration(0)
	}
}

func FormatWithPolarsStyle(dt DateTime, format string) string {
	format = convertPolarsToGoFormat(format)
	return dt.Time().Format(format)
}

func convertPolarsToGoFormat(format string) string {
	replacements := []struct {
		from string
		to   string
	}{
		{"%Y", "2006"},
		{"%y", "06"},
		{"%m", "01"},
		{"%b", "Jan"},
		{"%B", "January"},
		{"%d", "02"},
		{"%e", "_2"},
		{"%H", "15"},
		{"%I", "03"},
		{"%M", "04"},
		{"%S", "05"},
		{"%f", "999999"},
		{"%p", "PM"},
		{"%z", "-0700"},
		{"%Z", "MST"},
		{"%j", "002"},
		{"%w", "1"},
		{"%a", "Mon"},
		{"%A", "Monday"},
		{"%U", ""},
		{"%W", ""},
		{"%c", "Mon Jan 2 15:04:05 2006"},
		{"%x", "01/02/06"},
		{"%X", "15:04:05"},
		{"%%", "%"},
	}

	result := format
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.from, r.to)
	}

	return result
}

func FormatDuration(d Duration) string {
	var parts []string

	if d.months != 0 {
		years := d.months / 12
		months := d.months % 12

		if years != 0 {
			if years == 1 {
				parts = append(parts, "1 year")
			} else {
				parts = append(parts, fmt.Sprintf("%d years", years))
			}
		}

		if months != 0 {
			if months == 1 {
				parts = append(parts, "1 month")
			} else {
				parts = append(parts, fmt.Sprintf("%d months", months))
			}
		}
	}

	if d.days != 0 {
		if d.days == 1 {
			parts = append(parts, "1 day")
		} else {
			parts = append(parts, fmt.Sprintf("%d days", d.days))
		}
	}

	if d.nanoseconds != 0 {
		dur := time.Duration(d.nanoseconds)

		hours := dur / time.Hour
		dur %= time.Hour

		minutes := dur / time.Minute
		dur %= time.Minute

		seconds := dur / time.Second
		dur %= time.Second

		millis := dur / time.Millisecond
		dur %= time.Millisecond

		micros := dur / time.Microsecond
		dur %= time.Microsecond

		nanos := dur

		if hours > 0 {
			if hours == 1 {
				parts = append(parts, "1 hour")
			} else {
				parts = append(parts, fmt.Sprintf("%d hours", hours))
			}
		}

		if minutes > 0 {
			if minutes == 1 {
				parts = append(parts, "1 minute")
			} else {
				parts = append(parts, fmt.Sprintf("%d minutes", minutes))
			}
		}

		if seconds > 0 {
			if seconds == 1 {
				parts = append(parts, "1 second")
			} else {
				parts = append(parts, fmt.Sprintf("%d seconds", seconds))
			}
		}

		if millis > 0 {
			parts = append(parts, fmt.Sprintf("%d ms", millis))
		}

		if micros > 0 {
			parts = append(parts, fmt.Sprintf("%d µs", micros))
		}

		if nanos > 0 {
			parts = append(parts, fmt.Sprintf("%d ns", nanos))
		}
	}

	if len(parts) == 0 {
		return "0s"
	}

	return strings.Join(parts, " ")
}

func (d Duration) String() string {
	return FormatDuration(d)
}
