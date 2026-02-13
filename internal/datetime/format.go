package datetime

import (
	"fmt"
	"strings"
	"time"
)

func (dt DateTime) Format(layout string) string {
	if layout == "" {
		layout = time.RFC3339Nano
	} else {
		layout = convertPolarsToGoFormat(layout)
	}
	return dt.Time().Format(layout)
}

func (dt DateTime) ISOFormat() string {
	return dt.Time().Format(time.RFC3339Nano)
}

func (dt DateTime) String() string {
	return dt.ISOFormat()
}

func (d Date) Format(layout string) string {
	if layout == "" {
		layout = "2006-01-02"
	} else {
		layout = convertPolarsToGoFormat(layout)
	}
	return d.Time().Format(layout)
}

func (d Date) ISOFormat() string {
	return d.Time().Format("2006-01-02")
}

func (d Date) String() string {
	return d.ISOFormat()
}

func (t Time) Format(layout string) string {
	if layout == "" {
		layout = "15:04:05.999999999"
	} else {
		layout = convertPolarsToGoFormat(layout)
	}
	// Create a reference time with the Time components
	ref := time.Date(2000, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	return ref.Format(layout)
}

func (t Time) ISOFormat() string {
	return fmt.Sprintf("%02d:%02d:%02d.%09d", t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
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
		return 0
	}
}

func FormatWithPolarsStyle(dt DateTime, format string) string {
	goFormat := convertPolarsToGoFormat(format)
	return dt.Time().Format(goFormat)
}

func convertPolarsToGoFormat(format string) string {
	replacements := []struct {
		from string
		to   string
	}{
		{"%Y", "2006"},
		{"%m", "01"},
		{"%d", "02"},
		{"%H", "15"},
		{"%I", "03"},
		{"%M", "04"},
		{"%S", "05"},
		{"%f", "000000"},
		{"%p", "PM"},
		{"%Z", "MST"},
		{"%z", "-0700"},
		{"%a", "Mon"},
		{"%A", "Monday"},
		{"%b", "Jan"},
		{"%B", "January"},
		{"%c", "Mon Jan 02 15:04:05 2006"},
		{"%j", "002"},
	}

	result := format
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.from, r.to)
	}
	return result
}

func FormatDuration(d Duration) string {
	return d.String()
}

func (d Duration) String() string {
	parts := []string{}
	if d.months != 0 {
		years := d.months / 12
		months := d.months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		}
		if months != 0 {
			parts = append(parts, fmt.Sprintf("%d months", months))
		}
	}
	if d.days != 0 {
		if d.days == 1 || d.days == -1 {
			parts = append(parts, fmt.Sprintf("%d day", d.days))
		} else {
			parts = append(parts, fmt.Sprintf("%d days", d.days))
		}
	}
	if d.nanoseconds != 0 {
		dur := time.Duration(d.nanoseconds)
		hours := int(dur.Hours())
		remaining := dur - time.Duration(hours)*time.Hour
		minutes := int(remaining.Minutes())
		remaining -= time.Duration(minutes) * time.Minute
		seconds := int(remaining.Seconds())

		if hours != 0 {
			if hours == 1 || hours == -1 {
				parts = append(parts, fmt.Sprintf("%d hour", hours))
			} else {
				parts = append(parts, fmt.Sprintf("%d hours", hours))
			}
		}
		if minutes != 0 {
			parts = append(parts, fmt.Sprintf("%d minutes", minutes))
		}
		if seconds != 0 {
			parts = append(parts, fmt.Sprintf("%d seconds", seconds))
		}
		// If no larger components, show the raw duration
		if hours == 0 && minutes == 0 && seconds == 0 {
			parts = append(parts, dur.String())
		}
	}
	if len(parts) == 0 {
		return "0s"
	}
	return strings.Join(parts, " ")
}
