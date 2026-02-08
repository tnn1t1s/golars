package datetime

import (
	"time"
)

func (dt DateTime) Year() int {
	return dt.Time().Year()
}

func (dt DateTime) Month() time.Month {
	return dt.Time().Month()
}

func (dt DateTime) Day() int {
	return dt.Time().Day()
}

func (dt DateTime) Hour() int {
	return dt.Time().Hour()
}

func (dt DateTime) Minute() int {
	return dt.Time().Minute()
}

func (dt DateTime) Second() int {
	return dt.Time().Second()
}

func (dt DateTime) Nanosecond() int {
	return dt.Time().Nanosecond()
}

func (dt DateTime) Microsecond() int {
	return dt.Time().Nanosecond() / 1000
}

func (dt DateTime) Millisecond() int {
	return dt.Time().Nanosecond() / 1000000
}

func (dt DateTime) DayOfWeek() int {
	// Monday=1, ..., Sunday=7 (ISO-style, based on test expecting Monday=1)
	wd := dt.Time().Weekday()
	if wd == time.Sunday {
		return 7
	}
	return int(wd)
}

func (dt DateTime) DayOfYear() int {
	return dt.Time().YearDay()
}

func (dt DateTime) Quarter() int {
	return (int(dt.Time().Month()) - 1) / 3 + 1
}

func (dt DateTime) WeekOfYear() int {
	_, week := dt.Time().ISOWeek()
	return week
}

func (dt DateTime) ISOYear() int {
	year, _ := dt.Time().ISOWeek()
	return year
}

func (dt DateTime) IsLeapYear() bool {
	y := dt.Time().Year()
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

func (dt DateTime) IsWeekend() bool {
	wd := dt.Time().Weekday()
	return wd == time.Saturday || wd == time.Sunday
}

func (d Date) IsWeekend() bool {
	wd := d.Time().Weekday()
	return wd == time.Saturday || wd == time.Sunday
}

func (dt DateTime) IsMonthStart() bool {
	return dt.Time().Day() == 1
}

func (dt DateTime) IsMonthEnd() bool {
	t := dt.Time()
	// Add one day and check if month changes
	next := t.AddDate(0, 0, 1)
	return next.Month() != t.Month()
}

func (dt DateTime) IsQuarterStart() bool {
	t := dt.Time()
	m := t.Month()
	return t.Day() == 1 && (m == time.January || m == time.April || m == time.July || m == time.October)
}

func (dt DateTime) IsQuarterEnd() bool {
	t := dt.Time()
	m := t.Month()
	if !(m == time.March || m == time.June || m == time.September || m == time.December) {
		return false
	}
	next := t.AddDate(0, 0, 1)
	return next.Month() != t.Month()
}

func (dt DateTime) IsYearStart() bool {
	t := dt.Time()
	return t.Month() == time.January && t.Day() == 1
}

func (dt DateTime) IsYearEnd() bool {
	t := dt.Time()
	return t.Month() == time.December && t.Day() == 31
}

func (dt DateTime) Round(unit TimeUnit) DateTime {
	floored := dt.Floor(unit)
	ceiled := dt.Ceil(unit)
	// Round to nearest: if closer to floor, use floor; otherwise use ceil
	diffFloor := dt.timestamp - floored.timestamp
	diffCeil := ceiled.timestamp - dt.timestamp
	if diffFloor <= diffCeil {
		return floored
	}
	return ceiled
}

func (dt DateTime) Floor(unit TimeUnit) DateTime {
	t := dt.Time()
	var floored time.Time
	switch unit {
	case Nanosecond:
		floored = t
	case Microsecond:
		floored = t.Truncate(time.Microsecond)
	case Millisecond:
		floored = t.Truncate(time.Millisecond)
	case Second:
		floored = t.Truncate(time.Second)
	case Minute:
		floored = t.Truncate(time.Minute)
	case Hour:
		floored = t.Truncate(time.Hour)
	case Day:
		floored = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case Week:
		// Floor to Monday
		wd := t.Weekday()
		daysBack := int(wd) - int(time.Monday)
		if daysBack < 0 {
			daysBack += 7
		}
		floored = time.Date(t.Year(), t.Month(), t.Day()-daysBack, 0, 0, 0, 0, t.Location())
	case Month:
		floored = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case Quarter:
		q := (int(t.Month()) - 1) / 3
		floored = time.Date(t.Year(), time.Month(q*3+1), 1, 0, 0, 0, 0, t.Location())
	case Year:
		floored = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	default:
		floored = t
	}
	return NewDateTime(floored)
}

func (dt DateTime) Ceil(unit TimeUnit) DateTime {
	floored := dt.Floor(unit)
	if floored.timestamp == dt.timestamp {
		return dt
	}
	// Add one unit to the floored value
	t := floored.Time()
	var ceiled time.Time
	switch unit {
	case Nanosecond:
		ceiled = t.Add(1)
	case Microsecond:
		ceiled = t.Add(time.Microsecond)
	case Millisecond:
		ceiled = t.Add(time.Millisecond)
	case Second:
		ceiled = t.Add(time.Second)
	case Minute:
		ceiled = t.Add(time.Minute)
	case Hour:
		ceiled = t.Add(time.Hour)
	case Day:
		ceiled = t.AddDate(0, 0, 1)
	case Week:
		ceiled = t.AddDate(0, 0, 7)
	case Month:
		ceiled = t.AddDate(0, 1, 0)
	case Quarter:
		ceiled = t.AddDate(0, 3, 0)
	case Year:
		ceiled = t.AddDate(1, 0, 0)
	default:
		ceiled = t
	}
	return NewDateTime(ceiled)
}

func (dt DateTime) Truncate(unit TimeUnit) DateTime {
	return dt.Floor(unit)
}

func ExtractYear(timestamps []int64, tz *time.Location) []int32 {
	result := make([]int32, len(timestamps))
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		result[i] = int32(t.Year())
	}
	return result
}

func ExtractMonth(timestamps []int64, tz *time.Location) []int8 {
	result := make([]int8, len(timestamps))
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		result[i] = int8(t.Month())
	}
	return result
}

func ExtractDay(timestamps []int64, tz *time.Location) []int8 {
	result := make([]int8, len(timestamps))
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		result[i] = int8(t.Day())
	}
	return result
}

func ExtractHour(timestamps []int64, tz *time.Location) []int8 {
	result := make([]int8, len(timestamps))
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		result[i] = int8(t.Hour())
	}
	return result
}

func ExtractMinute(timestamps []int64, tz *time.Location) []int8 {
	result := make([]int8, len(timestamps))
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		result[i] = int8(t.Minute())
	}
	return result
}

func ExtractSecond(timestamps []int64, tz *time.Location) []int8 {
	result := make([]int8, len(timestamps))
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		result[i] = int8(t.Second())
	}
	return result
}
