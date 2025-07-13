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
	return dt.Nanosecond() / 1000
}

func (dt DateTime) Millisecond() int {
	return dt.Nanosecond() / 1_000_000
}

func (dt DateTime) DayOfWeek() int {
	return int(dt.Time().Weekday())
}

func (dt DateTime) DayOfYear() int {
	return dt.Time().YearDay()
}

func (dt DateTime) Quarter() int {
	month := dt.Month()
	return (int(month)-1)/3 + 1
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
	year := dt.Year()
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func (dt DateTime) IsWeekend() bool {
	dow := dt.Time().Weekday()
	return dow == time.Saturday || dow == time.Sunday
}

func (d Date) IsWeekend() bool {
	dow := d.Time().Weekday()
	return dow == time.Saturday || dow == time.Sunday
}

func (dt DateTime) IsMonthStart() bool {
	return dt.Day() == 1
}

func (dt DateTime) IsMonthEnd() bool {
	t := dt.Time()
	nextMonth := t.AddDate(0, 0, 1)
	return nextMonth.Month() != t.Month()
}

func (dt DateTime) IsQuarterStart() bool {
	return dt.Day() == 1 && (dt.Month() == 1 || dt.Month() == 4 || dt.Month() == 7 || dt.Month() == 10)
}

func (dt DateTime) IsQuarterEnd() bool {
	if !dt.IsMonthEnd() {
		return false
	}
	month := dt.Month()
	return month == 3 || month == 6 || month == 9 || month == 12
}

func (dt DateTime) IsYearStart() bool {
	return dt.Month() == 1 && dt.Day() == 1
}

func (dt DateTime) IsYearEnd() bool {
	return dt.Month() == 12 && dt.Day() == 31
}

func (dt DateTime) Round(unit TimeUnit) DateTime {
	t := dt.Time()
	
	switch unit {
	case Second:
		nanos := t.Nanosecond()
		if nanos >= 500_000_000 {
			t = t.Add(time.Second).Truncate(time.Second)
		} else {
			t = t.Truncate(time.Second)
		}
	case Minute:
		secs := t.Second()
		t = t.Truncate(time.Minute)
		if secs >= 30 {
			t = t.Add(time.Minute)
		}
	case Hour:
		mins := t.Minute()
		t = t.Truncate(time.Hour)
		if mins >= 30 {
			t = t.Add(time.Hour)
		}
	case Day:
		hour := t.Hour()
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		if hour >= 12 {
			t = t.AddDate(0, 0, 1)
		}
	case Week:
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		t = t.AddDate(0, 0, -weekday+1)
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case Month:
		t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		if t.Day() > 15 {
			t = t.AddDate(0, 1, 0)
		}
	case Quarter:
		month := t.Month()
		quarter := (month-1)/3 + 1
		firstMonthOfQuarter := (quarter-1)*3 + 1
		t = time.Date(t.Year(), time.Month(firstMonthOfQuarter), 1, 0, 0, 0, 0, t.Location())
	case Year:
		t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
		if t.Month() > 6 {
			t = t.AddDate(1, 0, 0)
		}
	}
	
	return NewDateTime(t)
}

func (dt DateTime) Floor(unit TimeUnit) DateTime {
	t := dt.Time()
	
	switch unit {
	case Nanosecond:
		return dt
	case Microsecond:
		nanos := t.Nanosecond()
		nanos = (nanos / 1000) * 1000
		t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), nanos, t.Location())
	case Millisecond:
		nanos := t.Nanosecond()
		nanos = (nanos / 1_000_000) * 1_000_000
		t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), nanos, t.Location())
	case Second:
		t = t.Truncate(time.Second)
	case Minute:
		t = t.Truncate(time.Minute)
	case Hour:
		t = t.Truncate(time.Hour)
	case Day:
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case Week:
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		t = t.AddDate(0, 0, -weekday+1)
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case Month:
		t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case Quarter:
		month := t.Month()
		quarter := (month-1)/3 + 1
		firstMonthOfQuarter := (quarter-1)*3 + 1
		t = time.Date(t.Year(), time.Month(firstMonthOfQuarter), 1, 0, 0, 0, 0, t.Location())
	case Year:
		t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	}
	
	return NewDateTime(t)
}

func (dt DateTime) Ceil(unit TimeUnit) DateTime {
	floored := dt.Floor(unit)
	if floored.timestamp == dt.timestamp {
		return dt
	}
	
	switch unit {
	case Microsecond:
		return NewDateTime(floored.Time().Add(time.Microsecond))
	case Millisecond:
		return NewDateTime(floored.Time().Add(time.Millisecond))
	case Second:
		return NewDateTime(floored.Time().Add(time.Second))
	case Minute:
		return NewDateTime(floored.Time().Add(time.Minute))
	case Hour:
		return NewDateTime(floored.Time().Add(time.Hour))
	case Day:
		return NewDateTime(floored.Time().AddDate(0, 0, 1))
	case Week:
		return NewDateTime(floored.Time().AddDate(0, 0, 7))
	case Month:
		return NewDateTime(floored.Time().AddDate(0, 1, 0))
	case Quarter:
		return NewDateTime(floored.Time().AddDate(0, 3, 0))
	case Year:
		return NewDateTime(floored.Time().AddDate(1, 0, 0))
	default:
		return floored
	}
}

func (dt DateTime) Truncate(unit TimeUnit) DateTime {
	return dt.Floor(unit)
}

func ExtractYear(timestamps []int64, tz *time.Location) []int32 {
	years := make([]int32, len(timestamps))
	
	if tz == nil {
		tz = time.UTC
	}
	
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		years[i] = int32(t.Year())
	}
	
	return years
}

func ExtractMonth(timestamps []int64, tz *time.Location) []int8 {
	months := make([]int8, len(timestamps))
	
	if tz == nil {
		tz = time.UTC
	}
	
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		months[i] = int8(t.Month())
	}
	
	return months
}

func ExtractDay(timestamps []int64, tz *time.Location) []int8 {
	days := make([]int8, len(timestamps))
	
	if tz == nil {
		tz = time.UTC
	}
	
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		days[i] = int8(t.Day())
	}
	
	return days
}

func ExtractHour(timestamps []int64, tz *time.Location) []int8 {
	hours := make([]int8, len(timestamps))
	
	if tz == nil {
		tz = time.UTC
	}
	
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		hours[i] = int8(t.Hour())
	}
	
	return hours
}

func ExtractMinute(timestamps []int64, tz *time.Location) []int8 {
	minutes := make([]int8, len(timestamps))
	
	if tz == nil {
		tz = time.UTC
	}
	
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		minutes[i] = int8(t.Minute())
	}
	
	return minutes
}

func ExtractSecond(timestamps []int64, tz *time.Location) []int8 {
	seconds := make([]int8, len(timestamps))
	
	if tz == nil {
		tz = time.UTC
	}
	
	for i, ts := range timestamps {
		t := time.Unix(0, ts).In(tz)
		seconds[i] = int8(t.Second())
	}
	
	return seconds
}