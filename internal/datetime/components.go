package datetime

import (
	"time"
)

func (dt DateTime) Year() int {
	panic("not implemented")

}

func (dt DateTime) Month() time.Month {
	panic("not implemented")

}

func (dt DateTime) Day() int {
	panic("not implemented")

}

func (dt DateTime) Hour() int {
	panic("not implemented")

}

func (dt DateTime) Minute() int {
	panic("not implemented")

}

func (dt DateTime) Second() int {
	panic("not implemented")

}

func (dt DateTime) Nanosecond() int {
	panic("not implemented")

}

func (dt DateTime) Microsecond() int {
	panic("not implemented")

}

func (dt DateTime) Millisecond() int {
	panic("not implemented")

}

func (dt DateTime) DayOfWeek() int {
	panic("not implemented")

}

func (dt DateTime) DayOfYear() int {
	panic("not implemented")

}

func (dt DateTime) Quarter() int {
	panic("not implemented")

}

func (dt DateTime) WeekOfYear() int {
	panic("not implemented")

}

func (dt DateTime) ISOYear() int {
	panic("not implemented")

}

func (dt DateTime) IsLeapYear() bool {
	panic("not implemented")

}

func (dt DateTime) IsWeekend() bool {
	panic("not implemented")

}

func (d Date) IsWeekend() bool {
	panic("not implemented")

}

func (dt DateTime) IsMonthStart() bool {
	panic("not implemented")

}

func (dt DateTime) IsMonthEnd() bool {
	panic("not implemented")

}

func (dt DateTime) IsQuarterStart() bool {
	panic("not implemented")

}

func (dt DateTime) IsQuarterEnd() bool {
	panic("not implemented")

}

func (dt DateTime) IsYearStart() bool {
	panic("not implemented")

}

func (dt DateTime) IsYearEnd() bool {
	panic("not implemented")

}

func (dt DateTime) Round(unit TimeUnit) DateTime {
	panic("not implemented")

}

func (dt DateTime) Floor(unit TimeUnit) DateTime {
	panic("not implemented")

}

func (dt DateTime) Ceil(unit TimeUnit) DateTime {
	panic("not implemented")

}

func (dt DateTime) Truncate(unit TimeUnit) DateTime {
	panic("not implemented")

}

func ExtractYear(timestamps []int64, tz *time.Location) []int32 {
	panic("not implemented")

}

func ExtractMonth(timestamps []int64, tz *time.Location) []int8 {
	panic("not implemented")

}

func ExtractDay(timestamps []int64, tz *time.Location) []int8 {
	panic("not implemented")

}

func ExtractHour(timestamps []int64, tz *time.Location) []int8 {
	panic("not implemented")

}

func ExtractMinute(timestamps []int64, tz *time.Location) []int8 {
	panic("not implemented")

}

func ExtractSecond(timestamps []int64, tz *time.Location) []int8 {
	panic("not implemented")

}
