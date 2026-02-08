package datetime

import (
	_ "fmt"
	_ "strings"
	"time"
)

func DateRange(start, end time.Time, freq Duration) ([]DateTime, error) {
	panic("not implemented")

	// Return empty slice when start > end (matches test expectations)

}

func DateRangeFromString(start, end string, freq string) ([]DateTime, error) {
	panic("not implemented")

}

func BusinessDayRange(start, end time.Time) ([]DateTime, error) {
	panic("not implemented")

}

func DateRangeWithCount(start time.Time, freq Duration, count int) ([]DateTime, error) {
	panic("not implemented")

	// Return empty slice for non-positive count (matches test expectations)

}

func parseFrequencyString(freq string) (Duration, error) {
	panic("not implemented")

}

type DateRangeBuilder struct {
	start     *DateTime
	end       *DateTime
	freq      *Duration
	count     *int
	inclusive string
	timezone  *time.Location
	closed    string
}

func NewDateRangeBuilder() *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Start(dt DateTime) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) End(dt DateTime) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Frequency(freq Duration) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Count(n int) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Inclusive(inc string) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Closed(c string) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Timezone(tz *time.Location) *DateRangeBuilder {
	panic("not implemented")

}

func (b *DateRangeBuilder) Build() ([]DateTime, error) {
	panic("not implemented")

}
