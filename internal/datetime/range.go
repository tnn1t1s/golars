package datetime

import (
	"fmt"
	"time"
)

func DateRange(start, end time.Time, freq Duration) ([]DateTime, error) {
	// Return empty slice when start > end
	if start.After(end) {
		return []DateTime{}, nil
	}

	var result []DateTime
	current := NewDateTime(start)
	endDt := NewDateTime(end)

	for current.timestamp <= endDt.timestamp {
		result = append(result, current)
		current = current.Add(freq)
	}
	return result, nil
}

func DateRangeFromString(start, end string, freq string) ([]DateTime, error) {
	startDt, err := ParseDateTime(start)
	if err != nil {
		return nil, fmt.Errorf("failed to parse start date: %w", err)
	}
	endDt, err := ParseDateTime(end)
	if err != nil {
		return nil, fmt.Errorf("failed to parse end date: %w", err)
	}
	duration, err := parseFrequencyString(freq)
	if err != nil {
		return nil, fmt.Errorf("failed to parse frequency: %w", err)
	}
	return DateRange(startDt.Time(), endDt.Time(), duration)
}

func BusinessDayRange(start, end time.Time) ([]DateTime, error) {
	if start.After(end) {
		return []DateTime{}, nil
	}

	var result []DateTime
	current := start
	for !current.After(end) {
		wd := current.Weekday()
		if wd != time.Saturday && wd != time.Sunday {
			result = append(result, NewDateTime(current))
		}
		current = current.AddDate(0, 0, 1)
	}
	return result, nil
}

func DateRangeWithCount(start time.Time, freq Duration, count int) ([]DateTime, error) {
	// Return empty slice for non-positive count
	if count <= 0 {
		return []DateTime{}, nil
	}

	result := make([]DateTime, count)
	current := NewDateTime(start)
	for i := 0; i < count; i++ {
		result[i] = current
		current = current.Add(freq)
	}
	return result, nil
}

func parseFrequencyString(freq string) (Duration, error) {
	return ParseDuration(freq)
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
	return &DateRangeBuilder{
		inclusive: "both",
	}
}

func (b *DateRangeBuilder) Start(dt DateTime) *DateRangeBuilder {
	b.start = &dt
	return b
}

func (b *DateRangeBuilder) End(dt DateTime) *DateRangeBuilder {
	b.end = &dt
	return b
}

func (b *DateRangeBuilder) Frequency(freq Duration) *DateRangeBuilder {
	b.freq = &freq
	return b
}

func (b *DateRangeBuilder) Count(n int) *DateRangeBuilder {
	b.count = &n
	return b
}

func (b *DateRangeBuilder) Inclusive(inc string) *DateRangeBuilder {
	b.inclusive = inc
	return b
}

func (b *DateRangeBuilder) Closed(c string) *DateRangeBuilder {
	b.closed = c
	return b
}

func (b *DateRangeBuilder) Timezone(tz *time.Location) *DateRangeBuilder {
	b.timezone = tz
	return b
}

func (b *DateRangeBuilder) Build() ([]DateTime, error) {
	if b.start == nil {
		return nil, fmt.Errorf("start date is required")
	}
	if b.freq == nil {
		return nil, fmt.Errorf("frequency is required")
	}
	if b.end == nil && b.count == nil {
		return nil, fmt.Errorf("end date or count is required")
	}

	if b.count != nil {
		return DateRangeWithCount(b.start.Time(), *b.freq, *b.count)
	}

	return DateRange(b.start.Time(), b.end.Time(), *b.freq)
}
