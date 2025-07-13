package datetime

import (
	"fmt"
	"strings"
	"time"
)

func DateRange(start, end time.Time, freq Duration) ([]DateTime, error) {
	if start.After(end) {
		// Return empty slice when start > end (matches test expectations)
		return []DateTime{}, nil
	}
	
	if freq.IsZero() {
		return nil, fmt.Errorf("frequency cannot be zero")
	}
	
	var result []DateTime
	current := NewDateTime(start)
	endDT := NewDateTime(end)
	
	for current.timestamp <= endDT.timestamp {
		result = append(result, current)
		current = current.Add(freq)
	}
	
	return result, nil
}

func DateRangeFromString(start, end string, freq string) ([]DateTime, error) {
	startDT, err := ParseDateTime(start)
	if err != nil {
		return nil, fmt.Errorf("invalid start datetime: %w", err)
	}
	
	endDT, err := ParseDateTime(end)
	if err != nil {
		return nil, fmt.Errorf("invalid end datetime: %w", err)
	}
	
	freqDur, err := parseFrequencyString(freq)
	if err != nil {
		return nil, fmt.Errorf("invalid frequency: %w", err)
	}
	
	return DateRange(startDT.Time(), endDT.Time(), freqDur)
}

func BusinessDayRange(start, end time.Time) ([]DateTime, error) {
	if start.After(end) {
		return nil, fmt.Errorf("start time %v is after end time %v", start, end)
	}
	
	var result []DateTime
	current := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
	endDate := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, end.Location())
	
	for !current.After(endDate) {
		weekday := current.Weekday()
		if weekday != time.Saturday && weekday != time.Sunday {
			result = append(result, NewDateTime(current))
		}
		current = current.AddDate(0, 0, 1)
	}
	
	return result, nil
}

func DateRangeWithCount(start time.Time, freq Duration, count int) ([]DateTime, error) {
	if count <= 0 {
		// Return empty slice for non-positive count (matches test expectations)
		return []DateTime{}, nil
	}
	
	if freq.IsZero() {
		return nil, fmt.Errorf("frequency cannot be zero")
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
	freq = strings.ToUpper(strings.TrimSpace(freq))
	
	if len(freq) == 0 {
		return Duration{}, fmt.Errorf("empty frequency string")
	}
	
	multiple := 1
	unit := freq
	
	for i, ch := range freq {
		if ch < '0' || ch > '9' {
			if i > 0 {
				fmt.Sscanf(freq[:i], "%d", &multiple)
				unit = freq[i:]
			}
			break
		}
	}
	
	switch unit {
	case "D":
		return Days(multiple), nil
	case "B":
		return Days(1), nil
	case "W":
		return Weeks(multiple), nil
	case "M":
		return Months(multiple), nil
	case "ME":
		return Months(multiple), nil
	case "Q", "QS":
		return Months(multiple * 3), nil
	case "QE":
		return Months(multiple * 3), nil
	case "Y", "YS":
		return Years(multiple), nil
	case "YE":
		return Years(multiple), nil
	case "H":
		return Hours(multiple), nil
	case "T", "MIN":
		return Minutes(multiple), nil
	case "S":
		return Seconds(multiple), nil
	case "L", "MS":
		return Milliseconds(multiple), nil
	case "U", "US":
		return Microseconds(multiple), nil
	case "N", "NS":
		return Nanoseconds(int64(multiple)), nil
	default:
		return ParseDuration(freq)
	}
}

type DateRangeBuilder struct {
	start      *DateTime
	end        *DateTime
	freq       *Duration
	count      *int
	inclusive  string
	timezone   *time.Location
	closed     string
}

func NewDateRangeBuilder() *DateRangeBuilder {
	return &DateRangeBuilder{
		inclusive: "both",
		closed:    "left",
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
	
	if b.count != nil && *b.count > 0 {
		return DateRangeWithCount(b.start.Time(), *b.freq, *b.count)
	}
	
	if b.end == nil {
		return nil, fmt.Errorf("either end date or count is required")
	}
	
	result, err := DateRange(b.start.Time(), b.end.Time(), *b.freq)
	if err != nil {
		return nil, err
	}
	
	switch b.closed {
	case "left":
		if len(result) > 0 && result[len(result)-1].timestamp > b.end.timestamp {
			result = result[:len(result)-1]
		}
	case "right":
		if len(result) > 0 {
			result = result[1:]
		}
	case "neither":
		if len(result) > 0 {
			if result[len(result)-1].timestamp > b.end.timestamp {
				result = result[:len(result)-1]
			}
			if len(result) > 0 {
				result = result[1:]
			}
		}
	}
	
	if b.timezone != nil {
		for i := range result {
			result[i] = result[i].WithTimezone(b.timezone)
		}
	}
	
	return result, nil
}