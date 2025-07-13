package datetime

import (
	"testing"
	"time"
)

func TestDateTimeComponents(t *testing.T) {
	dt := NewDateTime(time.Date(2024, 7, 15, 14, 30, 45, 123456789, time.UTC))

	tests := []struct {
		name string
		got  int
		want int
	}{
		{"Year", dt.Year(), 2024},
		{"Month", int(dt.Month()), 7},
		{"Day", dt.Day(), 15},
		{"Hour", dt.Hour(), 14},
		{"Minute", dt.Minute(), 30},
		{"Second", dt.Second(), 45},
		{"Nanosecond", dt.Nanosecond(), 123456789},
		{"Microsecond", dt.Microsecond(), 123456},
		{"Millisecond", dt.Millisecond(), 123},
		{"DayOfWeek", dt.DayOfWeek(), 1}, // Monday
		{"DayOfYear", dt.DayOfYear(), 197},
		{"Quarter", dt.Quarter(), 3},
		{"WeekOfYear", dt.WeekOfYear(), 29},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestDateTimePredicates(t *testing.T) {
	tests := []struct {
		name string
		dt   DateTime
		fn   func() bool
		want bool
	}{
		{
			name: "IsLeapYear - true",
			dt:   NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)).IsLeapYear() },
			want: true,
		},
		{
			name: "IsLeapYear - false",
			dt:   NewDateTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).IsLeapYear() },
			want: false,
		},
		{
			name: "IsWeekend - Saturday",
			dt:   NewDateTime(time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC)).IsWeekend() },
			want: true,
		},
		{
			name: "IsWeekend - Monday",
			dt:   NewDateTime(time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)).IsWeekend() },
			want: false,
		},
		{
			name: "IsMonthStart",
			dt:   NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)).IsMonthStart() },
			want: true,
		},
		{
			name: "IsMonthEnd",
			dt:   NewDateTime(time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)).IsMonthEnd() },
			want: true,
		},
		{
			name: "IsQuarterStart",
			dt:   NewDateTime(time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)).IsQuarterStart() },
			want: true,
		},
		{
			name: "IsQuarterEnd",
			dt:   NewDateTime(time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC)).IsQuarterEnd() },
			want: true,
		},
		{
			name: "IsYearStart",
			dt:   NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)).IsYearStart() },
			want: true,
		},
		{
			name: "IsYearEnd",
			dt:   NewDateTime(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)),
			fn:   func() bool { return NewDateTime(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)).IsYearEnd() },
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn(); got != tt.want {
				t.Errorf("%s = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDateTimeRounding(t *testing.T) {
	tests := []struct {
		name string
		dt   DateTime
		unit TimeUnit
		op   string
		want time.Time
	}{
		{
			name: "Floor to minute",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)),
			unit: Minute,
			op:   "floor",
			want: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name: "Ceil to minute",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)),
			unit: Minute,
			op:   "ceil",
			want: time.Date(2024, 1, 15, 10, 31, 0, 0, time.UTC),
		},
		{
			name: "Round to minute - down",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 25, 0, time.UTC)),
			unit: Minute,
			op:   "round",
			want: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name: "Round to minute - up",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 35, 0, time.UTC)),
			unit: Minute,
			op:   "round",
			want: time.Date(2024, 1, 15, 10, 31, 0, 0, time.UTC),
		},
		{
			name: "Floor to hour",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)),
			unit: Hour,
			op:   "floor",
			want: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		{
			name: "Floor to day",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)),
			unit: Day,
			op:   "floor",
			want: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Floor to week",
			dt:   NewDateTime(time.Date(2024, 1, 17, 10, 30, 45, 0, time.UTC)), // Wednesday
			unit: Week,
			op:   "floor",
			want: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), // Monday
		},
		{
			name: "Floor to month",
			dt:   NewDateTime(time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)),
			unit: Month,
			op:   "floor",
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Floor to quarter",
			dt:   NewDateTime(time.Date(2024, 5, 15, 10, 30, 45, 0, time.UTC)),
			unit: Quarter,
			op:   "floor",
			want: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Floor to year",
			dt:   NewDateTime(time.Date(2024, 7, 15, 10, 30, 45, 0, time.UTC)),
			unit: Year,
			op:   "floor",
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result DateTime
			switch tt.op {
			case "floor":
				result = tt.dt.Floor(tt.unit)
			case "ceil":
				result = tt.dt.Ceil(tt.unit)
			case "round":
				result = tt.dt.Round(tt.unit)
			}

			if !result.Time().Equal(tt.want) {
				t.Errorf("%s(%s) = %v, want %v", tt.op, tt.unit, result.Time(), tt.want)
			}
		})
	}
}

func TestExtractFunctions(t *testing.T) {
	timestamps := []int64{
		time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC).UnixNano(),
		time.Date(2024, 7, 31, 23, 59, 59, 0, time.UTC).UnixNano(),
		time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC).UnixNano(),
	}

	t.Run("ExtractYear", func(t *testing.T) {
		years := ExtractYear(timestamps, time.UTC)
		expected := []int32{2024, 2024, 2023}
		for i, got := range years {
			if got != expected[i] {
				t.Errorf("ExtractYear[%d] = %v, want %v", i, got, expected[i])
			}
		}
	})

	t.Run("ExtractMonth", func(t *testing.T) {
		months := ExtractMonth(timestamps, time.UTC)
		expected := []int8{1, 7, 12}
		for i, got := range months {
			if got != expected[i] {
				t.Errorf("ExtractMonth[%d] = %v, want %v", i, got, expected[i])
			}
		}
	})

	t.Run("ExtractDay", func(t *testing.T) {
		days := ExtractDay(timestamps, time.UTC)
		expected := []int8{15, 31, 25}
		for i, got := range days {
			if got != expected[i] {
				t.Errorf("ExtractDay[%d] = %v, want %v", i, got, expected[i])
			}
		}
	})

	t.Run("ExtractHour", func(t *testing.T) {
		hours := ExtractHour(timestamps, time.UTC)
		expected := []int8{10, 23, 0}
		for i, got := range hours {
			if got != expected[i] {
				t.Errorf("ExtractHour[%d] = %v, want %v", i, got, expected[i])
			}
		}
	})
}

func TestDateComponents(t *testing.T) {
	date := NewDate(2024, 7, 15)

	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"Year", date.Year(), 2024},
		{"Month", date.Month(), time.July},
		{"Day", date.Day(), 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestTimeComponents(t *testing.T) {
	tm := NewTime(14, 30, 45, 123456789)

	tests := []struct {
		name string
		got  int
		want int
	}{
		{"Hour", tm.Hour(), 14},
		{"Minute", tm.Minute(), 30},
		{"Second", tm.Second(), 45},
		{"Nanosecond", tm.Nanosecond(), 123456789},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}