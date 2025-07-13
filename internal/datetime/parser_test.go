package datetime

import (
	"testing"
	"time"
)

func TestParseDateTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "RFC3339",
			input: "2024-01-15T10:30:45Z",
			want:  time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			name:  "RFC3339Nano",
			input: "2024-01-15T10:30:45.123456789Z",
			want:  time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC),
		},
		{
			name:  "DateTime with space",
			input: "2024-01-15 10:30:45",
			want:  time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			name:  "Date only",
			input: "2024-01-15",
			want:  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "US format",
			input: "01/15/2024",
			want:  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "Month name",
			input: "15-Jan-2024",
			want:  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "Full month name",
			input: "January 15, 2024",
			want:  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "Empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "Invalid format",
			input:   "not a date",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := ParseDateTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDateTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !dt.Time().Equal(tt.want) {
				t.Errorf("ParseDateTime() = %v, want %v", dt.Time(), tt.want)
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Date
		wantErr bool
	}{
		{
			name:  "ISO format",
			input: "2024-01-15",
			want:  NewDate(2024, 1, 15),
		},
		{
			name:  "US format",
			input: "01/15/2024",
			want:  NewDate(2024, 1, 15),
		},
		{
			name:  "Month name",
			input: "15-Jan-2024",
			want:  NewDate(2024, 1, 15),
		},
		{
			name:    "Empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.days != tt.want.days {
				t.Errorf("ParseDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Time
		wantErr bool
	}{
		{
			name:  "Full time",
			input: "10:30:45",
			want:  NewTime(10, 30, 45, 0),
		},
		{
			name:  "Time with milliseconds",
			input: "10:30:45.123",
			want:  NewTime(10, 30, 45, 123000000),
		},
		{
			name:  "Time with nanoseconds",
			input: "10:30:45.123456789",
			want:  NewTime(10, 30, 45, 123456789),
		},
		{
			name:  "12-hour format",
			input: "10:30:45 AM",
			want:  NewTime(10, 30, 45, 0),
		},
		{
			name:  "12-hour PM",
			input: "10:30:45 PM",
			want:  NewTime(22, 30, 45, 0),
		},
		{
			name:    "Empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.nanoseconds != tt.want.nanoseconds {
				t.Errorf("ParseTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Duration
		wantErr bool
	}{
		{
			name:  "Hours",
			input: "2h",
			want:  Hours(2),
		},
		{
			name:  "Minutes",
			input: "30m",
			want:  Minutes(30),
		},
		{
			name:  "Seconds",
			input: "45s",
			want:  Seconds(45),
		},
		{
			name:  "Milliseconds",
			input: "500ms",
			want:  Milliseconds(500),
		},
		{
			name:  "Days",
			input: "7d",
			want:  Days(7),
		},
		{
			name:  "Weeks",
			input: "2w",
			want:  Weeks(2),
		},
		{
			name:  "Months",
			input: "3mo",
			want:  Months(3),
		},
		{
			name:  "Years",
			input: "1y",
			want:  Years(1),
		},
		{
			name:  "Combined",
			input: "1y 2mo 3d",
			want: Duration{
				months:      14,
				days:        3,
				nanoseconds: 0,
			},
		},
		{
			name:    "Empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDuration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.months != tt.want.months || got.days != tt.want.days || got.nanoseconds != tt.want.nanoseconds {
					t.Errorf("ParseDuration() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestParseWithFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		format  string
		want    time.Time
		wantErr bool
	}{
		{
			name:   "Python format - date",
			input:  "2024-01-15",
			format: "%Y-%m-%d",
			want:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "Python format - datetime",
			input:  "2024-01-15 10:30:45",
			format: "%Y-%m-%d %H:%M:%S",
			want:   time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			name:   "Month name",
			input:  "15-Jan-2024",
			format: "%d-%b-%Y",
			want:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "12-hour format",
			input:  "01/15/2024 10:30 PM",
			format: "%m/%d/%Y %I:%M %p",
			want:   time.Date(2024, 1, 15, 22, 30, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := ParseDateTimeWithFormat(tt.input, tt.format)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDateTimeWithFormat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !dt.Time().Equal(tt.want) {
				t.Errorf("ParseDateTimeWithFormat() = %v, want %v", dt.Time(), tt.want)
			}
		})
	}
}

func TestParseEpoch(t *testing.T) {
	tests := []struct {
		name  string
		epoch int64
		unit  TimeUnit
		want  time.Time
	}{
		{
			name:  "Seconds",
			epoch: 1705318245,
			unit:  Second,
			want:  time.Date(2024, 1, 15, 11, 30, 45, 0, time.UTC),
		},
		{
			name:  "Milliseconds",
			epoch: 1705318245123,
			unit:  Millisecond,
			want:  time.Date(2024, 1, 15, 11, 30, 45, 123000000, time.UTC),
		},
		{
			name:  "Microseconds",
			epoch: 1705318245123456,
			unit:  Microsecond,
			want:  time.Date(2024, 1, 15, 11, 30, 45, 123456000, time.UTC),
		},
		{
			name:  "Nanoseconds",
			epoch: 1705318245123456789,
			unit:  Nanosecond,
			want:  time.Date(2024, 1, 15, 11, 30, 45, 123456789, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := ParseDateTimeFromEpoch(tt.epoch, tt.unit)
			if !dt.Time().Equal(tt.want) {
				t.Errorf("ParseDateTimeFromEpoch() = %v, want %v", dt.Time(), tt.want)
			}
		})
	}
}