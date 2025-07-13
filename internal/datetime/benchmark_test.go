package datetime

import (
	"testing"
	"time"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Benchmark parsing operations

func BenchmarkParseDateTime(b *testing.B) {
	testCases := []struct {
		name   string
		input  string
		format string
	}{
		{"ISO8601", "2024-01-15T10:30:45Z", ""},
		{"CustomFormat", "2024-01-15 10:30:45", "%Y-%m-%d %H:%M:%S"},
		{"USFormat", "01/15/2024", ""},
		{"MonthName", "15 Jan 2024", ""},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tc.format == "" {
					_, _ = ParseDateTime(tc.input)
				} else {
					_, _ = ParseDateTimeWithFormat(tc.input, tc.format)
				}
			}
		})
	}
}

func BenchmarkParseEpoch(b *testing.B) {
	units := []TimeUnit{Second, Millisecond, Microsecond, Nanosecond}
	epoch := time.Now().Unix()
	
	for _, unit := range units {
		b.Run(unit.String(), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ParseDateTimeFromEpoch(epoch, unit)
			}
		})
	}
}

// Benchmark component extraction

func BenchmarkComponentExtraction(b *testing.B) {
	// Create test datetime
	dt := NewDateTime(time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC))
	
	b.Run("Year", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dt.Year()
		}
	})
	
	b.Run("Month", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dt.Month()
		}
	})
	
	b.Run("Day", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dt.Day()
		}
	})
	
	b.Run("Hour", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dt.Hour()
		}
	})
	
	b.Run("DayOfWeek", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dt.DayOfWeek()
		}
	})
	
	b.Run("IsLeapYear", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dt.IsLeapYear()
		}
	})
}

// Benchmark series operations

func BenchmarkSeriesCreation(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	
	for _, size := range sizes {
		times := make([]time.Time, size)
		base := time.Now()
		for i := 0; i < size; i++ {
			times[i] = base.Add(time.Duration(i) * time.Hour)
		}
		
		b.Run("Size="+string(rune(size)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NewDateTimeSeries("bench", times)
			}
		})
	}
}

func BenchmarkSeriesComponentExtraction(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	
	for _, size := range sizes {
		times := make([]time.Time, size)
		base := time.Now()
		for i := 0; i < size; i++ {
			times[i] = base.Add(time.Duration(i) * time.Hour)
		}
		
		s := NewDateTimeSeries("bench", times)
		dts, _ := DtSeries(s)
		
		b.Run("Year/Size="+string(rune(size)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = dts.Year()
			}
		})
		
		b.Run("Month/Size="+string(rune(size)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = dts.Month()
			}
		})
		
		b.Run("IsWeekend/Size="+string(rune(size)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = dts.IsWeekend()
			}
		})
	}
}

// Benchmark arithmetic operations

func BenchmarkArithmetic(b *testing.B) {
	size := 10000
	times := make([]time.Time, size)
	base := time.Now()
	for i := 0; i < size; i++ {
		times[i] = base.Add(time.Duration(i) * time.Hour)
	}
	
	s := NewDateTimeSeries("bench", times)
	dts, _ := DtSeries(s)
	
	b.Run("Add_Duration", func(b *testing.B) {
		dur := Days(7)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = dts.Add(dur)
		}
	})
	
	b.Run("Add_BusinessDays", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = dts.AddBusinessDays(5)
		}
	})
	
	b.Run("Diff", func(b *testing.B) {
		times2 := make([]time.Time, size)
		for i := 0; i < size; i++ {
			times2[i] = times[i].Add(24 * time.Hour)
		}
		s2 := NewDateTimeSeries("bench2", times2)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dts.Diff(s2)
		}
	})
}

// Benchmark formatting

func BenchmarkFormatting(b *testing.B) {
	dt := NewDateTime(time.Now())
	
	formats := []struct {
		name   string
		format string
	}{
		{"ISO", ""},
		{"YMD", "%Y-%m-%d"},
		{"Full", "%Y-%m-%d %H:%M:%S"},
		{"Complex", "%a, %d %b %Y %H:%M:%S %Z"},
	}
	
	for _, f := range formats {
		b.Run(f.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = dt.Format(f.format)
			}
		})
	}
}

func BenchmarkSeriesFormatting(b *testing.B) {
	size := 10000
	times := make([]time.Time, size)
	base := time.Now()
	for i := 0; i < size; i++ {
		times[i] = base.Add(time.Duration(i) * time.Hour)
	}
	
	s := NewDateTimeSeries("bench", times)
	dts, _ := DtSeries(s)
	
	b.Run("ISO_Format", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = dts.Format("")
		}
	})
	
	b.Run("Custom_Format", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = dts.Format("%Y-%m-%d %H:%M:%S")
		}
	})
}

// Benchmark rounding operations

func BenchmarkRounding(b *testing.B) {
	size := 10000
	times := make([]time.Time, size)
	base := time.Now()
	for i := 0; i < size; i++ {
		times[i] = base.Add(time.Duration(i) * time.Second)
	}
	
	s := NewDateTimeSeries("bench", times)
	dts, _ := DtSeries(s)
	
	units := []TimeUnit{Minute, Hour, Day}
	
	for _, unit := range units {
		b.Run("Floor_"+unit.String(), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = dts.Floor(unit)
			}
		})
	}
}

// Benchmark timezone operations

func BenchmarkTimezone(b *testing.B) {
	size := 10000
	times := make([]time.Time, size)
	base := time.Now()
	for i := 0; i < size; i++ {
		times[i] = base.Add(time.Duration(i) * time.Hour)
	}
	
	s := NewDateTimeSeries("bench", times)
	dts, _ := DtSeries(s)
	
	b.Run("ConvertTimezone", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dts.ConvertTimezone("America/New_York")
		}
	})
	
	b.Run("ToUTC", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = dts.ToUTC()
		}
	})
}

// Benchmark resampling

func BenchmarkResample(b *testing.B) {
	// Create hourly data for a month
	size := 24 * 30 // 30 days
	times := make([]time.Time, size)
	values := make([]float64, size)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	for i := 0; i < size; i++ {
		times[i] = base.Add(time.Duration(i) * time.Hour)
		values[i] = float64(i)
	}
	
	timeSeries := NewDateTimeSeries("timestamp", times)
	valueSeries := series.NewSeries("value", values, datatypes.Float64{})
	dts, _ := DtSeries(timeSeries)
	
	frequencies := []string{"6H", "1D", "1W"}
	
	for _, freq := range frequencies {
		b.Run("Freq="+freq, func(b *testing.B) {
			rule := NewResampleRule(freq)
			resampler, _ := dts.Resample(rule)
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = resampler.Sum(valueSeries)
			}
		})
	}
}

// Benchmark null handling

func BenchmarkNullHandling(b *testing.B) {
	size := 10000
	values := make([]string, size)
	
	// Create 20% nulls
	for i := 0; i < size; i++ {
		if i%5 == 0 {
			values[i] = ""
		} else {
			values[i] = "2024-01-15 10:30:45"
		}
	}
	
	b.Run("ParseWithNulls", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = NewDateTimeSeriesFromStrings("bench", values, "")
		}
	})
	
	s, _ := NewDateTimeSeriesFromStrings("bench", values, "")
	dts, _ := DtSeries(s)
	
	b.Run("ComponentExtractionWithNulls", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = dts.Year()
		}
	})
}