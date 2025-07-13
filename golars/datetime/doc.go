// Package datetime provides comprehensive temporal data handling for Golars.
// It includes support for dates, times, timestamps, durations, timezones,
// and various calendar operations that integrate seamlessly with the DataFrame
// and Series APIs.
//
// The package supports nanosecond precision timestamps, timezone-aware operations,
// business day calculations, and flexible parsing/formatting capabilities.
//
// Example:
//
//	df := golars.NewDataFrame(
//	    golars.NewDateTimeSeries("timestamp", []time.Time{
//	        time.Now(),
//	        time.Now().Add(24 * time.Hour),
//	        time.Now().Add(48 * time.Hour),
//	    }),
//	)
//	
//	result := df.WithColumn("year",
//	    golars.Col("timestamp").Dt().Year(),
//	).WithColumn("is_weekend",
//	    golars.Col("timestamp").Dt().IsWeekend(),
//	)
package datetime