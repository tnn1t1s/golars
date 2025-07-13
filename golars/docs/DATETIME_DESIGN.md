# DateTime Support Design Document

## Overview

This document outlines the design and implementation of DateTime support for Golars, providing comprehensive temporal data handling capabilities similar to Polars. DateTime operations are critical for time series analysis, financial data processing, and general data manipulation tasks.

## Goals

1. **Comprehensive Coverage**: Support all common datetime operations found in Polars
2. **Performance**: Leverage Arrow's temporal types for efficient storage and computation
3. **Timezone Awareness**: Full timezone support with conversion capabilities
4. **Precision**: Support from nanosecond to day precision
5. **Integration**: Seamless integration with DataFrame and expression APIs

## Architecture

### Package Structure

```
datetime/
├── types.go         // Core datetime types and constants
├── parser.go        // Parsing from strings and other formats
├── components.go    // Extract year, month, day, etc.
├── arithmetic.go    // Add/subtract operations
├── timezone.go      // Timezone conversions
├── format.go        // Formatting to strings
├── range.go         // Date range generation
├── business.go      // Business day calculations
├── expr.go          // Expression API integration
├── benchmarks_test.go
└── doc.go
```

### Core Types

```go
// DateTime represents a point in time with nanosecond precision
type DateTime struct {
    // Internal representation using Arrow's timestamp type
    timestamp int64  // nanoseconds since Unix epoch
    timezone  *time.Location
}

// Date represents a calendar date without time
type Date struct {
    // Days since Unix epoch (1970-01-01)
    days int32
}

// Time represents time of day without date
type Time struct {
    // Nanoseconds since midnight
    nanoseconds int64
}

// Duration represents a time duration
type Duration struct {
    // Can represent both absolute (nanoseconds) and relative (months/days)
    months      int32
    days        int32
    nanoseconds int64
}

// TimeUnit represents the precision of temporal data
type TimeUnit int

const (
    Nanosecond TimeUnit = iota
    Microsecond
    Millisecond
    Second
    Minute
    Hour
    Day
)
```

## API Design

### DateTime Series Creation

```go
// From various sources
func NewDateTimeSeries(name string, values []time.Time) Series
func NewDateTimeSeriesFromStrings(name string, values []string, format string) (Series, error)
func NewDateTimeSeriesFromEpoch(name string, values []int64, unit TimeUnit) Series
func NewDateTimeSeriesFromComponents(name string, years, months, days, hours, minutes, seconds []int) Series

// Date series
func NewDateSeries(name string, values []time.Time) Series
func NewDateSeriesFromStrings(name string, values []string, format string) (Series, error)
func NewDateSeriesFromComponents(name string, years, months, days []int) Series

// Time series
func NewTimeSeries(name string, hours, minutes, seconds []int) Series
func NewTimeSeriesFromStrings(name string, values []string, format string) (Series, error)
```

### DateTime Operations

```go
// DateTimeOps provides datetime operations on a Series
type DateTimeOps struct {
    s series.Series
}

// Component extraction
func (dt *DateTimeOps) Year() Series         // Extract year
func (dt *DateTimeOps) Month() Series        // Extract month (1-12)
func (dt *DateTimeOps) Day() Series          // Extract day of month
func (dt *DateTimeOps) Hour() Series         // Extract hour
func (dt *DateTimeOps) Minute() Series       // Extract minute
func (dt *DateTimeOps) Second() Series       // Extract second
func (dt *DateTimeOps) Nanosecond() Series   // Extract nanosecond
func (dt *DateTimeOps) DayOfWeek() Series    // Day of week (0=Sunday)
func (dt *DateTimeOps) DayOfYear() Series    // Day of year (1-366)
func (dt *DateTimeOps) Quarter() Series      // Quarter (1-4)
func (dt *DateTimeOps) WeekOfYear() Series   // ISO week number

// Arithmetic operations
func (dt *DateTimeOps) Add(duration Duration) Series
func (dt *DateTimeOps) Sub(duration Duration) Series
func (dt *DateTimeOps) Diff(other Series) Series  // Returns Duration

// Timezone operations
func (dt *DateTimeOps) WithTimezone(tz string) (Series, error)
func (dt *DateTimeOps) ConvertTimezone(tz string) (Series, error)
func (dt *DateTimeOps) LocalTime() Series
func (dt *DateTimeOps) UTCTime() Series

// Rounding and truncation
func (dt *DateTimeOps) Round(unit TimeUnit) Series
func (dt *DateTimeOps) Floor(unit TimeUnit) Series
func (dt *DateTimeOps) Ceil(unit TimeUnit) Series
func (dt *DateTimeOps) Truncate(unit TimeUnit) Series

// Formatting
func (dt *DateTimeOps) Format(layout string) Series  // Returns string series
func (dt *DateTimeOps) ISOFormat() Series           // ISO 8601 format

// Comparisons and checks
func (dt *DateTimeOps) IsLeapYear() Series     // Boolean series
func (dt *DateTimeOps) IsWeekend() Series      // Saturday or Sunday
func (dt *DateTimeOps) IsMonthStart() Series   // First day of month
func (dt *DateTimeOps) IsMonthEnd() Series     // Last day of month
func (dt *DateTimeOps) IsQuarterStart() Series // First day of quarter
func (dt *DateTimeOps) IsQuarterEnd() Series   // Last day of quarter
func (dt *DateTimeOps) IsYearStart() Series    // First day of year
func (dt *DateTimeOps) IsYearEnd() Series      // Last day of year
```

### Date Range Generation

```go
// Generate date ranges
func DateRange(start, end time.Time, freq Duration) Series
func DateRangeFromString(start, end string, freq string) (Series, error)
func BusinessDayRange(start, end time.Time) Series

// Frequency strings
// "D" - daily
// "B" - business days
// "W" - weekly
// "M" - monthly
// "Q" - quarterly
// "Y" - yearly
// "H" - hourly
// "T" or "min" - minutely
// "S" - secondly
// "ms" - milliseconds
// "us" - microseconds
// "ns" - nanoseconds
```

### Duration Operations

```go
// Duration creation
func Days(n int) Duration
func Hours(n int) Duration
func Minutes(n int) Duration
func Seconds(n int) Duration
func Milliseconds(n int) Duration
func Microseconds(n int) Duration
func Nanoseconds(n int) Duration

// Relative durations
func Months(n int) Duration
func Years(n int) Duration
func Weeks(n int) Duration
```

### Business Day Operations

```go
// Business day calculations
type BusinessDayOps struct {
    s        series.Series
    calendar BusinessCalendar
}

type BusinessCalendar interface {
    IsBusinessDay(date time.Time) bool
    NextBusinessDay(date time.Time) time.Time
    PrevBusinessDay(date time.Time) time.Time
}

func (bd *BusinessDayOps) AddBusinessDays(n int) Series
func (bd *BusinessDayOps) SubBusinessDays(n int) Series
func (bd *BusinessDayOps) BusinessDaysBetween(other Series) Series
func (bd *BusinessDayOps) IsBusinessDay() Series
```

### Expression API Integration

```go
// DateTime expressions
func (e Expr) Dt() *DateTimeExpr

type DateTimeExpr struct {
    expr Expr
}

// All DateTimeOps methods available as expressions
func (dte *DateTimeExpr) Year() Expr
func (dte *DateTimeExpr) Month() Expr
func (dte *DateTimeExpr) Day() Expr
// ... etc

// String to datetime parsing in expressions
func StrToDate(expr Expr, format string) Expr
func StrToDateTime(expr Expr, format string) Expr
func StrToTime(expr Expr, format string) Expr
```

## Implementation Details

### Arrow Integration

```go
// Use Arrow's temporal types for efficient storage
import "github.com/apache/arrow/go/v14/arrow"

// DateTime uses arrow.Timestamp
func newDateTimeChunkedArray(timestamps []int64, unit arrow.TimeUnit) *arrow.TimestampArray

// Date uses arrow.Date32 or Date64
func newDateChunkedArray(days []int32) *arrow.Date32Array

// Time uses arrow.Time32 or Time64
func newTimeChunkedArray(nanoseconds []int64) *arrow.Time64Array
```

### Parsing Implementation

```go
// Flexible parsing with format detection
func parseDateTime(value string) (time.Time, error) {
    // Try common formats in order
    formats := []string{
        time.RFC3339,
        time.RFC3339Nano,
        "2006-01-02 15:04:05",
        "2006-01-02",
        "01/02/2006",
        "02-Jan-2006",
        // ... more formats
    }
    
    for _, format := range formats {
        if t, err := time.Parse(format, value); err == nil {
            return t, nil
        }
    }
    
    // Try parsing with dateparse library for more flexibility
    return dateparse.ParseAny(value)
}

// Custom format parsing
func parseDateTimeWithFormat(value string, format string) (time.Time, error) {
    // Convert Python/Polars format to Go format
    goFormat := convertToGoTimeFormat(format)
    return time.Parse(goFormat, value)
}
```

### Timezone Handling

```go
// Timezone-aware operations
func (dt *DateTime) InTimezone(tz *time.Location) DateTime {
    return DateTime{
        timestamp: dt.timestamp,
        timezone:  tz,
    }
}

func (dt *DateTime) ToTimezone(tz *time.Location) DateTime {
    t := dt.Time()
    converted := t.In(tz)
    return DateTime{
        timestamp: converted.UnixNano(),
        timezone:  tz,
    }
}
```

### Performance Optimizations

```go
// Vectorized operations for component extraction
func extractYear(timestamps []int64, tz *time.Location) []int32 {
    years := make([]int32, len(timestamps))
    
    // Batch processing for same timezone
    if tz == time.UTC {
        for i, ts := range timestamps {
            // Fast path for UTC
            years[i] = int32(time.Unix(0, ts).Year())
        }
    } else {
        for i, ts := range timestamps {
            years[i] = int32(time.Unix(0, ts).In(tz).Year())
        }
    }
    
    return years
}

// Cached timezone lookups
var tzCache = &sync.Map{}

func getTimezone(name string) (*time.Location, error) {
    if cached, ok := tzCache.Load(name); ok {
        return cached.(*time.Location), nil
    }
    
    tz, err := time.LoadLocation(name)
    if err != nil {
        return nil, err
    }
    
    tzCache.Store(name, tz)
    return tz, nil
}
```

## Examples

### Basic DateTime Operations

```go
// Create datetime series
dates := golars.NewDateTimeSeriesFromStrings("timestamp", 
    []string{"2024-01-01 10:30:00", "2024-01-02 14:45:30", "2024-01-03 09:15:00"},
    "2006-01-02 15:04:05")

// Extract components
df.WithColumn("year", golars.Col("timestamp").Dt().Year()).
   WithColumn("month", golars.Col("timestamp").Dt().Month()).
   WithColumn("hour", golars.Col("timestamp").Dt().Hour())

// Arithmetic
tomorrow := golars.Col("timestamp").Dt().Add(golars.Days(1))
nextWeek := golars.Col("timestamp").Dt().Add(golars.Weeks(1))

// Timezone conversion
utc := golars.Col("timestamp").Dt().ConvertTimezone("UTC")
tokyo := golars.Col("timestamp").Dt().ConvertTimezone("Asia/Tokyo")
```

### Date Range Generation

```go
// Daily range
dates := golars.DateRange(
    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
    time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
    golars.Days(1))

// Business days only
businessDays := golars.BusinessDayRange(
    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
    time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC))

// Monthly range
months := golars.DateRangeFromString("2024-01-01", "2024-12-31", "M")
```

### Time Series Resampling

```go
// Resample to daily frequency
daily := df.GroupBy(
    golars.Col("timestamp").Dt().Truncate(golars.Day)).
    Agg(
        golars.Col("value").Mean().Alias("daily_avg"),
        golars.Col("value").Sum().Alias("daily_sum"))

// Resample to hourly
hourly := df.GroupBy(
    golars.Col("timestamp").Dt().Truncate(golars.Hour)).
    Agg(golars.Col("value").Mean())
```

### Business Day Calculations

```go
// Add business days
df.WithColumn("delivery_date",
    golars.Col("order_date").Dt().AddBusinessDays(5))

// Check if date is business day
df.Filter(golars.Col("date").Dt().IsBusinessDay())

// Count business days between dates
df.WithColumn("processing_days",
    golars.Col("end_date").Dt().BusinessDaysBetween(golars.Col("start_date")))
```

## Testing Strategy

### Unit Tests

1. **Parsing Tests**: Various formats, edge cases, invalid inputs
2. **Component Tests**: Verify correct extraction across timezones
3. **Arithmetic Tests**: Add/subtract with different units
4. **Timezone Tests**: Conversion accuracy, DST handling
5. **Range Tests**: Verify correct sequence generation
6. **Business Day Tests**: Holiday handling, weekend skipping

### Integration Tests

1. **DataFrame Integration**: Operations within DataFrames
2. **Expression Integration**: Datetime expressions in select/filter
3. **Performance Tests**: Large-scale operations
4. **Compatibility Tests**: Verify against Polars behavior

### Edge Cases

1. **Leap Years**: Feb 29 handling
2. **DST Transitions**: Spring forward/fall back
3. **Timezone Boundaries**: International date line
4. **Precision**: Nanosecond accuracy
5. **Null Handling**: Propagation through operations

## Implementation Plan

1. **Phase 1**: Core types and basic operations (parsing, components)
2. **Phase 2**: Arithmetic and timezone support
3. **Phase 3**: Range generation and business days
4. **Phase 4**: Expression API integration
5. **Phase 5**: Performance optimization
6. **Phase 6**: Advanced features (resampling, holidays)

## Performance Considerations

### Optimization Strategies

1. **Vectorized Operations**: Process arrays, not individual values
2. **Lazy Timezone Conversion**: Only convert when needed
3. **Component Caching**: Cache extracted components
4. **Batch Processing**: Group operations by timezone
5. **Memory Layout**: Use Arrow's columnar format

### Benchmarks

```go
func BenchmarkDateTimeOperations(b *testing.B) {
    sizes := []int{1000, 10000, 100000}
    
    for _, size := range sizes {
        timestamps := generateTimestamps(size)
        series := NewDateTimeSeries("test", timestamps)
        
        b.Run(fmt.Sprintf("ExtractYear_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = series.Dt().Year()
            }
        })
        
        b.Run(fmt.Sprintf("TimezoneConvert_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = series.Dt().ConvertTimezone("America/New_York")
            }
        })
    }
}
```

## Conclusion

DateTime support is essential for Golars to achieve feature parity with Polars. This design provides comprehensive temporal data handling while maintaining performance and usability. The implementation leverages Go's time package and Arrow's temporal types for efficient operations.