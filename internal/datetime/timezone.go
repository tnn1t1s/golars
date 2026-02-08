package datetime

import (
	"fmt"
	"time"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// ConvertTimezone converts a DateTime to a different timezone
func (dt DateTime) ConvertTimezone(tz *time.Location) DateTime {
	// Convert to time.Time in current timezone, then to new timezone
	// The underlying timestamp stays the same, just the timezone changes
	return DateTime{
		timestamp: dt.timestamp,
		timezone:  tz,
	}
}

// WithTimezone returns a new DateTime with the specified timezone
// without changing the underlying timestamp
func (dt DateTime) WithTimezone(tz *time.Location) DateTime {
	return DateTime{
		timestamp: dt.timestamp,
		timezone:  tz,
	}
}

// InTimezone converts the DateTime to the specified timezone
func (dt DateTime) InTimezone(tzName string) (DateTime, error) {
	tz, err := LoadTimezone(tzName)
	if err != nil {
		return DateTime{}, err
	}
	return dt.ConvertTimezone(tz), nil
}

// ToUTC converts the DateTime to UTC
func (dt DateTime) ToUTC() DateTime {
	return dt.ConvertTimezone(time.UTC)
}

// ToLocal converts the DateTime to the local timezone
func (dt DateTime) ToLocal() DateTime {
	return dt.ConvertTimezone(time.Local)
}

// LoadTimezone loads a timezone by name
func LoadTimezone(name string) (*time.Location, error) {
	// Handle common timezone aliases
	switch name {
	case "UTC":
		return time.UTC, nil
	case "Local":
		return time.Local, nil
	case "GMT":
		return time.UTC, nil
	case "EST":
		return time.FixedZone("EST", -5*60*60), nil
	case "PST":
		return time.FixedZone("PST", -8*60*60), nil
	case "CST":
		return time.FixedZone("CST", -6*60*60), nil
	case "MST":
		return time.FixedZone("MST", -7*60*60), nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("failed to load timezone %q: %w", name, err)
	}
	return loc, nil
}

// Series timezone operations

// ConvertTimezone converts all datetime values in the series to a new timezone
func (dts *DateTimeSeries) ConvertTimezone(tzName string) (series.Series, error) {
	tz, err := LoadTimezone(tzName)
	if err != nil {
		return nil, fmt.Errorf("failed to load timezone %q: %w", tzName, err)
	}

	name := dts.s.Name() + "_tz_" + tzName
	n := dts.s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)

	for i := 0; i < n; i++ {
		if dts.s.IsNull(i) {
			validity[i] = false
			continue
		}
		ts := dts.s.Get(i).(int64)
		values[i] = ts
		validity[i] = true
	}

	// Create new datetime type with timezone info
	dt := datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: tz}
	return series.NewSeriesWithValidity(name, values, validity, dt), nil
}

// ToUTC converts all datetime values to UTC
func (dts *DateTimeSeries) ToUTC() series.Series {
	s, _ := dts.ConvertTimezone("UTC")
	return s
}

// ToLocal converts all datetime values to local timezone
func (dts *DateTimeSeries) ToLocal() series.Series {
	name := dts.s.Name() + "_tz_Local"
	n := dts.s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)

	for i := 0; i < n; i++ {
		if dts.s.IsNull(i) {
			validity[i] = false
			continue
		}
		values[i] = dts.s.Get(i).(int64)
		validity[i] = true
	}

	dt := datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: time.Local}
	return series.NewSeriesWithValidity(name, values, validity, dt)
}

// Localize interprets naive timestamps as being in the specified timezone
func (dts *DateTimeSeries) Localize(tzName string) (series.Series, error) {
	tz, err := LoadTimezone(tzName)
	if err != nil {
		return nil, fmt.Errorf("failed to load timezone %q: %w", tzName, err)
	}

	name := dts.s.Name() + "_localized_" + tzName
	n := dts.s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)

	// Simply update the timezone metadata without changing timestamps
	for i := 0; i < n; i++ {
		if dts.s.IsNull(i) {
			validity[i] = false
			continue
		}
		values[i] = dts.s.Get(i).(int64)
		validity[i] = true
	}

	// For localize, we keep the same timestamps but update metadata
	dt := datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: tz}
	return series.NewSeriesWithValidity(name, values, validity, dt), nil
}

// GetTimezone returns the timezone of the datetime series
func (dts *DateTimeSeries) GetTimezone() string {
	switch dt := dts.s.DataType().(type) {
	case datatypes.Datetime:
		if dt.TimeZone != nil {
			return dt.TimeZone.String()
		}
		return "UTC"
	default:
		return ""
	}
}

// Expression API for timezone operations

// TimezoneConvertExpr converts datetime to a new timezone
type TimezoneConvertExpr struct {
	expr     expr.Expr
	timezone string
}

func (e *TimezoneConvertExpr) String() string {
	return fmt.Sprintf(`%s.dt.convert_timezone("%s")`, e.expr.String(), e.timezone)
}

func (e *TimezoneConvertExpr) DataType() datatypes.DataType {
	tz, err := LoadTimezone(e.timezone)
	if err != nil {
		return datatypes.Datetime{Unit: datatypes.Nanoseconds}
	}
	return datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: tz}
}

func (e *TimezoneConvertExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *TimezoneConvertExpr) IsColumn() bool {
	return false
}

func (e *TimezoneConvertExpr) Name() string {
	return e.expr.Name() + "_tz_" + e.timezone
}

// ConvertTimezone creates an expression to convert datetime to a new timezone
func (dte *DateTimeExpr) ConvertTimezone(tzName string) expr.Expr {
	return &TimezoneConvertExpr{
		expr:     dte.expr,
		timezone: tzName,
	}
}

// ToUTC creates an expression to convert datetime to UTC
func (dte *DateTimeExpr) ToUTC() expr.Expr {
	return dte.ConvertTimezone("UTC")
}

// ToLocal creates an expression to convert datetime to local timezone
func (dte *DateTimeExpr) ToLocal() expr.Expr {
	return dte.ConvertTimezone("Local")
}
