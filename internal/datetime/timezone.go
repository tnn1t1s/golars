package datetime

import (
	"fmt"
	"strings"
	"time"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

// ConvertTimezone converts a DateTime to a different timezone
func (dt DateTime) ConvertTimezone(tz *time.Location) DateTime {
	if tz == nil {
		tz = time.UTC
	}
	
	// Convert to time.Time in current timezone, then to new timezone
	t := dt.Time().In(tz)
	
	return DateTime{
		timestamp: t.UnixNano(),
		timezone:  tz,
	}
}

// WithTimezone returns a new DateTime with the specified timezone
// without changing the underlying timestamp
func (dt DateTime) WithTimezone(tz *time.Location) DateTime {
	if tz == nil {
		tz = time.UTC
	}
	
	return DateTime{
		timestamp: dt.timestamp,
		timezone:  tz,
	}
}

// InTimezone converts the DateTime to the specified timezone
func (dt DateTime) InTimezone(tzName string) (DateTime, error) {
	tz, err := LoadTimezone(tzName)
	if err != nil {
		return dt, err
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
	switch strings.ToUpper(name) {
	case "UTC", "GMT", "Z":
		return time.UTC, nil
	case "LOCAL":
		return time.Local, nil
	case "EST":
		return time.LoadLocation("America/New_York")
	case "CST":
		return time.LoadLocation("America/Chicago")
	case "MST":
		return time.LoadLocation("America/Denver")
	case "PST":
		return time.LoadLocation("America/Los_Angeles")
	default:
		return time.LoadLocation(name)
	}
}

// Series timezone operations

// ConvertTimezone converts all datetime values in the series to a new timezone
func (dts *DateTimeSeries) ConvertTimezone(tzName string) (series.Series, error) {
	tz, err := LoadTimezone(tzName)
	if err != nil {
		return nil, fmt.Errorf("failed to load timezone %s: %w", tzName, err)
	}
	
	name := fmt.Sprintf("%s_tz_%s", dts.s.Name(), tzName)
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		length := dts.s.Len()
		values := make([]int64, length)
		validity := make([]bool, length)
		
		for i := 0; i < length; i++ {
			if dts.s.IsNull(i) {
				validity[i] = false
			} else {
				ts := dts.s.Get(i).(int64)
				dt := DateTime{timestamp: ts, timezone: time.UTC} // Assume UTC if not specified
				converted := dt.ConvertTimezone(tz)
				values[i] = converted.timestamp
				validity[i] = true
			}
		}
		
		// Create new datetime type with timezone info
		newDt := datatypes.Datetime{
			Unit:     datatypes.Nanoseconds,
			TimeZone: tz,
		}
		
		return series.NewSeriesWithValidity(name, values, validity, newDt), nil
		
	default:
		return nil, fmt.Errorf("timezone conversion not supported for type %s", dts.s.DataType())
	}
}

// ToUTC converts all datetime values to UTC
func (dts *DateTimeSeries) ToUTC() series.Series {
	s, _ := dts.ConvertTimezone("UTC")
	return s
}

// ToLocal converts all datetime values to local timezone
func (dts *DateTimeSeries) ToLocal() series.Series {
	s, _ := dts.ConvertTimezone("Local")
	return s
}

// Localize interprets naive timestamps as being in the specified timezone
func (dts *DateTimeSeries) Localize(tzName string) (series.Series, error) {
	tz, err := LoadTimezone(tzName)
	if err != nil {
		return nil, fmt.Errorf("failed to load timezone %s: %w", tzName, err)
	}
	
	name := fmt.Sprintf("%s_localized_%s", dts.s.Name(), tzName)
	
	switch dt := dts.s.DataType().(type) {
	case datatypes.Datetime:
		// Simply update the timezone metadata without changing timestamps
		newDt := datatypes.Datetime{
			Unit:     dt.Unit,
			TimeZone: tz,
		}
		
		// For localize, we keep the same timestamps but update metadata
		// Since we can't directly modify the datatype, we'll create a new series
		// with the same values but new datatype
		length := dts.s.Len()
		values := make([]int64, length)
		validity := make([]bool, length)
		
		for i := 0; i < length; i++ {
			if dts.s.IsNull(i) {
				validity[i] = false
			} else {
				values[i] = dts.s.Get(i).(int64)
				validity[i] = true
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, newDt), nil
		
	default:
		return nil, fmt.Errorf("localize not supported for type %s", dts.s.DataType())
	}
}

// GetTimezone returns the timezone of the datetime series
func (dts *DateTimeSeries) GetTimezone() string {
	switch dt := dts.s.DataType().(type) {
	case datatypes.Datetime:
		if dt.TimeZone == nil {
			return "UTC"
		}
		return dt.TimeZone.String()
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
	return fmt.Sprintf("%s.dt.convert_timezone(%q)", e.expr.String(), e.timezone)
}

func (e *TimezoneConvertExpr) DataType() datatypes.DataType {
	// Parse timezone to get location
	tz, _ := LoadTimezone(e.timezone)
	return datatypes.Datetime{Unit: datatypes.Nanoseconds, TimeZone: tz}
}

func (e *TimezoneConvertExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *TimezoneConvertExpr) IsColumn() bool {
	return false
}

func (e *TimezoneConvertExpr) Name() string {
	return fmt.Sprintf("%s_tz_%s", e.expr.Name(), e.timezone)
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