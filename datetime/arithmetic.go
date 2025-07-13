package datetime

import (
	"fmt"
	"time"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

// DateTimeArithmetic provides arithmetic operations for DateTime series
type DateTimeArithmetic struct {
	s series.Series
}

// Add adds a duration to each datetime value
func (dts *DateTimeSeries) Add(duration Duration) series.Series {
	name := fmt.Sprintf("%s_plus_%s", dts.s.Name(), duration.String())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return addDurationToDateTime(dts.s, name, duration)
	case datatypes.Date:
		return addDurationToDate(dts.s, name, duration)
	default:
		return series.NewSeries(name, []int64{}, dts.s.DataType())
	}
}

// Sub subtracts a duration from each datetime value
func (dts *DateTimeSeries) Sub(duration Duration) series.Series {
	return dts.Add(duration.Negate())
}

// Diff calculates the difference between two datetime series
func (dts *DateTimeSeries) Diff(other series.Series) (series.Series, error) {
	if dts.s.Len() != other.Len() {
		return nil, fmt.Errorf("series lengths must match: %d != %d", dts.s.Len(), other.Len())
	}
	
	// Check that other is also a datetime type
	switch other.DataType().(type) {
	case datatypes.Datetime, datatypes.Date:
		// OK
	default:
		return nil, fmt.Errorf("can only diff with datetime or date series, got %s", other.DataType())
	}
	
	name := fmt.Sprintf("%s_diff_%s", dts.s.Name(), other.Name())
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return diffDateTime(dts.s, other, name)
	case datatypes.Date:
		return diffDate(dts.s, other, name)
	default:
		return nil, fmt.Errorf("unsupported type for diff: %s", dts.s.DataType())
	}
}

// AddBusinessDays adds business days to datetime/date series
func (dts *DateTimeSeries) AddBusinessDays(days int) series.Series {
	name := fmt.Sprintf("%s_plus_%d_business_days", dts.s.Name(), days)
	
	switch dts.s.DataType().(type) {
	case datatypes.Datetime:
		return addBusinessDaysToDateTime(dts.s, name, days)
	case datatypes.Date:
		return addBusinessDaysToDate(dts.s, name, days)
	default:
		return series.NewSeries(name, []int64{}, dts.s.DataType())
	}
}

// Helper functions

func addDurationToDateTime(s series.Series, name string, duration Duration) series.Series {
	length := s.Len()
	values := make([]int64, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			ts := s.Get(i).(int64)
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			newDt := dt.Add(duration)
			values[i] = newDt.timestamp
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, s.DataType())
}

func addDurationToDate(s series.Series, name string, duration Duration) series.Series {
	length := s.Len()
	values := make([]int32, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			days := s.Get(i).(int32)
			date := Date{days: days}
			t := date.Time()
			
			// Add duration components
			if duration.months != 0 {
				t = t.AddDate(0, int(duration.months), 0)
			}
			if duration.days != 0 {
				t = t.AddDate(0, 0, int(duration.days))
			}
			if duration.nanoseconds != 0 {
				t = t.Add(time.Duration(duration.nanoseconds))
			}
			
			newDate := NewDateFromTime(t)
			values[i] = newDate.days
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Date{})
}

func diffDateTime(s1, s2 series.Series, name string) (series.Series, error) {
	length := s1.Len()
	values := make([]int64, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s1.IsNull(i) || s2.IsNull(i) {
			validity[i] = false
		} else {
			ts1 := s1.Get(i).(int64)
			var ts2 int64
			
			switch s2.DataType().(type) {
			case datatypes.Datetime:
				ts2 = s2.Get(i).(int64)
			case datatypes.Date:
				days := s2.Get(i).(int32)
				date := Date{days: days}
				ts2 = date.Time().UnixNano()
			}
			
			// Difference in nanoseconds
			values[i] = ts1 - ts2
			validity[i] = true
		}
	}
	
	// Return as Duration type
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Duration{Unit: datatypes.Nanoseconds}), nil
}

func diffDate(s1, s2 series.Series, name string) (series.Series, error) {
	length := s1.Len()
	values := make([]int32, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s1.IsNull(i) || s2.IsNull(i) {
			validity[i] = false
		} else {
			days1 := s1.Get(i).(int32)
			var days2 int32
			
			switch s2.DataType().(type) {
			case datatypes.Date:
				days2 = s2.Get(i).(int32)
			case datatypes.Datetime:
				ts := s2.Get(i).(int64)
				dt := DateTime{timestamp: ts, timezone: time.UTC}
				date := NewDateFromTime(dt.Time())
				days2 = date.days
			}
			
			// Difference in days
			values[i] = days1 - days2
			validity[i] = true
		}
	}
	
	// Return as days (Int32)
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{}), nil
}

func addBusinessDaysToDateTime(s series.Series, name string, days int) series.Series {
	length := s.Len()
	values := make([]int64, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			ts := s.Get(i).(int64)
			dt := DateTime{timestamp: ts, timezone: time.UTC}
			t := dt.Time()
			
			// Add business days
			daysToAdd := days
			direction := 1
			if daysToAdd < 0 {
				direction = -1
				daysToAdd = -daysToAdd
			}
			
			for daysToAdd > 0 {
				t = t.AddDate(0, 0, direction)
				weekday := t.Weekday()
				if weekday != time.Saturday && weekday != time.Sunday {
					daysToAdd--
				}
			}
			
			values[i] = t.UnixNano()
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, s.DataType())
}

func addBusinessDaysToDate(s series.Series, name string, days int) series.Series {
	length := s.Len()
	values := make([]int32, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else {
			daysSinceEpoch := s.Get(i).(int32)
			date := Date{days: daysSinceEpoch}
			t := date.Time()
			
			// Add business days
			daysToAdd := days
			direction := 1
			if daysToAdd < 0 {
				direction = -1
				daysToAdd = -daysToAdd
			}
			
			for daysToAdd > 0 {
				t = t.AddDate(0, 0, direction)
				weekday := t.Weekday()
				if weekday != time.Saturday && weekday != time.Sunday {
					daysToAdd--
				}
			}
			
			newDate := NewDateFromTime(t)
			values[i] = newDate.days
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Date{})
}

// Series arithmetic operations for expressions

// DateTimeAddExpr represents adding a duration to a datetime expression
type DateTimeAddExpr struct {
	expr     expr.Expr
	duration Duration
}

func (e *DateTimeAddExpr) String() string {
	return fmt.Sprintf("%s + %s", e.expr.String(), e.duration.String())
}

func (e *DateTimeAddExpr) DataType() datatypes.DataType {
	return e.expr.DataType()
}

func (e *DateTimeAddExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeAddExpr) IsColumn() bool {
	return false
}

func (e *DateTimeAddExpr) Name() string {
	return fmt.Sprintf("%s_plus_%s", e.expr.Name(), e.duration.String())
}

// Add adds a duration to the datetime expression
func (dte *DateTimeExpr) Add(duration Duration) expr.Expr {
	return &DateTimeAddExpr{
		expr:     dte.expr,
		duration: duration,
	}
}

// DateTimeSubExpr represents subtracting a duration from a datetime expression
type DateTimeSubExpr struct {
	expr     expr.Expr
	duration Duration
}

func (e *DateTimeSubExpr) String() string {
	return fmt.Sprintf("%s - %s", e.expr.String(), e.duration.String())
}

func (e *DateTimeSubExpr) DataType() datatypes.DataType {
	return e.expr.DataType()
}

func (e *DateTimeSubExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeSubExpr) IsColumn() bool {
	return false
}

func (e *DateTimeSubExpr) Name() string {
	return fmt.Sprintf("%s_minus_%s", e.expr.Name(), e.duration.String())
}

// Sub subtracts a duration from the datetime expression
func (dte *DateTimeExpr) Sub(duration Duration) expr.Expr {
	return &DateTimeSubExpr{
		expr:     dte.expr,
		duration: duration,
	}
}

// DateTimeDiffExpr represents the difference between two datetime expressions
type DateTimeDiffExpr struct {
	left  expr.Expr
	right expr.Expr
}

func (e *DateTimeDiffExpr) String() string {
	return fmt.Sprintf("%s - %s", e.left.String(), e.right.String())
}

func (e *DateTimeDiffExpr) DataType() datatypes.DataType {
	// Returns duration for datetime diff, int32 for date diff
	leftType := e.left.DataType()
	
	// If left is a column expression, assume it's datetime by default
	if _, ok := leftType.(datatypes.Unknown); ok {
		return datatypes.Duration{Unit: datatypes.Nanoseconds}
	}
	
	switch leftType.(type) {
	case datatypes.Datetime:
		return datatypes.Duration{Unit: datatypes.Nanoseconds}
	case datatypes.Date:
		return datatypes.Int32{}
	default:
		// Default to Duration for datetime operations
		return datatypes.Duration{Unit: datatypes.Nanoseconds}
	}
}

func (e *DateTimeDiffExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeDiffExpr) IsColumn() bool {
	return false
}

func (e *DateTimeDiffExpr) Name() string {
	return fmt.Sprintf("%s_diff_%s", e.left.Name(), e.right.Name())
}

// Diff calculates the difference with another datetime expression
func (dte *DateTimeExpr) Diff(other expr.Expr) expr.Expr {
	return &DateTimeDiffExpr{
		left:  dte.expr,
		right: other,
	}
}