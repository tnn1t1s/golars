package datetime

import (
	"fmt"
	"time"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// DateTimeArithmetic provides arithmetic operations for DateTime series
type DateTimeArithmetic struct {
	s series.Series
}

// Add adds a duration to each datetime value
func (dts *DateTimeSeries) Add(duration Duration) series.Series {
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return addDurationToDate(dts.s, dts.s.Name()+"_plus_"+duration.String(), duration)
	default:
		return addDurationToDateTime(dts.s, dts.s.Name()+"_plus_"+duration.String(), duration)
	}
}

// Sub subtracts a duration from each datetime value
func (dts *DateTimeSeries) Sub(duration Duration) series.Series {
	negated := duration.Negate()
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return addDurationToDate(dts.s, dts.s.Name()+"_minus_"+duration.String(), negated)
	default:
		return addDurationToDateTime(dts.s, dts.s.Name()+"_minus_"+duration.String(), negated)
	}
}

// Diff calculates the difference between two datetime series
func (dts *DateTimeSeries) Diff(other series.Series) (series.Series, error) {
	if dts.s.Len() != other.Len() {
		return nil, fmt.Errorf("series length mismatch: %d vs %d", dts.s.Len(), other.Len())
	}
	name := dts.s.Name() + "_diff_" + other.Name()

	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return diffDate(dts.s, other, name)
	default:
		return diffDateTime(dts.s, other, name)
	}
}

// AddBusinessDays adds business days to datetime/date series
func (dts *DateTimeSeries) AddBusinessDays(days int) series.Series {
	name := fmt.Sprintf("%s_plus_%d_business_days", dts.s.Name(), days)
	switch dts.s.DataType().(type) {
	case datatypes.Date:
		return addBusinessDaysToDate(dts.s, name, days)
	default:
		return addBusinessDaysToDateTime(dts.s, name, days)
	}
}

// Helper functions

func addDurationToDateTime(s series.Series, name string, duration Duration) series.Series {
	n := s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		ts := s.Get(i).(int64)
		dt := DateTime{timestamp: ts, timezone: time.UTC}
		result := dt.Add(duration)
		values[i] = result.timestamp
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, s.DataType())
}

func addDurationToDate(s series.Series, name string, duration Duration) series.Series {
	n := s.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		days := s.Get(i).(int32)
		d := Date{days: days}
		t := d.Time()
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
		values[i] = int32(t.Sub(epoch).Hours() / 24)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Date{})
}

func diffDateTime(s1, s2 series.Series, name string) (series.Series, error) {
	n := s1.Len()
	values := make([]int64, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s1.IsNull(i) || s2.IsNull(i) {
			validity[i] = false
			continue
		}
		ts1 := s1.Get(i).(int64)
		ts2 := s2.Get(i).(int64)
		// Difference in nanoseconds
		values[i] = ts1 - ts2
		validity[i] = true
	}
	// Return as Duration type
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Duration{Unit: datatypes.Nanoseconds}), nil
}

func diffDate(s1, s2 series.Series, name string) (series.Series, error) {
	n := s1.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s1.IsNull(i) || s2.IsNull(i) {
			validity[i] = false
			continue
		}
		d1 := s1.Get(i).(int32)
		d2 := s2.Get(i).(int32)
		// Difference in days
		values[i] = d1 - d2
		validity[i] = true
	}
	// Return as days (Int32)
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int32{}), nil
}

func addBusinessDaysToDateTime(s series.Series, name string, days int) series.Series {
	n := s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		ts := s.Get(i).(int64)
		t := time.Unix(0, ts).UTC()
		t = addBusinessDaysToTime(t, days)
		values[i] = t.UnixNano()
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, s.DataType())
}

func addBusinessDaysToDate(s series.Series, name string, days int) series.Series {
	n := s.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			validity[i] = false
			continue
		}
		d := s.Get(i).(int32)
		t := Date{days: d}.Time()
		t = addBusinessDaysToTime(t, days)
		values[i] = int32(t.Sub(epoch).Hours() / 24)
		validity[i] = true
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Date{})
}

func addBusinessDaysToTime(t time.Time, days int) time.Time {
	direction := 1
	if days < 0 {
		direction = -1
		days = -days
	}
	added := 0
	for added < days {
		t = t.AddDate(0, 0, direction)
		wd := t.Weekday()
		if wd != time.Saturday && wd != time.Sunday {
			added++
		}
	}
	return t
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
	return datatypes.Datetime{Unit: datatypes.Nanoseconds}
}

func (e *DateTimeAddExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeAddExpr) IsColumn() bool {
	return false
}

func (e *DateTimeAddExpr) Name() string {
	return e.expr.Name() + "_plus_" + e.duration.String()
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
	return datatypes.Datetime{Unit: datatypes.Nanoseconds}
}

func (e *DateTimeSubExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeSubExpr) IsColumn() bool {
	return false
}

func (e *DateTimeSubExpr) Name() string {
	return e.expr.Name() + "_minus_" + e.duration.String()
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
	return datatypes.Duration{Unit: datatypes.Nanoseconds}
}

func (e *DateTimeDiffExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeDiffExpr) IsColumn() bool {
	return false
}

func (e *DateTimeDiffExpr) Name() string {
	return e.left.Name() + "_diff_" + e.right.Name()
}

// Diff calculates the difference with another datetime expression
func (dte *DateTimeExpr) Diff(other expr.Expr) expr.Expr {
	return &DateTimeDiffExpr{
		left:  dte.expr,
		right: other,
	}
}
