package datetime

import (
	_ "fmt"
	_ "time"

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
	panic("not implemented")

}

// Sub subtracts a duration from each datetime value
func (dts *DateTimeSeries) Sub(duration Duration) series.Series {
	panic("not implemented")

}

// Diff calculates the difference between two datetime series
func (dts *DateTimeSeries) Diff(other series.Series) (series.Series, error) {
	panic("not implemented")

	// Check that other is also a datetime type

	// OK

}

// AddBusinessDays adds business days to datetime/date series
func (dts *DateTimeSeries) AddBusinessDays(days int) series.Series {
	panic("not implemented")

}

// Helper functions

func addDurationToDateTime(s series.Series, name string, duration Duration) series.Series {
	panic("not implemented")

}

func addDurationToDate(s series.Series, name string, duration Duration) series.Series {
	panic("not implemented")

	// Add duration components

}

func diffDateTime(s1, s2 series.Series, name string) (series.Series, error) {
	panic("not implemented")

	// Difference in nanoseconds

	// Return as Duration type

}

func diffDate(s1, s2 series.Series, name string) (series.Series, error) {
	panic("not implemented")

	// Difference in days

	// Return as days (Int32)

}

func addBusinessDaysToDateTime(s series.Series, name string, days int) series.Series {
	panic("not implemented")

	// Add business days

}

func addBusinessDaysToDate(s series.Series, name string, days int) series.Series {
	panic("not implemented")

	// Add business days

}

// Series arithmetic operations for expressions

// DateTimeAddExpr represents adding a duration to a datetime expression
type DateTimeAddExpr struct {
	expr     expr.Expr
	duration Duration
}

func (e *DateTimeAddExpr) String() string {
	panic("not implemented")

}

func (e *DateTimeAddExpr) DataType() datatypes.DataType {
	panic("not implemented")

}

func (e *DateTimeAddExpr) Alias(name string) expr.Expr {
	panic("not implemented")

}

func (e *DateTimeAddExpr) IsColumn() bool {
	panic("not implemented")

}

func (e *DateTimeAddExpr) Name() string {
	panic("not implemented")

}

// Add adds a duration to the datetime expression
func (dte *DateTimeExpr) Add(duration Duration) expr.Expr {
	panic("not implemented")

}

// DateTimeSubExpr represents subtracting a duration from a datetime expression
type DateTimeSubExpr struct {
	expr     expr.Expr
	duration Duration
}

func (e *DateTimeSubExpr) String() string {
	panic("not implemented")

}

func (e *DateTimeSubExpr) DataType() datatypes.DataType {
	panic("not implemented")

}

func (e *DateTimeSubExpr) Alias(name string) expr.Expr {
	panic("not implemented")

}

func (e *DateTimeSubExpr) IsColumn() bool {
	panic("not implemented")

}

func (e *DateTimeSubExpr) Name() string {
	panic("not implemented")

}

// Sub subtracts a duration from the datetime expression
func (dte *DateTimeExpr) Sub(duration Duration) expr.Expr {
	panic("not implemented")

}

// DateTimeDiffExpr represents the difference between two datetime expressions
type DateTimeDiffExpr struct {
	left  expr.Expr
	right expr.Expr
}

func (e *DateTimeDiffExpr) String() string {
	panic("not implemented")

}

func (e *DateTimeDiffExpr) DataType() datatypes.DataType {
	panic(
		// Returns duration for datetime diff, int32 for date diff
		"not implemented")

	// If left is a column expression, assume it's datetime by default

	// Default to Duration for datetime operations

}

func (e *DateTimeDiffExpr) Alias(name string) expr.Expr {
	panic("not implemented")

}

func (e *DateTimeDiffExpr) IsColumn() bool {
	panic("not implemented")

}

func (e *DateTimeDiffExpr) Name() string {
	panic("not implemented")

}

// Diff calculates the difference with another datetime expression
func (dte *DateTimeExpr) Diff(other expr.Expr) expr.Expr {
	panic("not implemented")

}
