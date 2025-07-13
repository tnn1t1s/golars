package datetime

import (
	"fmt"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/expr"
)

// DateTimeExpr provides datetime-specific operations on expressions
type DateTimeExpr struct {
	expr expr.Expr
}

// dateTimeAliasExpr wraps an expression with an alias
type dateTimeAliasExpr struct {
	expr  expr.Expr
	alias string
}

func (e *dateTimeAliasExpr) String() string {
	return e.alias
}

func (e *dateTimeAliasExpr) DataType() datatypes.DataType {
	return e.expr.DataType()
}

func (e *dateTimeAliasExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e.expr, alias: name}
}

func (e *dateTimeAliasExpr) IsColumn() bool {
	return false
}

func (e *dateTimeAliasExpr) Name() string {
	return e.alias
}

// DtExpr returns datetime operations for an expression
func DtExpr(e expr.Expr) *DateTimeExpr {
	return &DateTimeExpr{expr: e}
}

// DateTimeComponentExpr represents a datetime component extraction
type DateTimeComponentExpr struct {
	expr      expr.Expr
	component string
}

func (e *DateTimeComponentExpr) String() string {
	return fmt.Sprintf("%s.dt.%s()", e.expr.String(), e.component)
}

func (e *DateTimeComponentExpr) DataType() datatypes.DataType {
	// Most components return Int32
	switch e.component {
	case "nanosecond", "microsecond", "millisecond":
		return datatypes.Int64{}
	default:
		return datatypes.Int32{}
	}
}

func (e *DateTimeComponentExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeComponentExpr) IsColumn() bool {
	return false
}

func (e *DateTimeComponentExpr) Name() string {
	return fmt.Sprintf("%s_%s", e.expr.Name(), e.component)
}

// Year extracts the year component
func (dte *DateTimeExpr) Year() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "year",
	}
}

// Month extracts the month component
func (dte *DateTimeExpr) Month() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "month",
	}
}

// Day extracts the day component
func (dte *DateTimeExpr) Day() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "day",
	}
}

// Hour extracts the hour component
func (dte *DateTimeExpr) Hour() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "hour",
	}
}

// Minute extracts the minute component
func (dte *DateTimeExpr) Minute() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "minute",
	}
}

// Second extracts the second component
func (dte *DateTimeExpr) Second() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "second",
	}
}

// Nanosecond extracts the nanosecond component
func (dte *DateTimeExpr) Nanosecond() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "nanosecond",
	}
}

// DayOfWeek extracts the day of week
func (dte *DateTimeExpr) DayOfWeek() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "dayofweek",
	}
}

// DayOfYear extracts the day of year
func (dte *DateTimeExpr) DayOfYear() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "dayofyear",
	}
}

// Quarter extracts the quarter
func (dte *DateTimeExpr) Quarter() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "quarter",
	}
}

// WeekOfYear extracts the week of year
func (dte *DateTimeExpr) WeekOfYear() expr.Expr {
	return &DateTimeComponentExpr{
		expr:      dte.expr,
		component: "weekofyear",
	}
}

// DateTimeFormatExpr represents datetime formatting
type DateTimeFormatExpr struct {
	expr   expr.Expr
	format string
}

func (e *DateTimeFormatExpr) String() string {
	return fmt.Sprintf("%s.dt.format(%q)", e.expr.String(), e.format)
}

func (e *DateTimeFormatExpr) DataType() datatypes.DataType {
	return datatypes.String{}
}

func (e *DateTimeFormatExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeFormatExpr) IsColumn() bool {
	return false
}

func (e *DateTimeFormatExpr) Name() string {
	return fmt.Sprintf("%s_formatted", e.expr.Name())
}

// Format formats the datetime values
func (dte *DateTimeExpr) Format(format string) expr.Expr {
	return &DateTimeFormatExpr{
		expr:   dte.expr,
		format: format,
	}
}

// DateTimeRoundExpr represents datetime rounding operations
type DateTimeRoundExpr struct {
	expr expr.Expr
	unit TimeUnit
	op   string // "floor", "ceil", "round", "truncate"
}

func (e *DateTimeRoundExpr) String() string {
	return fmt.Sprintf("%s.dt.%s(%s)", e.expr.String(), e.op, e.unit.String())
}

func (e *DateTimeRoundExpr) DataType() datatypes.DataType {
	// Returns the same datetime type as input
	return e.expr.DataType()
}

func (e *DateTimeRoundExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *DateTimeRoundExpr) IsColumn() bool {
	return false
}

func (e *DateTimeRoundExpr) Name() string {
	return fmt.Sprintf("%s_%s_%s", e.expr.Name(), e.op, e.unit.String())
}

// Floor rounds down to the nearest unit
func (dte *DateTimeExpr) Floor(unit TimeUnit) expr.Expr {
	return &DateTimeRoundExpr{
		expr: dte.expr,
		unit: unit,
		op:   "floor",
	}
}

// Ceil rounds up to the nearest unit
func (dte *DateTimeExpr) Ceil(unit TimeUnit) expr.Expr {
	return &DateTimeRoundExpr{
		expr: dte.expr,
		unit: unit,
		op:   "ceil",
	}
}

// Round rounds to the nearest unit
func (dte *DateTimeExpr) Round(unit TimeUnit) expr.Expr {
	return &DateTimeRoundExpr{
		expr: dte.expr,
		unit: unit,
		op:   "round",
	}
}

// Truncate is an alias for Floor
func (dte *DateTimeExpr) Truncate(unit TimeUnit) expr.Expr {
	return dte.Floor(unit)
}

// String to DateTime conversion expressions

// StrToDateTimeExpr converts string to datetime
type StrToDateTimeExpr struct {
	expr   expr.Expr
	format string
}

func (e *StrToDateTimeExpr) String() string {
	if e.format == "" {
		return fmt.Sprintf("str_to_datetime(%s)", e.expr.String())
	}
	return fmt.Sprintf("str_to_datetime(%s, %q)", e.expr.String(), e.format)
}

func (e *StrToDateTimeExpr) DataType() datatypes.DataType {
	return datatypes.Datetime{
		Unit:     datatypes.Nanoseconds,
		TimeZone: nil,
	}
}

func (e *StrToDateTimeExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *StrToDateTimeExpr) IsColumn() bool {
	return false
}

func (e *StrToDateTimeExpr) Name() string {
	return fmt.Sprintf("%s_datetime", e.expr.Name())
}

// StrToDateTime converts a string expression to datetime
func StrToDateTime(e expr.Expr, format string) expr.Expr {
	return &StrToDateTimeExpr{
		expr:   e,
		format: format,
	}
}

// StrToDateExpr converts string to date
type StrToDateExpr struct {
	expr   expr.Expr
	format string
}

func (e *StrToDateExpr) String() string {
	if e.format == "" {
		return fmt.Sprintf("str_to_date(%s)", e.expr.String())
	}
	return fmt.Sprintf("str_to_date(%s, %q)", e.expr.String(), e.format)
}

func (e *StrToDateExpr) DataType() datatypes.DataType {
	return datatypes.Date{}
}

func (e *StrToDateExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *StrToDateExpr) IsColumn() bool {
	return false
}

func (e *StrToDateExpr) Name() string {
	return fmt.Sprintf("%s_date", e.expr.Name())
}

// StrToDate converts a string expression to date
func StrToDate(e expr.Expr, format string) expr.Expr {
	return &StrToDateExpr{
		expr:   e,
		format: format,
	}
}

// StrToTimeExpr converts string to time
type StrToTimeExpr struct {
	expr   expr.Expr
	format string
}

func (e *StrToTimeExpr) String() string {
	if e.format == "" {
		return fmt.Sprintf("str_to_time(%s)", e.expr.String())
	}
	return fmt.Sprintf("str_to_time(%s, %q)", e.expr.String(), e.format)
}

func (e *StrToTimeExpr) DataType() datatypes.DataType {
	return datatypes.Time{}
}

func (e *StrToTimeExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *StrToTimeExpr) IsColumn() bool {
	return false
}

func (e *StrToTimeExpr) Name() string {
	return fmt.Sprintf("%s_time", e.expr.Name())
}

// StrToTime converts a string expression to time
func StrToTime(e expr.Expr, format string) expr.Expr {
	return &StrToTimeExpr{
		expr:   e,
		format: format,
	}
}