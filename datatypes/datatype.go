package datatypes

import (
	"fmt"
	"time"
)

// DataType represents all possible data types in Golars
type DataType interface {
	String() string
	Equals(other DataType) bool
	IsNumeric() bool
	IsNested() bool
	IsTemporal() bool
	IsFloat() bool
	IsInteger() bool
	IsSigned() bool
}

// TimeUnit represents the unit of time for temporal types
type TimeUnit int

const (
	Nanoseconds TimeUnit = iota
	Microseconds
	Milliseconds
)

func (tu TimeUnit) String() string {
	switch tu {
	case Nanoseconds:
		return "ns"
	case Microseconds:
		return "us"
	case Milliseconds:
		return "ms"
	default:
		return "unknown"
	}
}

// Base types
type (
	Boolean      struct{}
	Int8         struct{}
	Int16        struct{}
	Int32        struct{}
	Int64        struct{}
	UInt8        struct{}
	UInt16       struct{}
	UInt32       struct{}
	UInt64       struct{}
	Float32      struct{}
	Float64      struct{}
	String       struct{}
	Binary       struct{}
	Date         struct{}
	Time         struct{}
	Null         struct{}
	Unknown      struct{}
)

// Complex types
type (
	Datetime struct {
		Unit     TimeUnit
		TimeZone *time.Location
	}
	Duration struct {
		Unit TimeUnit
	}
	List struct {
		Inner DataType
	}
	Array struct {
		Inner  DataType
		Width  int
	}
	Struct struct {
		Fields []Field
	}
	Categorical struct {
		Ordered bool
	}
	Decimal struct {
		Precision int
		Scale     int
	}
)

// Field represents a field in a struct or schema
type Field struct {
	Name     string
	DataType DataType
	Nullable bool
}

// Schema represents a collection of fields
type Schema struct {
	Fields []Field
}

// NewSchema creates a new schema from fields
func NewSchema(fields ...Field) *Schema {
	return &Schema{Fields: fields}
}

// GetField returns a field by name
func (s *Schema) GetField(name string) (*Field, bool) {
	for _, field := range s.Fields {
		if field.Name == name {
			return &field, true
		}
	}
	return nil, false
}

// FieldNames returns all field names
func (s *Schema) FieldNames() []string {
	names := make([]string, len(s.Fields))
	for i, field := range s.Fields {
		names[i] = field.Name
	}
	return names
}

// Implementation of DataType interface for base types

func (Boolean) String() string     { return "bool" }
func (Boolean) Equals(o DataType) bool { _, ok := o.(Boolean); return ok }
func (Boolean) IsNumeric() bool    { return false }
func (Boolean) IsNested() bool     { return false }
func (Boolean) IsTemporal() bool   { return false }
func (Boolean) IsFloat() bool      { return false }
func (Boolean) IsInteger() bool    { return false }
func (Boolean) IsSigned() bool     { return false }

func (Int8) String() string     { return "i8" }
func (Int8) Equals(o DataType) bool { _, ok := o.(Int8); return ok }
func (Int8) IsNumeric() bool    { return true }
func (Int8) IsNested() bool     { return false }
func (Int8) IsTemporal() bool   { return false }
func (Int8) IsFloat() bool      { return false }
func (Int8) IsInteger() bool    { return true }
func (Int8) IsSigned() bool     { return true }

func (Int16) String() string     { return "i16" }
func (Int16) Equals(o DataType) bool { _, ok := o.(Int16); return ok }
func (Int16) IsNumeric() bool    { return true }
func (Int16) IsNested() bool     { return false }
func (Int16) IsTemporal() bool   { return false }
func (Int16) IsFloat() bool      { return false }
func (Int16) IsInteger() bool    { return true }
func (Int16) IsSigned() bool     { return true }

func (Int32) String() string     { return "i32" }
func (Int32) Equals(o DataType) bool { _, ok := o.(Int32); return ok }
func (Int32) IsNumeric() bool    { return true }
func (Int32) IsNested() bool     { return false }
func (Int32) IsTemporal() bool   { return false }
func (Int32) IsFloat() bool      { return false }
func (Int32) IsInteger() bool    { return true }
func (Int32) IsSigned() bool     { return true }

func (Int64) String() string     { return "i64" }
func (Int64) Equals(o DataType) bool { _, ok := o.(Int64); return ok }
func (Int64) IsNumeric() bool    { return true }
func (Int64) IsNested() bool     { return false }
func (Int64) IsTemporal() bool   { return false }
func (Int64) IsFloat() bool      { return false }
func (Int64) IsInteger() bool    { return true }
func (Int64) IsSigned() bool     { return true }

func (UInt8) String() string     { return "u8" }
func (UInt8) Equals(o DataType) bool { _, ok := o.(UInt8); return ok }
func (UInt8) IsNumeric() bool    { return true }
func (UInt8) IsNested() bool     { return false }
func (UInt8) IsTemporal() bool   { return false }
func (UInt8) IsFloat() bool      { return false }
func (UInt8) IsInteger() bool    { return true }
func (UInt8) IsSigned() bool     { return false }

func (UInt16) String() string     { return "u16" }
func (UInt16) Equals(o DataType) bool { _, ok := o.(UInt16); return ok }
func (UInt16) IsNumeric() bool    { return true }
func (UInt16) IsNested() bool     { return false }
func (UInt16) IsTemporal() bool   { return false }
func (UInt16) IsFloat() bool      { return false }
func (UInt16) IsInteger() bool    { return true }
func (UInt16) IsSigned() bool     { return false }

func (UInt32) String() string     { return "u32" }
func (UInt32) Equals(o DataType) bool { _, ok := o.(UInt32); return ok }
func (UInt32) IsNumeric() bool    { return true }
func (UInt32) IsNested() bool     { return false }
func (UInt32) IsTemporal() bool   { return false }
func (UInt32) IsFloat() bool      { return false }
func (UInt32) IsInteger() bool    { return true }
func (UInt32) IsSigned() bool     { return false }

func (UInt64) String() string     { return "u64" }
func (UInt64) Equals(o DataType) bool { _, ok := o.(UInt64); return ok }
func (UInt64) IsNumeric() bool    { return true }
func (UInt64) IsNested() bool     { return false }
func (UInt64) IsTemporal() bool   { return false }
func (UInt64) IsFloat() bool      { return false }
func (UInt64) IsInteger() bool    { return true }
func (UInt64) IsSigned() bool     { return false }

func (Float32) String() string     { return "f32" }
func (Float32) Equals(o DataType) bool { _, ok := o.(Float32); return ok }
func (Float32) IsNumeric() bool    { return true }
func (Float32) IsNested() bool     { return false }
func (Float32) IsTemporal() bool   { return false }
func (Float32) IsFloat() bool      { return true }
func (Float32) IsInteger() bool    { return false }
func (Float32) IsSigned() bool     { return true }

func (Float64) String() string     { return "f64" }
func (Float64) Equals(o DataType) bool { _, ok := o.(Float64); return ok }
func (Float64) IsNumeric() bool    { return true }
func (Float64) IsNested() bool     { return false }
func (Float64) IsTemporal() bool   { return false }
func (Float64) IsFloat() bool      { return true }
func (Float64) IsInteger() bool    { return false }
func (Float64) IsSigned() bool     { return true }

func (String) String() string     { return "str" }
func (String) Equals(o DataType) bool { _, ok := o.(String); return ok }
func (String) IsNumeric() bool    { return false }
func (String) IsNested() bool     { return false }
func (String) IsTemporal() bool   { return false }
func (String) IsFloat() bool      { return false }
func (String) IsInteger() bool    { return false }
func (String) IsSigned() bool     { return false }

func (Binary) String() string     { return "binary" }
func (Binary) Equals(o DataType) bool { _, ok := o.(Binary); return ok }
func (Binary) IsNumeric() bool    { return false }
func (Binary) IsNested() bool     { return false }
func (Binary) IsTemporal() bool   { return false }
func (Binary) IsFloat() bool      { return false }
func (Binary) IsInteger() bool    { return false }
func (Binary) IsSigned() bool     { return false }

func (Date) String() string     { return "date" }
func (Date) Equals(o DataType) bool { _, ok := o.(Date); return ok }
func (Date) IsNumeric() bool    { return false }
func (Date) IsNested() bool     { return false }
func (Date) IsTemporal() bool   { return true }
func (Date) IsFloat() bool      { return false }
func (Date) IsInteger() bool    { return false }
func (Date) IsSigned() bool     { return false }

func (Time) String() string     { return "time" }
func (Time) Equals(o DataType) bool { _, ok := o.(Time); return ok }
func (Time) IsNumeric() bool    { return false }
func (Time) IsNested() bool     { return false }
func (Time) IsTemporal() bool   { return true }
func (Time) IsFloat() bool      { return false }
func (Time) IsInteger() bool    { return false }
func (Time) IsSigned() bool     { return false }

func (Null) String() string     { return "null" }
func (Null) Equals(o DataType) bool { _, ok := o.(Null); return ok }
func (Null) IsNumeric() bool    { return false }
func (Null) IsNested() bool     { return false }
func (Null) IsTemporal() bool   { return false }
func (Null) IsFloat() bool      { return false }
func (Null) IsInteger() bool    { return false }
func (Null) IsSigned() bool     { return false }

func (Unknown) String() string     { return "unknown" }
func (Unknown) Equals(o DataType) bool { _, ok := o.(Unknown); return ok }
func (Unknown) IsNumeric() bool    { return false }
func (Unknown) IsNested() bool     { return false }
func (Unknown) IsTemporal() bool   { return false }
func (Unknown) IsFloat() bool      { return false }
func (Unknown) IsInteger() bool    { return false }
func (Unknown) IsSigned() bool     { return false }

// Complex types implementations

func (dt Datetime) String() string {
	tz := ""
	if dt.TimeZone != nil {
		tz = fmt.Sprintf("[%s]", dt.TimeZone)
	}
	return fmt.Sprintf("datetime[%s%s]", dt.Unit, tz)
}
func (dt Datetime) Equals(o DataType) bool {
	other, ok := o.(Datetime)
	if !ok {
		return false
	}
	return dt.Unit == other.Unit && 
		((dt.TimeZone == nil && other.TimeZone == nil) ||
		 (dt.TimeZone != nil && other.TimeZone != nil && dt.TimeZone.String() == other.TimeZone.String()))
}
func (Datetime) IsNumeric() bool  { return false }
func (Datetime) IsNested() bool   { return false }
func (Datetime) IsTemporal() bool { return true }
func (Datetime) IsFloat() bool    { return false }
func (Datetime) IsInteger() bool  { return false }
func (Datetime) IsSigned() bool   { return false }

func (d Duration) String() string     { return fmt.Sprintf("duration[%s]", d.Unit) }
func (d Duration) Equals(o DataType) bool {
	other, ok := o.(Duration)
	return ok && d.Unit == other.Unit
}
func (Duration) IsNumeric() bool  { return false }
func (Duration) IsNested() bool   { return false }
func (Duration) IsTemporal() bool { return true }
func (Duration) IsFloat() bool    { return false }
func (Duration) IsInteger() bool  { return false }
func (Duration) IsSigned() bool   { return false }

func (l List) String() string     { return fmt.Sprintf("list[%s]", l.Inner) }
func (l List) Equals(o DataType) bool {
	other, ok := o.(List)
	return ok && l.Inner.Equals(other.Inner)
}
func (List) IsNumeric() bool  { return false }
func (List) IsNested() bool   { return true }
func (List) IsTemporal() bool { return false }
func (List) IsFloat() bool    { return false }
func (List) IsInteger() bool  { return false }
func (List) IsSigned() bool   { return false }

func (a Array) String() string     { return fmt.Sprintf("array[%s, %d]", a.Inner, a.Width) }
func (a Array) Equals(o DataType) bool {
	other, ok := o.(Array)
	return ok && a.Inner.Equals(other.Inner) && a.Width == other.Width
}
func (Array) IsNumeric() bool  { return false }
func (Array) IsNested() bool   { return true }
func (Array) IsTemporal() bool { return false }
func (Array) IsFloat() bool    { return false }
func (Array) IsInteger() bool  { return false }
func (Array) IsSigned() bool   { return false }

func (s Struct) String() string {
	// TODO: implement struct string representation
	return "struct"
}
func (s Struct) Equals(o DataType) bool {
	other, ok := o.(Struct)
	if !ok || len(s.Fields) != len(other.Fields) {
		return false
	}
	for i, field := range s.Fields {
		otherField := other.Fields[i]
		if field.Name != otherField.Name || !field.DataType.Equals(otherField.DataType) {
			return false
		}
	}
	return true
}
func (Struct) IsNumeric() bool  { return false }
func (Struct) IsNested() bool   { return true }
func (Struct) IsTemporal() bool { return false }
func (Struct) IsFloat() bool    { return false }
func (Struct) IsInteger() bool  { return false }
func (Struct) IsSigned() bool   { return false }

func (Categorical) String() string     { return "cat" }
func (c Categorical) Equals(o DataType) bool {
	other, ok := o.(Categorical)
	return ok && c.Ordered == other.Ordered
}
func (Categorical) IsNumeric() bool  { return false }
func (Categorical) IsNested() bool   { return false }
func (Categorical) IsTemporal() bool { return false }
func (Categorical) IsFloat() bool    { return false }
func (Categorical) IsInteger() bool  { return false }
func (Categorical) IsSigned() bool   { return false }

func (d Decimal) String() string     { return fmt.Sprintf("decimal[%d, %d]", d.Precision, d.Scale) }
func (d Decimal) Equals(o DataType) bool {
	other, ok := o.(Decimal)
	return ok && d.Precision == other.Precision && d.Scale == other.Scale
}
func (Decimal) IsNumeric() bool  { return true }
func (Decimal) IsNested() bool   { return false }
func (Decimal) IsTemporal() bool { return false }
func (Decimal) IsFloat() bool    { return false }
func (Decimal) IsInteger() bool  { return false }
func (Decimal) IsSigned() bool   { return true }