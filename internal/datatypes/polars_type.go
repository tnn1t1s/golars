package datatypes

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// PolarsDataType is a generic interface that ties together Go types with their
// DataType representation and Arrow array types
type PolarsDataType interface {
	// DataType returns the Golars DataType for this type
	DataType() DataType

	// ArrowType returns the Arrow DataType for this type
	ArrowType() arrow.DataType

	// NewBuilder creates a new Arrow array builder for this type
	NewBuilder(mem memory.Allocator) array.Builder
}

// Type-safe generic constraint for values that can be stored in arrays
type ArrayValue interface {
	bool | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 |
		float32 | float64 | string | []byte | ~[]bool | ~[]int8 | ~[]int16 | ~[]int32 |
		~[]int64 | ~[]uint16 | ~[]uint32 | ~[]uint64 | ~[]float32 | ~[]float64 |
		~[]string
}

// Physical type implementations
type (
	BooleanType  struct{}
	Int8Type     struct{}
	Int16Type    struct{}
	Int32Type    struct{}
	Int64Type    struct{}
	UInt8Type    struct{}
	UInt16Type   struct{}
	UInt32Type   struct{}
	UInt64Type   struct{}
	Float32Type  struct{}
	Float64Type  struct{}
	StringType   struct{}
	BinaryType   struct{}
	DateType     struct{}
	TimeType     struct{}
	DatetimeType struct {
		Unit     TimeUnit
		TimeZone *time.Location
	}
	DurationType struct {
		Unit TimeUnit
	}
)

// BooleanType implementation
func (BooleanType) DataType() DataType        { return Boolean{} }
func (BooleanType) ArrowType() arrow.DataType { return arrow.FixedWidthTypes.Boolean }
func (BooleanType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewBooleanBuilder(mem)
}

// Int8Type implementation
func (Int8Type) DataType() DataType        { return Int8{} }
func (Int8Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Int8 }
func (Int8Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewInt8Builder(mem)
}

// Int16Type implementation
func (Int16Type) DataType() DataType        { return Int16{} }
func (Int16Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Int16 }
func (Int16Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewInt16Builder(mem)
}

// Int32Type implementation
func (Int32Type) DataType() DataType        { return Int32{} }
func (Int32Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Int32 }
func (Int32Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewInt32Builder(mem)
}

// Int64Type implementation
func (Int64Type) DataType() DataType        { return Int64{} }
func (Int64Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Int64 }
func (Int64Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewInt64Builder(mem)
}

// UInt8Type implementation
func (UInt8Type) DataType() DataType        { return UInt8{} }
func (UInt8Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Uint8 }
func (UInt8Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewUint8Builder(mem)
}

// UInt16Type implementation
func (UInt16Type) DataType() DataType        { return UInt16{} }
func (UInt16Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Uint16 }
func (UInt16Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewUint16Builder(mem)
}

// UInt32Type implementation
func (UInt32Type) DataType() DataType        { return UInt32{} }
func (UInt32Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Uint32 }
func (UInt32Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewUint32Builder(mem)
}

// UInt64Type implementation
func (UInt64Type) DataType() DataType        { return UInt64{} }
func (UInt64Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Uint64 }
func (UInt64Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewUint64Builder(mem)
}

// Float32Type implementation
func (Float32Type) DataType() DataType        { return Float32{} }
func (Float32Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Float32 }
func (Float32Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewFloat32Builder(mem)
}

// Float64Type implementation
func (Float64Type) DataType() DataType        { return Float64{} }
func (Float64Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Float64 }
func (Float64Type) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewFloat64Builder(mem)
}

// StringType implementation
func (StringType) DataType() DataType        { return String{} }
func (StringType) ArrowType() arrow.DataType { return arrow.BinaryTypes.String }
func (StringType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewStringBuilder(mem)
}

// BinaryType implementation
func (BinaryType) DataType() DataType        { return Binary{} }
func (BinaryType) ArrowType() arrow.DataType { return arrow.BinaryTypes.Binary }
func (BinaryType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
}

// DateType implementation (Date32 in Arrow)
func (DateType) DataType() DataType        { return Date{} }
func (DateType) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Date32 }
func (DateType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewDate32Builder(mem)
}

// TimeType implementation (Time64 with nanoseconds in Arrow)
func (TimeType) DataType() DataType        { return Time{} }
func (TimeType) ArrowType() arrow.DataType { return arrow.FixedWidthTypes.Time64ns }
func (TimeType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64ns.(*arrow.Time64Type))
}

// DatetimeType implementation (Timestamp with nanoseconds in Arrow)
func (dt DatetimeType) DataType() DataType {
	return Datetime{Unit: dt.Unit, TimeZone: dt.TimeZone}
}
func (dt DatetimeType) ArrowType() arrow.DataType {
	unit := arrow.Nanosecond
	switch dt.Unit {
	case Microseconds:
		unit = arrow.Microsecond
	case Milliseconds:
		unit = arrow.Millisecond
	}
	tz := ""
	if dt.TimeZone != nil {
		tz = dt.TimeZone.String()
	}
	return &arrow.TimestampType{Unit: unit, TimeZone: tz}
}
func (dt DatetimeType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewTimestampBuilder(mem, dt.ArrowType().(*arrow.TimestampType))
}

// DurationType implementation (Duration in Arrow)
func (dt DurationType) DataType() DataType {
	return Duration{Unit: dt.Unit}
}
func (dt DurationType) ArrowType() arrow.DataType {
	unit := arrow.Nanosecond
	switch dt.Unit {
	case Microseconds:
		unit = arrow.Microsecond
	case Milliseconds:
		unit = arrow.Millisecond
	}
	return &arrow.DurationType{Unit: unit}
}
func (dt DurationType) NewBuilder(mem memory.Allocator) array.Builder {
	return array.NewDurationBuilder(mem, dt.ArrowType().(*arrow.DurationType))
}

// Type mapping functions

// GetPolarsType returns the PolarsDataType for a given DataType
func GetPolarsType(dt DataType) PolarsDataType {
	switch dt.(type) {
	case Boolean:
		return BooleanType{}
	case Int8:
		return Int8Type{}
	case Int16:
		return Int16Type{}
	case Int32:
		return Int32Type{}
	case Int64:
		return Int64Type{}
	case UInt8:
		return UInt8Type{}
	case UInt16:
		return UInt16Type{}
	case UInt32:
		return UInt32Type{}
	case UInt64:
		return UInt64Type{}
	case Float32:
		return Float32Type{}
	case Float64:
		return Float64Type{}
	case String:
		return StringType{}
	case Binary:
		return BinaryType{}
	case Date:
		return DateType{}
	case Time:
		return TimeType{}
	case Datetime:
		dt := dt.(Datetime)
		return DatetimeType{Unit: dt.Unit, TimeZone: dt.TimeZone}
	case Duration:
		dur := dt.(Duration)
		return DurationType{Unit: dur.Unit}
	default:
		panic("unsupported data type")
	}
}

// FromArrowType converts an Arrow DataType to a Golars DataType
func FromArrowType(dt arrow.DataType) DataType {
	switch dt.ID() {
	case arrow.BOOL:
		return Boolean{}
	case arrow.INT8:
		return Int8{}
	case arrow.INT16:
		return Int16{}
	case arrow.INT32:
		return Int32{}
	case arrow.INT64:
		return Int64{}
	case arrow.UINT8:
		return UInt8{}
	case arrow.UINT16:
		return UInt16{}
	case arrow.UINT32:
		return UInt32{}
	case arrow.UINT64:
		return UInt64{}
	case arrow.FLOAT32:
		return Float32{}
	case arrow.FLOAT64:
		return Float64{}
	case arrow.STRING:
		return String{}
	case arrow.BINARY:
		return Binary{}
	case arrow.DATE32:
		return Date{}
	case arrow.TIME64:
		return Time{}
	default:
		return Unknown{}
	}
}
