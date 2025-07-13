# Type System Architecture

## Overview

Golars uses a two-level type system:
1. **Logical Types** (DataType interface) - What users see
2. **Physical Types** (PolarsDataType) - How data is stored

## DataType Hierarchy

```
DataType (interface)
├── Primitive Types
│   ├── Boolean
│   ├── Numeric (Int8-64, UInt8-64, Float32/64)
│   └── String/Binary
├── Temporal Types
│   ├── Date
│   ├── Time
│   ├── Datetime(unit, timezone)
│   └── Duration(unit)
├── Nested Types
│   ├── List(inner)
│   ├── Array(inner, width)
│   └── Struct(fields)
└── Special Types
    ├── Categorical(ordered)
    ├── Decimal(precision, scale)
    └── Null/Unknown
```

## Key Interfaces

### DataType
```go
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
```

### PolarsDataType
```go
type PolarsDataType interface {
    DataType() DataType
    ArrowType() arrow.DataType
    NewBuilder(mem memory.Allocator) array.Builder
}
```

## Type Mapping

| Golars Type | Arrow Type | Go Type |
|-------------|------------|---------|
| Boolean | arrow.BOOL | bool |
| Int32 | arrow.INT32 | int32 |
| Float64 | arrow.FLOAT64 | float64 |
| String | arrow.STRING | string |
| Binary | arrow.BINARY | []byte |
| Date | arrow.DATE32 | time.Time |

## Generic Constraints

```go
// ArrayValue constrains types that can be stored in arrays
type ArrayValue interface {
    bool | int8 | int16 | int32 | int64 | 
    uint8 | uint16 | uint32 | uint64 |
    float32 | float64 | string | []byte
}
```

## Adding New Types

1. Add type struct to datatypes/datatype.go
2. Implement DataType interface methods
3. Add physical type to datatypes/polars_type.go
4. Implement PolarsDataType interface
5. Update GetPolarsType() mapping function
6. Add to ArrayValue constraint if needed