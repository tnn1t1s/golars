# Advanced Data Types Design for Golars

## Executive Summary

This document outlines the design for implementing advanced data types in Golars to achieve feature parity with Polars. Based on a comprehensive gap analysis, we identify missing data types, propose implementation strategies, and define a roadmap for enhancing Golars' type system.

## Current State Analysis

### Existing Data Types in Golars

Golars currently implements a solid foundation of data types:

**Primitive Types:**
- ✅ Boolean
- ✅ Int8, Int16, Int32, Int64
- ✅ UInt8, UInt16, UInt32, UInt64
- ✅ Float32, Float64
- ✅ String, Binary
- ✅ Null, Unknown

**Temporal Types:**
- ✅ Date (days since epoch)
- ✅ DateTime (with timezone support)
- ✅ Duration (time intervals)
- ✅ Time (time of day)

**Complex Types:**
- ✅ List (variable-length arrays)
- ✅ Array (fixed-size arrays)
- ✅ Struct (composite types)
- ✅ Categorical (string categories)
- ✅ Decimal (fixed-point numbers)

### Gap Analysis with Polars

#### Missing Data Types

**Performance-Critical Types:**
1. **StringView/BinaryView** - Zero-copy string operations
2. **Int128/UInt128** - Large integer support
3. **Enum** - Compile-time categorical types

**Extended Types:**
1. **Float16** - Half-precision floating point
2. **Object** - Custom object storage
3. **FixedSizeList** - Compile-time sized lists
4. **Large* variants** - 64-bit offset types

#### Architectural Gaps

1. **Type System Features:**
   - Limited type inference
   - No type coercion rules
   - Missing type parameter support
   - No extension type mechanism

2. **Performance Optimizations:**
   - No specialized memory pools
   - Missing zero-copy optimizations
   - Limited vectorized operations

3. **Schema Evolution:**
   - No schema versioning
   - Limited type compatibility checking
   - Missing migration support

## Design Principles

1. **Go Idiomatic**: Use interfaces and composition over complex type hierarchies
2. **Performance First**: Minimize allocations and maximize cache efficiency
3. **Type Safety**: Leverage Go's type system for compile-time guarantees
4. **Extensibility**: Design for future type additions
5. **Arrow Compatibility**: Maintain interoperability with Apache Arrow

## Proposed Architecture

### 1. Enhanced Type System

```go
// internal/datatypes/types.go

// DataType represents the logical type of data
type DataType interface {
    // Core methods
    String() string
    Equals(other DataType) bool
    PhysicalType() PhysicalType
    
    // Type properties
    IsNumeric() bool
    IsTemporal() bool
    IsNested() bool
    IsView() bool
    
    // Arrow integration
    ToArrow() arrow.DataType
    FromArrow(arrow.DataType) error
    
    // Type parameters (for generic types)
    TypeParameters() []DataType
}

// PhysicalType represents the in-memory representation
type PhysicalType interface {
    DataType
    
    // Memory layout
    ByteWidth() int
    IsFixedWidth() bool
    
    // Allocation
    NewBuilder(capacity int) Builder
    NewArray(length int) Array
}

// TypeWithMetadata adds metadata support
type TypeWithMetadata interface {
    DataType
    Metadata() map[string]string
    WithMetadata(map[string]string) DataType
}
```

### 2. StringView Implementation

StringView provides zero-copy string operations by storing strings in a shared buffer with views pointing to substrings.

```go
// internal/datatypes/stringview.go

type StringViewType struct {
    baseType
}

// StringView represents a view into string data
type StringView struct {
    // Inline storage for small strings (≤ 12 bytes)
    prefix [4]byte
    length uint32
    
    // For larger strings
    bufferIdx uint32
    offset    uint32
}

// StringViewArray manages string data efficiently
type StringViewArray struct {
    views   []StringView
    buffers [][]byte // Shared string buffers
    
    nullBitmap *Bitmap
}

// Key operations
func (s *StringViewArray) Value(i int) string
func (s *StringViewArray) CompactBuffers() error
func (s *StringViewArray) Slice(offset, length int) Array
```

### 3. Int128/UInt128 Support

Large integer support for compatibility with systems requiring 128-bit integers.

```go
// internal/datatypes/int128.go

type Int128Type struct {
    baseType
}

// Int128 represents a 128-bit signed integer
type Int128 struct {
    lo uint64
    hi int64
}

// Operations
func (i Int128) Add(other Int128) Int128
func (i Int128) Mul(other Int128) Int128
func (i Int128) String() string
func (i Int128) ToFloat64() float64

// Array implementation
type Int128Array struct {
    data       []Int128
    nullBitmap *Bitmap
}
```

### 4. Enum Type

Enum provides categorical data with compile-time known categories.

```go
// internal/datatypes/enum.go

type EnumType struct {
    baseType
    categories []string
    ordered    bool
}

// EnumArray stores indices into the categories
type EnumArray struct {
    indices    []uint32
    enumType   *EnumType
    nullBitmap *Bitmap
}

// Builder pattern for construction
type EnumBuilder struct {
    indices  []uint32
    enumType *EnumType
    lookup   map[string]uint32
}

func (b *EnumBuilder) Append(value string) error {
    idx, ok := b.lookup[value]
    if !ok {
        return fmt.Errorf("unknown category: %s", value)
    }
    b.indices = append(b.indices, idx)
    return nil
}
```

### 5. Type Inference System

Enhanced type inference for better user experience.

```go
// internal/datatypes/inference.go

// TypeInferrer infers types from data
type TypeInferrer struct {
    // Configuration
    inferStringsAsDatetime bool
    inferIntegers         bool
    decimalPrecision      int
    
    // State
    samples []interface{}
    nullCount int
}

// InferType determines the most appropriate type
func (t *TypeInferrer) InferType() (DataType, error) {
    // Analyze samples to determine type
    // Handle nulls, mixed types, etc.
}

// InferSchema infers schema from multiple columns
func InferSchema(data [][]interface{}) (*Schema, error)
```

### 6. Type Coercion Rules

Automatic type promotion for operations.

```go
// internal/datatypes/coercion.go

// CoercionRules defines type promotion rules
type CoercionRules struct {
    rules map[TypePair]DataType
}

type TypePair struct {
    left  DataType
    right DataType
}

// Common coercion rules
var DefaultCoercionRules = &CoercionRules{
    rules: map[TypePair]DataType{
        {Int32Type{}, Float64Type{}}: Float64Type{},
        {Int64Type{}, Float64Type{}}: Float64Type{},
        // ... more rules
    },
}

// FindCommonType determines the common type for operations
func (c *CoercionRules) FindCommonType(types ...DataType) (DataType, error)
```

### 7. Extension Type System

Support for custom types via extension mechanism.

```go
// internal/datatypes/extension.go

// ExtensionType allows custom type implementations
type ExtensionType interface {
    DataType
    
    // Extension identification
    ExtensionName() string
    
    // Serialization
    Serialize() ([]byte, error)
    Deserialize([]byte) error
    
    // Storage type
    StorageType() DataType
}

// Registry for extension types
type ExtensionTypeRegistry struct {
    types map[string]func() ExtensionType
}

var GlobalExtensionRegistry = &ExtensionTypeRegistry{
    types: make(map[string]func() ExtensionType),
}

// Example: UUID type
type UUIDType struct {
    ExtensionType
    storage BinaryType // Store as 16-byte binary
}
```

## Implementation Roadmap

### Phase 1: Core Infrastructure (2 weeks)
1. **Type System Refactoring**
   - Enhance DataType interface
   - Add TypeParameters support
   - Implement TypeWithMetadata
   - Create type registry

2. **Type Inference Framework**
   - Basic type inference
   - Schema inference
   - Configurable inference rules

### Phase 2: Performance Types (3 weeks)
1. **StringView/BinaryView**
   - Implement view types
   - Buffer management
   - Zero-copy operations
   - Integration with Series

2. **Memory Optimizations**
   - Type-specific memory pools
   - Buffer compaction
   - Reference counting

### Phase 3: Extended Numeric Types (2 weeks)
1. **Int128/UInt128**
   - 128-bit arithmetic
   - Conversion utilities
   - Arrow integration
   - Performance optimization

2. **Float16** (optional)
   - Half-precision support
   - Conversion routines
   - Limited operations

### Phase 4: Advanced Categorical Types (2 weeks)
1. **Enum Type**
   - Fixed categories
   - Compile-time validation
   - Efficient storage
   - Ordering support

2. **Enhanced Categorical**
   - Global string cache
   - Dictionary encoding
   - Merge operations

### Phase 5: Type Operations (2 weeks)
1. **Coercion System**
   - Rule-based coercion
   - Automatic promotion
   - Error handling
   - Performance optimization

2. **Extension Types**
   - Registry implementation
   - Example extensions
   - Serialization support

### Phase 6: Integration & Testing (1 week)
1. **Series/DataFrame Integration**
   - Update constructors
   - Type-specific operations
   - Performance benchmarks

2. **Comprehensive Testing**
   - Unit tests for all types
   - Integration tests
   - Performance benchmarks
   - Compatibility tests

## Performance Considerations

### Memory Layout
- Use Arrow-compatible layouts where possible
- Minimize padding and alignment issues
- Consider cache line optimization

### Zero-Copy Operations
- StringView for substring operations
- Slice operations on arrays
- Reference counting for shared buffers

### Vectorization
- Design for SIMD operations
- Batch processing APIs
- Type-specific optimizations

## Testing Strategy

### Unit Tests
- Type construction and properties
- Conversion operations
- Arrow compatibility
- Edge cases and error handling

### Integration Tests
- Series operations with new types
- DataFrame operations
- I/O with new types
- Query engine compatibility

### Performance Tests
- Benchmark against current implementation
- Memory usage profiling
- Operation throughput
- Comparison with Polars

### Compatibility Tests
- Arrow round-trip conversion
- Schema evolution scenarios
- Type coercion edge cases

## Migration Strategy

### Backward Compatibility
- Existing types remain unchanged
- New types are additive
- Gradual adoption path

### Migration Path
1. Add new types alongside existing
2. Update documentation and examples
3. Provide migration utilities
4. Deprecate old patterns (if any)

## Success Criteria

1. **Feature Parity**
   - All Polars data types supported
   - Equivalent functionality
   - Similar performance characteristics

2. **Performance**
   - StringView operations 2-5x faster
   - Minimal memory overhead
   - Efficient type conversions

3. **Usability**
   - Intuitive API
   - Good error messages
   - Comprehensive documentation

4. **Compatibility**
   - Full Arrow compatibility
   - Clean integration with existing code
   - No breaking changes

## Future Considerations

### Potential Extensions
1. **Geometry Types** - For spatial data
2. **JSON Type** - Native JSON support
3. **Tensor Type** - Multi-dimensional arrays
4. **Interval Type** - Time intervals

### Performance Enhancements
1. GPU acceleration for certain types
2. Custom SIMD implementations
3. Compression support
4. Memory mapping for large data

### Ecosystem Integration
1. Database type mapping
2. Parquet type extensions
3. Network serialization
4. Language bindings

## Conclusion

This design provides a comprehensive approach to implementing advanced data types in Golars. The phased implementation allows for incremental progress while maintaining system stability. Focus on high-impact types (StringView, Int128, Enum) will provide the most benefit to users while establishing patterns for future type additions.