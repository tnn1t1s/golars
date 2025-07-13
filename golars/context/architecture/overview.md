# Golars Architecture Overview

## Core Design Principles

1. **Column-Oriented Storage**: All data is stored in columnar format using Apache Arrow
2. **Type Safety**: Extensive use of Go generics for compile-time type checking
3. **Zero-Copy Operations**: Minimize memory allocations and copies
4. **Thread Safety**: All public APIs are thread-safe using sync.RWMutex
5. **Lazy Evaluation Ready**: Architecture supports future lazy evaluation

## Component Hierarchy

```
DataFrame
    ├── Series (multiple)
    │   └── ChunkedArray[T]
    │       └── Arrow Arrays (chunks)
    ├── Schema
    │   └── Fields (name, type, nullable)
    └── Operations
        ├── Filter (expression-based)
        ├── Select/Drop
        └── Slice/Head/Tail
```

## Key Design Decisions

### 1. Type System
- Created custom DataType interface instead of using Arrow directly
- Allows for Polars-specific types and future extensions
- Bridge to Arrow types via PolarsDataType interface

### 2. ChunkedArray Design
- Multiple Arrow arrays as chunks (allows efficient appends)
- Generic over value types using Go 1.18+ generics
- Statistics tracking (null count, length, future: min/max)

### 3. Series Type Erasure
- Interface-based to allow dynamic dispatch
- Wraps typed ChunkedArray[T] implementations
- Enables DataFrame to hold heterogeneous column types

### 4. Expression System
- Separate expression types from evaluation
- Builder pattern for fluent API
- Prepared for lazy evaluation (expressions are just descriptions)

### 5. Memory Management
- Uses Arrow's memory allocator
- Reference counting on chunks
- Careful null handling throughout

## Performance Considerations

1. **Minimize Allocations**: Reuse buffers where possible
2. **Batch Operations**: Process entire chunks rather than individual values
3. **Early Filtering**: Push filters down to reduce data movement
4. **Type Specialization**: Separate code paths for each type to avoid boxing

## Thread Safety Strategy

- Read-Write mutex on DataFrame and ChunkedArray
- Immutable operations return new instances
- Series are effectively immutable (operations create new Series)

## Extension Points

1. **New Types**: Add to datatypes package, implement PolarsDataType
2. **New Operations**: Add methods to DataFrame/Series
3. **New Expressions**: Extend expr package
4. **New Kernels**: Add to compute package
5. **I/O Formats**: Will go in io package