# Design Decisions and Rationale

## 1. Type System Architecture

### Decision: Separate DataType and PolarsDataType
**Rationale**: 
- DataType provides the logical type system (what users work with)
- PolarsDataType bridges to Arrow's physical representation
- Allows future flexibility to change underlying storage without breaking API

**Trade-offs**:
- ✅ Clean separation of concerns
- ✅ Easier to extend type system
- ❌ Additional abstraction layer
- ❌ Some duplication between type definitions

## 2. Generic ChunkedArray with Type Erasure

### Decision: Use generics for ChunkedArray but type-erased Series interface
```go
type ChunkedArray[T ArrayValue] struct { ... }
type Series interface { ... }
type TypedSeries[T ArrayValue] struct { ... }
```

**Rationale**:
- ChunkedArray benefits from compile-time type safety
- DataFrame needs heterogeneous columns (different types)
- Type erasure at Series level enables mixed-type DataFrames

**Trade-offs**:
- ✅ Type safety where it matters (storage)
- ✅ Flexibility for DataFrame operations
- ❌ Runtime type checks needed at Series level
- ❌ Some performance overhead from interface calls

## 3. Arrow Arrays as Storage Backend

### Decision: Use Apache Arrow arrays internally
**Rationale**:
- Industry standard for columnar data
- Zero-copy interoperability with other systems
- Optimized memory layout for analytics
- Built-in null bitmap support

**Trade-offs**:
- ✅ Performance and memory efficiency
- ✅ Ecosystem compatibility
- ✅ Well-tested implementation
- ❌ Additional dependency
- ❌ Learning curve for Arrow concepts

## 4. Expression System Design

### Decision: Builder pattern for expressions with explicit Build()
```go
expr := ColBuilder("age").Gt(25).And(ColBuilder("active").Eq(true)).Build()
```

**Rationale**:
- Fluent API is intuitive
- Compile-time checking of method chains
- Clear distinction between building and built expressions
- Prevents accidental use of incomplete expressions

**Trade-offs**:
- ✅ Type-safe expression building
- ✅ Readable, chainable API
- ✅ Hard to misuse
- ❌ Verbose (requires Build())
- ❌ Extra allocation for builder

## 5. Immutable Operations

### Decision: All DataFrame/Series operations return new instances
**Rationale**:
- Thread safety by default
- Prevents surprising mutations
- Enables safe concurrent access
- Follows functional programming principles

**Trade-offs**:
- ✅ No race conditions
- ✅ Predictable behavior
- ✅ Easy to reason about
- ❌ Memory overhead
- ❌ Allocation pressure

## 6. Null Handling Strategy

### Decision: Use Arrow's validity bitmaps, null-propagating arithmetic
**Rationale**:
- Consistent with SQL semantics
- Memory efficient (1 bit per value)
- Aligns with Arrow ecosystem
- Predictable null propagation

**Trade-offs**:
- ✅ Standard behavior
- ✅ Memory efficient
- ✅ Fast null checks
- ❌ Must always consider null cases
- ❌ Some operations become complex

## 7. Error Handling Approach

### Decision: Return explicit errors, no panics in library code
**Rationale**:
- Go idiomatic error handling
- Allows users to handle errors appropriately
- Makes failure modes explicit
- Easier to debug issues

**Trade-offs**:
- ✅ Robust and predictable
- ✅ User has full control
- ✅ Clear error messages
- ❌ More verbose code
- ❌ Error checking boilerplate

## 8. Thread Safety via RWMutex

### Decision: Each major structure has its own sync.RWMutex
**Rationale**:
- Allows concurrent reads
- Safe mutations when needed
- Per-object locking reduces contention
- Simple to understand and maintain

**Trade-offs**:
- ✅ Safe by default
- ✅ Good read performance
- ✅ Simple implementation
- ❌ Locking overhead
- ❌ Can't optimize for single-threaded use

## 9. Column-Oriented Storage

### Decision: DataFrame stores columns, not rows
**Rationale**:
- Optimal for analytical queries
- Cache-friendly for column operations
- Aligns with Arrow's design
- Enables vectorized operations

**Trade-offs**:
- ✅ Fast column operations
- ✅ Memory efficient
- ✅ SIMD-friendly
- ❌ Row operations are slower
- ❌ Transposing is expensive

## 10. Lazy Evaluation Preparation

### Decision: Design APIs to support future lazy evaluation
**Rationale**:
- Expressions are already declarative
- Can add query optimization later
- Maintains API compatibility
- Follows Polars' successful pattern

**Trade-offs**:
- ✅ Future-proof API
- ✅ Enables optimizations
- ✅ Better for large data
- ❌ More complex initial design
- ❌ Some overhead for eager mode

## 11. No External DSL

### Decision: Use Go methods/functions instead of string-based query language
**Rationale**:
- Type safety at compile time
- IDE autocompletion works
- No parsing overhead
- Refactoring-friendly

**Trade-offs**:
- ✅ Type safe
- ✅ Better tooling support
- ✅ No parsing errors
- ❌ More verbose than SQL
- ❌ Steeper learning curve

## 12. Separate I/O Package

### Decision: I/O operations in separate packages (csv, json, parquet)
**Rationale**:
- Keep core library focused
- Optional dependencies
- Easier to add new formats
- Clear separation of concerns

**Trade-offs**:
- ✅ Modular design
- ✅ Smaller core
- ✅ Format-specific optimizations
- ❌ More packages to manage
- ❌ Potential API inconsistencies

## Alternative Approaches Considered

### 1. Row-Based Storage
**Rejected because**: Poor cache locality for analytical operations

### 2. Interface{} Everywhere
**Rejected because**: Loss of type safety, poor performance

### 3. Code Generation for Types
**Rejected because**: Complex build process, Go generics are sufficient

### 4. Mutable Operations
**Rejected because**: Thread safety issues, harder to reason about

### 5. Panic on Errors
**Rejected because**: Not idiomatic Go, poor user experience

## Future Design Considerations

1. **Memory Pool Management**: Add pooling for frequently allocated objects
2. **SIMD Optimizations**: Use Go assembly for critical paths
3. **Query Optimization**: Add cost-based optimizer for lazy evaluation
4. **Distributed Execution**: Design with distribution in mind
5. **GPU Acceleration**: Consider GPU kernels for large operations