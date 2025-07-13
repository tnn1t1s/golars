# Changelog

All notable changes to Golars will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Query optimization framework with predicate and projection pushdown
- Expression optimization combining multiple filters with AND
- Plan inspection methods: Explain() and ExplainOptimized()
- Comprehensive optimizer test suite and benchmarks
- Performance improvements for chained operations through lazy evaluation

### Fixed
- Projection pushdown now correctly preserves columns needed by filters
- AND expression combination in predicate pushdown optimizer

## [0.2.0] - 2024-01-XX

### Added
- Lazy evaluation framework with LazyFrame API
- Logical plan representation for query building
- Query execution engine
- CSV scan support for lazy evaluation
- Comprehensive lazy evaluation examples

### Core Features
- All DataFrame operations available in lazy mode
- Thread-safe lazy operations
- Integration with existing eager operations

## [0.1.0] - 2024-01-XX

### Added

#### Core Data Structures
- ChunkedArray: Generic columnar storage using Apache Arrow
- Series: Type-erased wrapper with dynamic dispatch
- DataFrame: Column-oriented table with schema validation
- Full null value support throughout

#### Type System
- Complete DataType definitions for all basic types
- Complex types: List, Array, Struct, Categorical, Decimal
- Schema system with Field definitions
- Full Apache Arrow integration

#### Expression System
- Fluent expression builder API
- Column references and literals
- Binary operations (arithmetic, comparison, logical)
- Aggregation expressions (sum, mean, min, max, count, std, var)
- Conditional expressions (when-then-otherwise)

#### Operations
- **Filtering**: Expression-based with complex boolean logic
- **Selection**: Column projection with computed columns
- **GroupBy**: Single/multi-column grouping with all aggregations
- **Joins**: All join types (Inner, Left, Right, Outer, Cross, Anti, Semi)
- **Sorting**: Single/multi-column with null handling options
- **I/O**: CSV read/write with type inference and custom options

#### Compute Kernels
- Vectorized arithmetic operations
- Comparison operations with null handling
- Aggregation operations optimized for performance
- Scalar-array operations

#### Performance
- Zero-allocation operations where possible
- Thread-safe implementations
- Excellent benchmark results (see PERFORMANCE.md)

### Documentation
- Comprehensive API documentation
- Multiple example programs
- Implementation guides
- Performance benchmarks

## [0.0.1] - 2024-01-01

### Added
- Initial project structure
- Basic DataFrame concept
- Apache Arrow integration planning

---

## Upgrade Guide

### From 0.1.x to 0.2.x
- Lazy evaluation is opt-in, existing code continues to work
- Use `golars.LazyFromDataFrame()` to convert to lazy mode
- Call `.Collect()` to execute lazy operations

### Future Releases
- 0.3.0: Window functions and rolling operations
- 0.4.0: Parquet support and additional I/O formats
- 0.5.0: String and DateTime operations
- 1.0.0: API stability guarantee