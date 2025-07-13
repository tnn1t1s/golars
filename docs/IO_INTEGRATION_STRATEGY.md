# Golars I/O Integration Strategy

## Core Philosophy

Golars maintains a focused scope for I/O operations:
- **Built-in**: CSV and Parquet readers/writers that work with any filesystem (local SSD, NAS, FUSE-mounted cloud storage)
- **External**: All other formats and connectors are implemented as separate packages

## Built-in I/O Support

### CSV
- Full read/write support with type inference
- Streaming capabilities for large files
- Works over any mounted filesystem (including FUSE-mounted GCS, S3, etc.)
- Comprehensive options (delimiters, headers, encoding, etc.)

### Parquet
- Full read/write support with Arrow integration
- Compression support (Snappy, Gzip, Zstd, LZ4)
- Predicate pushdown for efficient filtering
- Column pruning for selective reading
- Works over any mounted filesystem

### Design Principle
Both CSV and Parquet readers work with `io.Reader`/`io.Writer` interfaces, enabling:
- Local file system access
- Network-attached storage (NAS)
- FUSE-mounted cloud storage (GCS, S3, Azure via gcsfuse, s3fs, etc.)
- Any Go-compatible I/O stream

## Integration Interface

### Reader Interface
```go
// DataFrameReader defines the interface for external format readers
type DataFrameReader interface {
    // Read reads data from the source and returns a DataFrame
    Read() (*DataFrame, error)
    
    // ReadLazy returns a LazyFrame for deferred execution
    ReadLazy() (*LazyFrame, error)
}

// ReaderOption allows configuration of readers
type ReaderOption interface {
    Apply(interface{}) error
}
```

### Writer Interface
```go
// DataFrameWriter defines the interface for external format writers
type DataFrameWriter interface {
    // Write writes a DataFrame to the destination
    Write(df *DataFrame) error
    
    // WriteLazy writes a LazyFrame (triggers collection)
    WriteLazy(lf *LazyFrame) error
}

// WriterOption allows configuration of writers
type WriterOption interface {
    Apply(interface{}) error
}
```

### Registration System
```go
// RegisterReader registers a reader factory for a format
func RegisterReader(format string, factory ReaderFactory) error

// RegisterWriter registers a writer factory for a format
func RegisterWriter(format string, factory WriterFactory) error

// ReaderFactory creates readers for a specific format
type ReaderFactory func(source io.Reader, opts ...ReaderOption) DataFrameReader

// WriterFactory creates writers for a specific format
type WriterFactory func(dest io.Writer, opts ...WriterOption) DataFrameWriter
```

## External Connector Strategy

### Separate Packages
External connectors should be implemented as separate Go modules:

```
github.com/davidpalaitis/golars-excel     # Excel support
github.com/davidpalaitis/golars-postgres  # PostgreSQL connector
github.com/davidpalaitis/golars-mysql     # MySQL connector
github.com/davidpalaitis/golars-sqlite    # SQLite connector
github.com/davidpalaitis/golars-avro      # Avro format
github.com/davidpalaitis/golars-arrow-ipc # Arrow IPC format
github.com/davidpalaitis/golars-s3        # Native S3 support (no FUSE)
```

### Example: Excel Connector
```go
// In github.com/davidpalaitis/golars-excel
package excel

import (
    "github.com/davidpalaitis/golars"
    "github.com/xuri/excelize/v2"
)

type ExcelReader struct {
    file *excelize.File
    sheet string
    options *ExcelOptions
}

func init() {
    golars.RegisterReader("excel", NewExcelReader)
    golars.RegisterReader("xlsx", NewExcelReader)
}

func NewExcelReader(source io.Reader, opts ...golars.ReaderOption) golars.DataFrameReader {
    // Implementation
}
```

### Example: PostgreSQL Connector
```go
// In github.com/davidpalaitis/golars-postgres
package postgres

import (
    "github.com/davidpalaitis/golars"
    "github.com/jackc/pgx/v5"
)

type PostgresReader struct {
    conn *pgx.Conn
    query string
    options *PostgresOptions
}

func NewPostgresReader(connString string, query string, opts ...golars.ReaderOption) golars.DataFrameReader {
    // Implementation
}
```

## Usage Examples

### Built-in Formats
```go
// CSV - works over any filesystem
df, err := golars.ReadCSV("/mnt/gcs/data.csv")
df, err := golars.ReadCSV("/nas/shared/data.csv")
df, err := golars.ReadCSV("/home/user/data.csv")

// Parquet - works over any filesystem  
df, err := golars.ReadParquet("/mnt/s3/data.parquet")
```

### External Formats (user imports)
```go
import (
    "github.com/davidpalaitis/golars"
    _ "github.com/davidpalaitis/golars-excel"     // Registers excel format
    _ "github.com/davidpalaitis/golars-postgres"  // Registers postgres connector
)

// Excel (after importing golars-excel)
df, err := golars.Read("data.xlsx", golars.Format("excel"))

// PostgreSQL (after importing golars-postgres)
df, err := golars.Read("postgres://localhost/db", 
    golars.Format("postgres"),
    golars.Query("SELECT * FROM users"))
```

## Benefits

1. **Clean Separation**: Core library stays focused on DataFrames and computation
2. **Flexibility**: Users only import what they need
3. **Maintainability**: External formats can be updated independently
4. **Community**: Others can contribute format readers without touching core
5. **Testing**: Each connector can be tested in isolation
6. **Dependencies**: Format-specific dependencies don't bloat the core

## Implementation Guidelines for External Connectors

1. **Separate Repository**: Each connector in its own repo/module
2. **Minimal Dependencies**: Only depend on golars core interfaces
3. **Comprehensive Tests**: Include integration tests with real systems
4. **Documentation**: Clear examples and limitations
5. **Error Handling**: Consistent with golars error patterns
6. **Performance**: Batch operations, connection pooling where applicable
7. **Type Mapping**: Clear documentation of type conversions

## Future Considerations

1. **Plugin System**: Consider Go plugins for dynamic loading
2. **Schema Registry**: For formats that support schema evolution
3. **Streaming**: Ensure interface supports streaming for large datasets
4. **Transactions**: For database writers, support transactional writes
5. **Catalog API**: For discovering available tables/schemas

This strategy keeps Golars core focused while enabling a rich ecosystem of connectors maintained by the community.