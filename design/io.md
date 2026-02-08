# io -- Input/Output Layer

## Purpose

Read and write DataFrames in CSV, Parquet, and JSON formats. The top-level
`io` package defines option types and delegates to sub-packages.

## Key Design Decisions

**Functional options pattern.** Each format has option types:
- `CSVReadOption func(*csv.ReadOptions)` -- delimiter, header, skip rows, etc.
- `ParquetReadOption func(*parquet.ReadOptions)` -- columns, row groups, batch size
- `CSVWriteOption`, `ParquetWriteOption`, etc.

The top-level `io.go` file defines these option types and the public
`ReadCSV`, `WriteCSV`, `ReadParquet`, `WriteParquet` functions that apply
options then delegate to the sub-package.

**CSV (io/csv).** Uses Go's `encoding/csv` reader. `reader.go` reads the file,
optionally skips rows, infers types from the first N rows (configurable via
`WithInferSchemaRows`), then builds typed Series for each column. Type
inference examines values and picks the narrowest type (bool > int > float >
string). `writer.go` writes Series values as CSV rows.

**Parquet (io/parquet).** Uses `github.com/apache/arrow-go/v18/parquet` for
reading and writing. `reader.go` opens a Parquet file, reads row groups,
converts Arrow record batches to Series. Supports column projection
(reading only requested columns), parallel row group reading, and memory
mapping. `writer.go` converts Series to Arrow arrays, writes them as
Parquet row groups with configurable compression (Snappy, Gzip, Zstd, LZ4).

**JSON (io/json).** Three sub-components:
- `reader.go` -- reads JSON arrays of objects into DataFrames
- `ndjson_reader.go` -- reads newline-delimited JSON (one object per line)
- `writer.go` -- writes DataFrames as JSON arrays or NDJSON
- `api.go` -- public API (ReadJSON, WriteJSON) that auto-detects format

`test_helpers.go` is preserved (not stubbed) as it provides test fixtures.

**Schema inference.** All readers must infer column types when not explicitly
provided. The pattern: read a sample of values, determine the type, then
convert the full column. CSV uses string parsing heuristics; Parquet and JSON
get types from the format metadata.
