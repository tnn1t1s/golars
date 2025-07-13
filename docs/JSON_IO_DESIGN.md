# JSON/NDJSON I/O Design Document

## Overview

This document outlines the design for JSON and NDJSON (Newline Delimited JSON) support in Golars. JSON is a ubiquitous data interchange format, and NDJSON is particularly useful for streaming large datasets.

## Goals

1. **Read JSON/NDJSON files** into DataFrames
2. **Write DataFrames** to JSON/NDJSON format
3. **Support streaming** for large files
4. **Type inference** from JSON data
5. **Schema validation** options
6. **Nested object handling**

## Design Decisions

### File Formats

#### JSON Format
Standard JSON with array of objects:
```json
[
  {"name": "Alice", "age": 30, "active": true},
  {"name": "Bob", "age": 25, "active": false}
]
```

#### NDJSON Format
One JSON object per line:
```json
{"name": "Alice", "age": 30, "active": true}
{"name": "Bob", "age": 25, "active": false}
```

### API Design

#### Reading JSON

```go
// Read entire JSON file
df, err := golars.ReadJSON("data.json")

// With options
df, err := golars.ReadJSON("data.json",
    golars.WithSchema(schema),           // Optional schema
    golars.WithInferSchema(true),        // Type inference (default: true)
    golars.WithMaxRecords(1000),         // Limit records for inference
    golars.WithColumns([]string{"name", "age"}), // Select columns
)

// Read NDJSON
df, err := golars.ReadNDJSON("data.ndjson",
    golars.WithChunkSize(10000),         // Process in chunks
    golars.WithParallel(true),           // Parallel processing
)
```

#### Writing JSON

```go
// Write to JSON
err := df.WriteJSON("output.json",
    golars.WithPretty(true),             // Pretty print
    golars.WithOrient("records"),        // Output orientation
)

// Write to NDJSON
err := df.WriteNDJSON("output.ndjson",
    golars.WithCompression("gzip"),      // Optional compression
)
```

### Type Mapping

| JSON Type | Golars Type | Notes |
|-----------|-------------|-------|
| number (int) | Int64 | Can be configured to Int32 |
| number (float) | Float64 | Preserves decimals |
| string | String | UTF-8 encoded |
| boolean | Bool | Native boolean |
| null | null | Handled in all types |
| array | List (future) | Or error/skip |
| object | Struct (future) | Or flatten/skip |

### Schema Inference

1. **Sample-based**: Read first N records to infer types
2. **Full scan**: Read entire file for accurate inference
3. **User-provided**: Use explicit schema
4. **Mixed types**: Promote to most general type

### Nested Data Handling

#### Flattening Strategy
```json
{"user": {"name": "Alice", "age": 30}, "active": true}
```
Becomes:
- `user.name`: "Alice"
- `user.age`: 30
- `active`: true

#### Options
1. **Flatten** (default): Nested objects become dot-notation columns
2. **Ignore**: Skip nested fields
3. **Error**: Fail on nested data
4. **Stringify**: Convert nested objects to JSON strings

## Implementation Plan

### Phase 1: Basic JSON Reading
- [ ] JSON parser integration (encoding/json)
- [ ] Type inference engine
- [ ] Basic DataFrame construction
- [ ] Error handling

### Phase 2: NDJSON Support
- [ ] Line-by-line reader
- [ ] Streaming parser
- [ ] Chunk processing
- [ ] Memory management

### Phase 3: Writing Support
- [ ] DataFrame to JSON serialization
- [ ] NDJSON writer
- [ ] Compression support
- [ ] Configuration options

### Phase 4: Advanced Features
- [ ] Nested data flattening
- [ ] Custom type converters
- [ ] Parallel processing
- [ ] Schema evolution

## Technical Architecture

### Components

```go
// Reader interface
type JSONReader interface {
    Read(io.Reader) (*DataFrame, error)
    ReadFile(string) (*DataFrame, error)
}

// Writer interface
type JSONWriter interface {
    Write(io.Writer, *DataFrame) error
    WriteFile(string, *DataFrame) error
}

// Options
type JSONOptions struct {
    Schema       *Schema
    InferSchema  bool
    MaxRecords   int
    Columns      []string
    SkipInvalid  bool
    Flatten      bool
    DateFormat   string
}
```

### Type Inference Algorithm

```go
func inferType(values []interface{}) DataType {
    // 1. Check for nulls
    // 2. Try parsing as bool
    // 3. Try parsing as int
    // 4. Try parsing as float
    // 5. Try parsing as date
    // 6. Default to string
}
```

## Performance Considerations

1. **Streaming**: Process large files without loading entirely into memory
2. **Chunking**: Read NDJSON in configurable chunks
3. **Parallel**: Parse multiple chunks concurrently
4. **Type caching**: Cache inferred types to avoid re-parsing
5. **Buffer pooling**: Reuse buffers for better memory usage

## Error Handling

1. **Malformed JSON**: Skip or fail with clear error
2. **Type mismatches**: Promote types or fail
3. **Missing fields**: Use null values
4. **Extra fields**: Ignore or include based on schema

## Testing Strategy

1. **Unit tests**: Type inference, parsing logic
2. **Integration tests**: Full read/write cycle
3. **Performance tests**: Large file handling
4. **Edge cases**: Malformed data, mixed types
5. **Compatibility**: Test with various JSON producers

## Example Usage

```go
// Simple read
df, err := golars.ReadJSON("users.json")
if err != nil {
    log.Fatal(err)
}

// Complex read with options
schema := golars.NewSchema().
    WithColumn("id", golars.Int64Type).
    WithColumn("name", golars.StringType).
    WithColumn("score", golars.Float64Type)

df, err := golars.ReadJSON("data.json",
    golars.WithSchema(schema),
    golars.WithMaxRecords(10000),
)

// Process NDJSON stream
df, err := golars.ReadNDJSON("events.ndjson",
    golars.WithChunkSize(5000),
    golars.WithColumns([]string{"timestamp", "event", "user_id"}),
)

// Write with compression
err = df.WriteNDJSON("output.ndjson.gz",
    golars.WithCompression("gzip"),
)
```

## Future Enhancements

1. **JSON Path queries**: Select nested data with path expressions
2. **Streaming writes**: Write large DataFrames in chunks
3. **Custom serializers**: User-defined type conversions
4. **JSON Schema validation**: Validate against JSON Schema
5. **Lazy evaluation**: Defer parsing until needed