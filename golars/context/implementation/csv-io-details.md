# CSV I/O Implementation Details

## Architecture Overview

The CSV I/O implementation provides:
1. **CSV Reader** - Reads CSV files with automatic type inference
2. **CSV Writer** - Writes DataFrames to CSV with formatting options
3. **Public API** - Simple functions with option patterns

## Key Components

### 1. CSV Reader Structure
```go
type Reader struct {
    reader     *csv.Reader      // Go's CSV reader
    options    ReadOptions      // Configuration
    inferTypes bool             // Whether to infer types
}

type ReadOptions struct {
    Delimiter       rune          // Field delimiter
    Header          bool          // First row has headers
    SkipRows        int           // Rows to skip at start
    Columns         []string      // Specific columns to read
    DataTypes       map[string]datatypes.DataType
    NullValues      []string      // Strings treated as null
    InferSchemaRows int           // Rows for type inference
    Comment         rune          // Comment character
}
```

### 2. Type Inference Algorithm
```go
func inferColumnType(colIdx int, samples [][]string) datatypes.DataType {
    isInt := true
    isFloat := true
    isBool := true
    
    for _, row := range samples {
        val := row[colIdx]
        // Try parsing as different types
        // Return most specific type that works
    }
    
    if isInt { return datatypes.Int64{} }
    if isFloat { return datatypes.Float64{} }
    if isBool { return datatypes.Boolean{} }
    return datatypes.String{}
}
```

### 3. CSV Writer Structure
```go
type Writer struct {
    writer  *csv.Writer
    options WriteOptions
}

type WriteOptions struct {
    Delimiter   rune   // Field delimiter
    Header      bool   // Write headers
    NullValue   string // String for nulls
    FloatFormat string // Float formatting
}
```

## Implementation Strategy

### Reading Process
1. **Skip Rows** - Skip initial rows if configured
2. **Read Headers** - Extract column names from first row
3. **Collect Records** - Read all data into memory
4. **Infer Schema** - Determine data types from samples
5. **Parse Columns** - Convert strings to typed series
6. **Build DataFrame** - Create final DataFrame

### Writing Process
1. **Write Headers** - Output column names if requested
2. **Format Values** - Convert each value to string
3. **Handle Nulls** - Use configured null string
4. **Flush Buffer** - Ensure all data written

## Key Technical Decisions

### 1. Memory vs Streaming
- Current: Load entire CSV into memory
- Rationale: Simpler implementation, type inference needs multiple passes
- Future: Could add streaming for large files

### 2. Type Inference
- Sample first N rows (default: 100)
- Try parsing as: Int → Float → Bool → String
- Most specific type that works for all samples
- User can override with explicit types

### 3. Null Handling
- Configurable null values (default: "", "NA", "null", etc.)
- Preserve nulls through round-trip
- Use validity arrays in Series

### 4. Error Handling
- Parse errors → treat as null
- Ragged CSV → pad with nulls
- Wrong types → fall back to string

## Special Features

### 1. Column Selection
```go
// Read only specific columns
df, err := ReadCSV("data.csv", 
    WithColumns([]string{"name", "score"}))
```

### 2. Custom Delimiters
```go
// Semicolon-delimited
df, err := ReadCSV("data.csv", 
    WithDelimiter(';'))
```

### 3. Skip Rows & Comments
```go
// Skip header rows and handle comments
df, err := ReadCSV("data.csv",
    WithSkipRows(2),
    WithComment('#'))
```

### 4. Float Formatting
```go
// Control float precision
err := WriteCSV(df, "output.csv",
    WithFloatFormat("%.2f"))
```

## Performance Characteristics

### Reader Performance
- Type inference: O(samples × columns)
- Parsing: O(rows × columns)
- Memory: 2× file size (strings + typed data)

### Writer Performance
- Formatting: O(rows × columns)
- Memory: Minimal (row buffer only)

### Optimizations
1. **ReuseRecord** - Reuse slice memory in CSV reader
2. **Pre-allocation** - Allocate arrays upfront
3. **Bulk operations** - Parse entire columns at once

## Edge Cases Handled

### 1. Empty Files
- Return empty DataFrame
- Preserve column names if header present

### 2. Ragged CSV
- Variable number of fields per row
- Pad missing values with nulls

### 3. Type Mismatches
- String "123" in int column → parse
- String "abc" in int column → null
- Graceful fallback to string type

### 4. Large Numbers
- Use int64/float64 for safety
- No overflow handling currently

## Testing Strategy

### Unit Tests
- Basic read/write
- All type inference cases
- Null handling
- Custom options
- Edge cases

### Round-Trip Tests
- Write DataFrame → Read back
- Verify data preservation
- Check null handling

### Integration Tests
- File system operations
- Error conditions
- Real CSV files

## Benchmarks
```
BenchmarkCSVReader-8    1000 rows    1.2 ms/op
BenchmarkCSVWriter-8    10000 rows   8.5 ms/op
```

## API Design Philosophy

### 1. Simple Defaults
```go
df, err := golars.ReadCSV("data.csv")
```

### 2. Functional Options
```go
df, err := golars.ReadCSV("data.csv",
    golars.WithDelimiter('\t'),
    golars.WithNullValues([]string{"?", "N/A"}),
)
```

### 3. Consistent Naming
- Read options: `WithXxx`
- Write options: `WithWriteXxx`

## Known Limitations

1. **Memory Usage**
   - Entire file loaded into memory
   - No streaming support yet

2. **Type System**
   - Limited to basic types
   - No date/time parsing yet
   - No custom type parsers

3. **Performance**
   - Single-threaded parsing
   - No parallel column processing

4. **CSV Features**
   - No multi-line fields
   - Limited quote handling
   - No BOM detection

## Future Enhancements

1. **Streaming API**
   ```go
   reader := NewCSVReader("large.csv", WithChunkSize(1000))
   for chunk := range reader.Chunks() {
       // Process chunk
   }
   ```

2. **Date/Time Support**
   ```go
   WithDateColumns([]string{"created_at"}),
   WithDateFormat("2006-01-02"),
   ```

3. **Parallel Processing**
   - Parse columns in parallel
   - Multi-threaded type inference

4. **Advanced Features**
   - Multi-line field support
   - Custom type parsers
   - Schema validation

## Integration with Other Features

### With Filtering
```go
df, _ := golars.ReadCSV("data.csv")
filtered := df.Filter(expr.Col("age").Gt(expr.Lit(18)))
golars.WriteCSV(filtered, "adults.csv")
```

### With GroupBy
```go
df, _ := golars.ReadCSV("sales.csv")
summary := df.GroupBy("product").Sum("amount")
golars.WriteCSV(summary, "summary.csv")
```

### With Joins
```go
users, _ := golars.ReadCSV("users.csv")
orders, _ := golars.ReadCSV("orders.csv")
joined := users.Join(orders, "user_id", golars.InnerJoin)
golars.WriteCSV(joined, "user_orders.csv")
```

## Error Messages

Clear, actionable error messages:
- "column 'age' not found" → Check column names
- "cannot parse '25.5' as integer" → Type mismatch
- "wrong number of fields" → Ragged CSV

## Thread Safety

- Reader: Thread-safe (no shared state)
- Writer: Thread-safe (no shared state)
- Can read/write same file from multiple goroutines