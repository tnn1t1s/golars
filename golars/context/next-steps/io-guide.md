# I/O Operations Implementation Guide

## Overview

Implement readers and writers for common data formats:
- CSV (Comma-Separated Values)
- JSON (JavaScript Object Notation)
- Parquet (columnar storage format)
- Excel (XLSX)

## CSV Implementation

### CSV Reader

```go
// golars/io/csv/reader.go
package csv

import (
    "encoding/csv"
    "io"
    "strconv"
)

type CSVReader struct {
    reader      *csv.Reader
    options     CSVReadOptions
    inferTypes  bool
    chunkSize   int
}

type CSVReadOptions struct {
    Delimiter       rune
    Header          bool
    SkipRows        int
    Columns         []string  // Specific columns to read
    DataTypes       map[string]datatypes.DataType
    NullValues      []string  // Strings to treat as null
    ParseDates      bool
    DateFormat      string
    ChunkSize       int       // For reading large files
    InferSchemaRows int       // Rows to scan for type inference
}

func NewCSVReader(r io.Reader, options CSVReadOptions) *CSVReader {
    csvReader := csv.NewReader(r)
    csvReader.Comma = options.Delimiter
    csvReader.ReuseRecord = true
    
    return &CSVReader{
        reader:     csvReader,
        options:    options,
        inferTypes: len(options.DataTypes) == 0,
        chunkSize:  options.ChunkSize,
    }
}

func (r *CSVReader) Read() (*frame.DataFrame, error) {
    // Skip rows if needed
    for i := 0; i < r.options.SkipRows; i++ {
        if _, err := r.reader.Read(); err != nil {
            return nil, err
        }
    }
    
    // Read header
    var headers []string
    if r.options.Header {
        record, err := r.reader.Read()
        if err != nil {
            return nil, err
        }
        headers = record
    }
    
    // Infer schema if needed
    schema, err := r.inferSchema(headers)
    if err != nil {
        return nil, err
    }
    
    // Read all data
    builders := r.createBuilders(schema)
    
    for {
        record, err := r.reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        
        if err := r.parseRecord(record, builders, schema); err != nil {
            return nil, err
        }
    }
    
    // Build DataFrame
    return r.buildDataFrame(builders, schema)
}

func (r *CSVReader) inferSchema(headers []string) (*datatypes.Schema, error) {
    // Read sample rows for type inference
    samples := make([][]string, 0, r.options.InferSchemaRows)
    position := 0
    
    for i := 0; i < r.options.InferSchemaRows; i++ {
        record, err := r.reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        samples = append(samples, append([]string{}, record...))
        position++
    }
    
    // Infer types from samples
    fields := make([]datatypes.Field, len(headers))
    for i, header := range headers {
        dtype := r.inferColumnType(i, samples)
        fields[i] = datatypes.Field{
            Name:     header,
            DataType: dtype,
            Nullable: r.hasNulls(i, samples),
        }
    }
    
    // Reset reader to beginning of data
    // This is tricky with io.Reader - might need to buffer
    
    return &datatypes.Schema{Fields: fields}, nil
}

func (r *CSVReader) inferColumnType(colIdx int, samples [][]string) datatypes.DataType {
    // Check if user specified type
    if r.options.DataTypes != nil {
        if dtype, exists := r.options.DataTypes[headers[colIdx]]; exists {
            return dtype
        }
    }
    
    // Try parsing as different types
    isInt := true
    isFloat := true
    isDate := r.options.ParseDates
    isBool := true
    
    for _, row := range samples {
        if colIdx >= len(row) {
            continue
        }
        
        val := row[colIdx]
        if r.isNull(val) {
            continue
        }
        
        // Try integer
        if isInt {
            if _, err := strconv.ParseInt(val, 10, 64); err != nil {
                isInt = false
            }
        }
        
        // Try float
        if isFloat && !isInt {
            if _, err := strconv.ParseFloat(val, 64); err != nil {
                isFloat = false
            }
        }
        
        // Try boolean
        if isBool {
            lower := strings.ToLower(val)
            if lower != "true" && lower != "false" && 
               lower != "1" && lower != "0" {
                isBool = false
            }
        }
        
        // Try date
        if isDate {
            if _, err := time.Parse(r.options.DateFormat, val); err != nil {
                isDate = false
            }
        }
    }
    
    // Return most specific type that works
    if isInt {
        return datatypes.Int64
    }
    if isFloat {
        return datatypes.Float64
    }
    if isBool {
        return datatypes.Boolean
    }
    if isDate {
        return datatypes.Date
    }
    
    return datatypes.String
}
```

### CSV Writer

```go
// golars/io/csv/writer.go
package csv

type CSVWriter struct {
    writer  *csv.Writer
    options CSVWriteOptions
}

type CSVWriteOptions struct {
    Delimiter   rune
    Header      bool
    NullValue   string
    DateFormat  string
    FloatFormat string
}

func NewCSVWriter(w io.Writer, options CSVWriteOptions) *CSVWriter {
    csvWriter := csv.NewWriter(w)
    csvWriter.Comma = options.Delimiter
    
    return &CSVWriter{
        writer:  csvWriter,
        options: options,
    }
}

func (w *CSVWriter) Write(df *frame.DataFrame) error {
    // Write header
    if w.options.Header {
        headers := make([]string, df.Width())
        for i, col := range df.Columns() {
            headers[i] = col.Name()
        }
        if err := w.writer.Write(headers); err != nil {
            return err
        }
    }
    
    // Write data rows
    record := make([]string, df.Width())
    for i := 0; i < df.Height(); i++ {
        for j, col := range df.Columns() {
            record[j] = w.formatValue(col, i)
        }
        if err := w.writer.Write(record); err != nil {
            return err
        }
    }
    
    w.writer.Flush()
    return w.writer.Error()
}

func (w *CSVWriter) formatValue(col series.Series, idx int) string {
    if col.IsNull(idx) {
        return w.options.NullValue
    }
    
    val := col.Get(idx)
    
    switch v := val.(type) {
    case float64:
        if w.options.FloatFormat != "" {
            return fmt.Sprintf(w.options.FloatFormat, v)
        }
        return strconv.FormatFloat(v, 'g', -1, 64)
    case time.Time:
        if w.options.DateFormat != "" {
            return v.Format(w.options.DateFormat)
        }
        return v.Format(time.RFC3339)
    default:
        return fmt.Sprint(v)
    }
}
```

## JSON Implementation

### JSON Reader

```go
// golars/io/json/reader.go
package json

type JSONReader struct {
    options JSONReadOptions
}

type JSONReadOptions struct {
    Orient      string // "records", "columns", "index"
    DateFormat  string
    ParseDates  bool
}

func ReadJSON(r io.Reader, options JSONReadOptions) (*frame.DataFrame, error) {
    decoder := json.NewDecoder(r)
    
    switch options.Orient {
    case "records":
        return readRecordsOriented(decoder, options)
    case "columns":
        return readColumnsOriented(decoder, options)
    default:
        return nil, fmt.Errorf("unsupported orient: %s", options.Orient)
    }
}

func readRecordsOriented(decoder *json.Decoder, options JSONReadOptions) (*frame.DataFrame, error) {
    var records []map[string]interface{}
    if err := decoder.Decode(&records); err != nil {
        return nil, err
    }
    
    if len(records) == 0 {
        return frame.NewDataFrame()
    }
    
    // Extract columns
    columns := make(map[string][]interface{})
    
    // Get all unique keys
    keys := make(map[string]bool)
    for _, record := range records {
        for k := range record {
            keys[k] = true
        }
    }
    
    // Initialize columns
    for k := range keys {
        columns[k] = make([]interface{}, 0, len(records))
    }
    
    // Fill columns
    for _, record := range records {
        for k := range keys {
            if val, exists := record[k]; exists {
                columns[k] = append(columns[k], val)
            } else {
                columns[k] = append(columns[k], nil)
            }
        }
    }
    
    // Create DataFrame
    return frame.NewDataFrameFromMap(columns)
}
```

### JSON Writer

```go
// golars/io/json/writer.go
func WriteJSON(w io.Writer, df *frame.DataFrame, options JSONWriteOptions) error {
    encoder := json.NewEncoder(w)
    if options.Indent {
        encoder.SetIndent("", "  ")
    }
    
    switch options.Orient {
    case "records":
        return writeRecordsOriented(encoder, df, options)
    case "columns":
        return writeColumnsOriented(encoder, df, options)
    default:
        return fmt.Errorf("unsupported orient: %s", options.Orient)
    }
}
```

## Parquet Implementation

### Parquet Reader (using parquet-go)

```go
// golars/io/parquet/reader.go
package parquet

import (
    "github.com/xitongsys/parquet-go/parquet"
    "github.com/xitongsys/parquet-go/reader"
)

func ReadParquet(filename string, options ParquetReadOptions) (*frame.DataFrame, error) {
    fr, err := local.NewLocalFileReader(filename)
    if err != nil {
        return nil, err
    }
    defer fr.Close()
    
    pr, err := reader.NewParquetReader(fr, nil, 4)
    if err != nil {
        return nil, err
    }
    defer pr.ReadStop()
    
    // Get schema
    schema := pr.SchemaHandler.GetSchema()
    
    // Read columns
    columns := make([]series.Series, 0)
    
    for _, field := range schema.Fields {
        colData, err := readColumn(pr, field)
        if err != nil {
            return nil, err
        }
        
        series := createSeriesFromParquet(field.Name, colData, field.Type)
        columns = append(columns, series)
    }
    
    return frame.NewDataFrame(columns...)
}
```

## Public API

```go
// golars/io.go
package golars

// CSV functions
func ReadCSV(filename string, options ...CSVReadOption) (*DataFrame, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    opts := defaultCSVReadOptions()
    for _, opt := range options {
        opt(&opts)
    }
    
    reader := csv.NewCSVReader(file, opts)
    return reader.Read()
}

func (df *DataFrame) ToCSV(filename string, options ...CSVWriteOption) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    opts := defaultCSVWriteOptions()
    for _, opt := range options {
        opt(&opts)
    }
    
    writer := csv.NewCSVWriter(file, opts)
    return writer.Write(df)
}

// JSON functions
func ReadJSON(filename string, options ...JSONReadOption) (*DataFrame, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    opts := defaultJSONReadOptions()
    for _, opt := range options {
        opt(&opts)
    }
    
    return json.ReadJSON(file, opts)
}

// Option functions
func WithDelimiter(d rune) CSVReadOption {
    return func(o *CSVReadOptions) {
        o.Delimiter = d
    }
}

func WithHeader(h bool) CSVReadOption {
    return func(o *CSVReadOptions) {
        o.Header = h
    }
}
```

## Usage Examples

```go
// Read CSV with options
df, err := golars.ReadCSV("data.csv",
    golars.WithDelimiter(';'),
    golars.WithHeader(true),
    golars.WithNullValues([]string{"NA", "null"}),
)

// Write CSV
err = df.ToCSV("output.csv",
    golars.WithHeader(true),
    golars.WithFloatFormat("%.2f"),
)

// Read JSON
df, err = golars.ReadJSON("data.json",
    golars.WithOrient("records"),
)

// Streaming large CSV
reader := golars.NewCSVReader("large.csv",
    golars.WithChunkSize(10000),
)

for {
    chunk, err := reader.ReadChunk()
    if err == io.EOF {
        break
    }
    // Process chunk
}
```

## Performance Optimizations

1. **Buffered I/O**: Use bufio for better performance
2. **Parallel parsing**: Parse different columns in parallel
3. **Memory pre-allocation**: Estimate size and pre-allocate
4. **Type-specific parsing**: Avoid interface{} in hot paths
5. **Streaming**: Support chunked reading for large files

## Testing

```go
func TestCSVReader(t *testing.T) {
    // Test basic CSV
    csv := `name,age,score
Alice,25,95.5
Bob,30,87.0
Charlie,35,`
    
    df, err := ReadCSVString(csv)
    assert.NoError(t, err)
    assert.Equal(t, 3, df.Height())
    
    // Test type inference
    age, _ := df.Column("age")
    assert.Equal(t, datatypes.Int64, age.DataType())
    
    // Test null handling
    score, _ := df.Column("score")
    assert.True(t, score.IsNull(2))
}
```

## Edge Cases

1. **Empty files**: Return empty DataFrame
2. **Inconsistent columns**: Handle ragged CSV
3. **Large values**: Use appropriate numeric types
4. **Special characters**: Handle quotes, escapes
5. **BOM**: Handle UTF-8 BOM
6. **Memory limits**: Stream large files