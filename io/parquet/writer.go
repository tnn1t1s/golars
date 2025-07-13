package parquet

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// CompressionType represents the compression algorithm to use
type CompressionType int

const (
	CompressionNone CompressionType = iota
	CompressionSnappy
	CompressionGzip
	CompressionZstd
	CompressionLz4
)

// WriterOptions configures parquet writing behavior
type WriterOptions struct {
	// Compression type
	Compression CompressionType
	// Compression level (for gzip and zstd)
	CompressionLevel int
	// Row group size (default 128MB worth of rows)
	RowGroupSize int64
	// Page size (default 1MB)
	PageSize int64
	// Dictionary encoding
	UseDictionary bool
	// Memory allocator
	Allocator memory.Allocator
}

// DefaultWriterOptions returns default writer options
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		Compression:      CompressionSnappy,
		CompressionLevel: -1, // Default level
		RowGroupSize:     128 * 1024 * 1024, // 128MB
		PageSize:         1024 * 1024,       // 1MB
		UseDictionary:    true,
		Allocator:        memory.DefaultAllocator,
	}
}

// Writer writes DataFrames to Parquet files
type Writer struct {
	opts WriterOptions
}

// NewWriter creates a new Parquet writer
func NewWriter(opts WriterOptions) *Writer {
	if opts.Allocator == nil {
		opts.Allocator = memory.DefaultAllocator
	}
	if opts.RowGroupSize <= 0 {
		opts.RowGroupSize = 128 * 1024 * 1024
	}
	if opts.PageSize <= 0 {
		opts.PageSize = 1024 * 1024
	}
	return &Writer{opts: opts}
}

// WriteFile writes a DataFrame to a Parquet file
func (w *Writer) WriteFile(df *frame.DataFrame, filename string) error {
	// Convert DataFrame to Arrow table
	table, err := w.dataFrameToTable(df)
	if err != nil {
		return fmt.Errorf("failed to convert dataframe to table: %w", err)
	}
	defer table.Release()

	// Create output file
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	// Create writer properties
	writerProps := w.createWriterProperties()

	// Create arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithAllocator(w.opts.Allocator),
	)

	// Create parquet file writer
	writer, err := pqarrow.NewFileWriter(table.Schema(), f, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Write the table
	if err := writer.WriteTable(table, w.opts.RowGroupSize); err != nil {
		return fmt.Errorf("failed to write table: %w", err)
	}

	return nil
}

// createWriterProperties creates Parquet writer properties from options
func (w *Writer) createWriterProperties() *parquet.WriterProperties {
	// Build options list
	options := []parquet.WriterProperty{
		parquet.WithAllocator(w.opts.Allocator),
		parquet.WithCompression(w.getCompressionCodec()),
		parquet.WithDataPageSize(w.opts.PageSize),
	}
	
	// Add dictionary encoding option
	if w.opts.UseDictionary {
		options = append(options, parquet.WithDictionaryDefault(true))
	} else {
		options = append(options, parquet.WithDictionaryDefault(false))
	}
	
	// Add compression level if applicable
	if w.opts.CompressionLevel >= 0 {
		switch w.opts.Compression {
		case CompressionGzip, CompressionZstd:
			options = append(options, parquet.WithCompressionLevel(w.opts.CompressionLevel))
		}
	}
	
	return parquet.NewWriterProperties(options...)
}

// getCompressionCodec returns the Arrow compression codec
func (w *Writer) getCompressionCodec() compress.Compression {
	switch w.opts.Compression {
	case CompressionNone:
		return compress.Codecs.Uncompressed
	case CompressionSnappy:
		return compress.Codecs.Snappy
	case CompressionGzip:
		return compress.Codecs.Gzip
	case CompressionZstd:
		return compress.Codecs.Zstd
	case CompressionLz4:
		return compress.Codecs.Lz4
	default:
		return compress.Codecs.Snappy
	}
}

// dataFrameToTable converts a Golars DataFrame to an Arrow table
func (w *Writer) dataFrameToTable(df *frame.DataFrame) (arrow.Table, error) {
	numCols := df.Width()
	if numCols == 0 {
		// Empty table
		schema := arrow.NewSchema([]arrow.Field{}, nil)
		return array.NewTable(schema, []arrow.Column{}, 0), nil
	}

	// Build schema
	fields := make([]arrow.Field, numCols)
	columns := make([]arrow.Column, numCols)
	
	columnNames := df.Columns()
	for i := 0; i < numCols; i++ {
		col, err := df.Column(columnNames[i])
		if err != nil {
			return nil, err
		}
		
		// Convert series to arrow column
		arrowField, arrowColumn, err := w.seriesToArrowColumn(col)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", col.Name(), err)
		}
		
		fields[i] = arrowField
		columns[i] = *arrowColumn
	}
	
	// Create schema
	schema := arrow.NewSchema(fields, nil)
	
	// Create table
	table := array.NewTable(schema, columns, -1)
	
	return table, nil
}

// seriesToArrowColumn converts a Golars Series to an Arrow column
func (w *Writer) seriesToArrowColumn(s series.Series) (arrow.Field, *arrow.Column, error) {
	name := s.Name()
	dtype := s.DataType()
	
	// Get the appropriate Arrow data type and convert
	switch dtype.(type) {
	case datatypes.Boolean:
		return w.boolSeriesToArrow(name, s)
	case datatypes.Int32:
		return w.int32SeriesToArrow(name, s)
	case datatypes.Int64:
		return w.int64SeriesToArrow(name, s)
	case datatypes.Float32:
		return w.float32SeriesToArrow(name, s)
	case datatypes.Float64:
		return w.float64SeriesToArrow(name, s)
	case datatypes.String:
		return w.stringSeriesToArrow(name, s)
	default:
		return arrow.Field{}, nil, fmt.Errorf("unsupported data type: %v", dtype)
	}
}

// Type-specific conversion methods

func (w *Writer) boolSeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	builder := array.NewBooleanBuilder(w.opts.Allocator)
	defer builder.Release()
	
	length := s.Len()
	builder.Reserve(length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val, ok := s.Get(i).(bool)
			if !ok {
				return arrow.Field{}, nil, fmt.Errorf("failed to convert value at index %d to bool", i)
			}
			builder.Append(val)
		}
	}
	
	arr := builder.NewArray()
	field := arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: s.NullCount() > 0}
	chunk := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunk)
	
	return field, col, nil
}

func (w *Writer) int32SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	builder := array.NewInt32Builder(w.opts.Allocator)
	defer builder.Release()
	
	length := s.Len()
	builder.Reserve(length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val, ok := s.Get(i).(int32)
			if !ok {
				return arrow.Field{}, nil, fmt.Errorf("failed to convert value at index %d to int32", i)
			}
			builder.Append(val)
		}
	}
	
	arr := builder.NewArray()
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: s.NullCount() > 0}
	chunk := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunk)
	
	return field, col, nil
}

func (w *Writer) int64SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	builder := array.NewInt64Builder(w.opts.Allocator)
	defer builder.Release()
	
	length := s.Len()
	builder.Reserve(length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val, ok := s.Get(i).(int64)
			if !ok {
				return arrow.Field{}, nil, fmt.Errorf("failed to convert value at index %d to int64", i)
			}
			builder.Append(val)
		}
	}
	
	arr := builder.NewArray()
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: s.NullCount() > 0}
	chunk := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunk)
	
	return field, col, nil
}

func (w *Writer) float32SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	builder := array.NewFloat32Builder(w.opts.Allocator)
	defer builder.Release()
	
	length := s.Len()
	builder.Reserve(length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val, ok := s.Get(i).(float32)
			if !ok {
				return arrow.Field{}, nil, fmt.Errorf("failed to convert value at index %d to float32", i)
			}
			builder.Append(val)
		}
	}
	
	arr := builder.NewArray()
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32, Nullable: s.NullCount() > 0}
	chunk := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunk)
	
	return field, col, nil
}

func (w *Writer) float64SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	builder := array.NewFloat64Builder(w.opts.Allocator)
	defer builder.Release()
	
	length := s.Len()
	builder.Reserve(length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val, ok := s.Get(i).(float64)
			if !ok {
				return arrow.Field{}, nil, fmt.Errorf("failed to convert value at index %d to float64", i)
			}
			builder.Append(val)
		}
	}
	
	arr := builder.NewArray()
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: s.NullCount() > 0}
	chunk := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunk)
	
	return field, col, nil
}

func (w *Writer) stringSeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	builder := array.NewStringBuilder(w.opts.Allocator)
	defer builder.Release()
	
	length := s.Len()
	builder.Reserve(length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val, ok := s.Get(i).(string)
			if !ok {
				return arrow.Field{}, nil, fmt.Errorf("failed to convert value at index %d to string", i)
			}
			builder.Append(val)
		}
	}
	
	arr := builder.NewArray()
	field := arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: s.NullCount() > 0}
	chunk := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunk)
	
	return field, col, nil
}

// WriteParquet is a convenience function to write a DataFrame to Parquet with default options
func WriteParquet(df *frame.DataFrame, filename string) error {
	writer := NewWriter(DefaultWriterOptions())
	return writer.WriteFile(df, filename)
}

// WriteParquetWithOptions writes a DataFrame to Parquet with custom options
func WriteParquetWithOptions(df *frame.DataFrame, filename string, opts WriterOptions) error {
	writer := NewWriter(opts)
	return writer.WriteFile(df, filename)
}