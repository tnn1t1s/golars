package parquet

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
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
		Allocator:        memory.NewGoAllocator(),
	}
}

// Writer writes DataFrames to Parquet files
type Writer struct {
	opts WriterOptions
}

// NewWriter creates a new Parquet writer
func NewWriter(opts WriterOptions) *Writer {
	if opts.Allocator == nil {
		opts.Allocator = memory.NewGoAllocator()
	}
	return &Writer{opts: opts}
}

// WriteFile writes a DataFrame to a Parquet file
func (w *Writer) WriteFile(df *frame.DataFrame, filename string) error {
	// Convert DataFrame to Arrow table
	table, err := w.dataFrameToTable(df)
	if err != nil {
		return fmt.Errorf("failed to convert DataFrame to Arrow table: %w", err)
	}
	defer table.Release()

	// Create output file
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// Create writer properties
	writerProps := w.createWriterProperties()

	// Create arrow writer properties
	arrowProps := pqarrow.DefaultWriterProps()

	// Create parquet file writer
	pqWriter, err := pqarrow.NewFileWriter(table.Schema(), f, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Write the table
	if err := pqWriter.WriteTable(table, int64(table.NumRows())); err != nil {
		pqWriter.Close()
		return fmt.Errorf("failed to write table: %w", err)
	}

	return pqWriter.Close()
}

// createWriterProperties creates Parquet writer properties from options
func (w *Writer) createWriterProperties() *parquet.WriterProperties {
	// Build options list
	opts := []parquet.WriterProperty{
		parquet.WithCompression(w.getCompressionCodec()),
	}

	// Add dictionary encoding option
	if w.opts.UseDictionary {
		opts = append(opts, parquet.WithDictionaryDefault(true))
	} else {
		opts = append(opts, parquet.WithDictionaryDefault(false))
	}

	if w.opts.PageSize > 0 {
		opts = append(opts, parquet.WithDataPageSize(w.opts.PageSize))
	}

	return parquet.NewWriterProperties(opts...)
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
	// Empty table
	if df.Width() == 0 {
		schema := arrow.NewSchema(nil, nil)
		return array.NewTable(schema, nil, 0), nil
	}

	// Build schema and columns
	fields := make([]arrow.Field, df.Width())
	columns := make([]arrow.Column, df.Width())

	for i := 0; i < df.Width(); i++ {
		s, err := df.ColumnAt(i)
		if err != nil {
			return nil, err
		}

		// Convert series to arrow column
		field, col, err := w.seriesToArrowColumn(s)
		if err != nil {
			return nil, fmt.Errorf("failed to convert series %s: %w", s.Name(), err)
		}
		fields[i] = field
		columns[i] = *col
	}

	// Create schema
	schema := arrow.NewSchema(fields, nil)

	// Create table
	colPtrs := make([]arrow.Column, len(columns))
	copy(colPtrs, columns)

	return array.NewTable(schema, colPtrs, int64(df.Height())), nil
}

// seriesToArrowColumn converts a Golars Series to an Arrow column
func (w *Writer) seriesToArrowColumn(s series.Series) (arrow.Field, *arrow.Column, error) {
	name := s.Name()

	// Get the appropriate Arrow data type and convert
	switch s.DataType().(type) {
	case datatypes.Boolean:
		return w.boolSeriesToArrow(name, s)
	case datatypes.Int8:
		return w.int8SeriesToArrow(name, s)
	case datatypes.Int16:
		return w.int16SeriesToArrow(name, s)
	case datatypes.Int32:
		return w.int32SeriesToArrow(name, s)
	case datatypes.Int64:
		return w.int64SeriesToArrow(name, s)
	case datatypes.UInt8:
		return w.uint8SeriesToArrow(name, s)
	case datatypes.UInt16:
		return w.uint16SeriesToArrow(name, s)
	case datatypes.UInt32:
		return w.uint32SeriesToArrow(name, s)
	case datatypes.UInt64:
		return w.uint64SeriesToArrow(name, s)
	case datatypes.Float32:
		return w.float32SeriesToArrow(name, s)
	case datatypes.Float64:
		return w.float64SeriesToArrow(name, s)
	case datatypes.String:
		return w.stringSeriesToArrow(name, s)
	default:
		return arrow.Field{}, nil, fmt.Errorf("unsupported data type: %s", s.DataType().String())
	}
}

// Type-specific conversion methods

func (w *Writer) boolSeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: true}
	builder := array.NewBooleanBuilder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(bool))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) int8SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int8, Nullable: true}
	builder := array.NewInt8Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(int8))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) int16SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int16, Nullable: true}
	builder := array.NewInt16Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(int16))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) int32SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	builder := array.NewInt32Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(int32))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) int64SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	builder := array.NewInt64Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(int64))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) uint8SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint8, Nullable: true}
	builder := array.NewUint8Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(uint8))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) uint16SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint16, Nullable: true}
	builder := array.NewUint16Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(uint16))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) uint32SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint32, Nullable: true}
	builder := array.NewUint32Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(uint32))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) uint64SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint64, Nullable: true}
	builder := array.NewUint64Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(uint64))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) float32SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32, Nullable: true}
	builder := array.NewFloat32Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(float32))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) float64SeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: true}
	builder := array.NewFloat64Builder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(float64))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
	return field, col, nil
}

func (w *Writer) stringSeriesToArrow(name string, s series.Series) (arrow.Field, *arrow.Column, error) {
	field := arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
	builder := array.NewStringBuilder(w.opts.Allocator)
	defer builder.Release()

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(s.Get(i).(string))
		}
	}

	arr := builder.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
	col := arrow.NewColumn(field, chunked)
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
