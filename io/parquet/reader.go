package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	parquetlib "github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/chunked"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// ReaderOptions configures parquet reading behavior
type ReaderOptions struct {
	// Columns to read (nil means all columns)
	Columns []string
	// Row groups to read (nil means all row groups)
	RowGroups []int
	// Number of rows to read (0 means all rows)
	NumRows int64
	// Memory allocator
	Allocator memory.Allocator
	// Read columns in parallel when possible
	Parallel bool
	// Batch size used by the Arrow reader
	BatchSize int64
	// Use buffered stream reader for parquet pages
	BufferedStream bool
	// Buffer size for buffered streams (0 uses default)
	BufferSize int64
	// Use memory-mapped IO when reading local files
	MemoryMap bool
}

// DefaultReaderOptions returns default reader options
func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		Allocator: memory.DefaultAllocator,
		Parallel:  true,
		BatchSize: 1024 * 1024,
		MemoryMap: true,
	}
}

// Reader reads Parquet files into DataFrames
type Reader struct {
	opts ReaderOptions
}

// NewReader creates a new Parquet reader
func NewReader(opts ReaderOptions) *Reader {
	if opts.Allocator == nil {
		opts.Allocator = memory.DefaultAllocator
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1024 * 1024
	}
	return &Reader{opts: opts}
}

// ReadFile reads a Parquet file into a DataFrame
func (r *Reader) ReadFile(filename string) (*frame.DataFrame, error) {
	readProps := parquetlib.NewReaderProperties(r.opts.Allocator)
	if r.opts.BufferSize > 0 {
		readProps.BufferSize = r.opts.BufferSize
	}
	readProps.BufferedStreamEnabled = r.opts.BufferedStream

	// Create parquet file reader
	pqReader, err := file.OpenParquetFile(filename, r.opts.MemoryMap, file.WithReadProps(readProps))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader,
		pqarrow.ArrowReadProperties{
			BatchSize: r.opts.BatchSize,
			Parallel:  r.opts.Parallel,
		}, r.opts.Allocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Get schema
	schema, err := arrowReader.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Select columns if specified
	columnIndices := r.selectColumns(schema)

	// Select row groups if specified
	rowGroups := r.selectRowGroups(pqReader.NumRowGroups())

	// Read using the record reader for batch scanning
	df, err := r.readRecordBatches(arrowReader, columnIndices, rowGroups)
	if err != nil {
		return nil, err
	}
	return df, nil
}

// selectColumns returns the indices of columns to read
func (r *Reader) selectColumns(schema *arrow.Schema) []int {
	if len(r.opts.Columns) == 0 {
		// Read all columns
		indices := make([]int, schema.NumFields())
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Build column name to index map
	nameToIdx := make(map[string]int)
	for i, field := range schema.Fields() {
		nameToIdx[field.Name] = i
	}

	// Select requested columns
	indices := make([]int, 0, len(r.opts.Columns))
	for _, col := range r.opts.Columns {
		if idx, ok := nameToIdx[col]; ok {
			indices = append(indices, idx)
		}
	}

	return indices
}

// selectRowGroups returns the row groups to read
func (r *Reader) selectRowGroups(numRowGroups int) []int {
	if len(r.opts.RowGroups) == 0 {
		// Read all row groups
		groups := make([]int, numRowGroups)
		for i := range groups {
			groups[i] = i
		}
		return groups
	}

	// Filter valid row groups
	groups := make([]int, 0, len(r.opts.RowGroups))
	for _, rg := range r.opts.RowGroups {
		if rg >= 0 && rg < numRowGroups {
			groups = append(groups, rg)
		}
	}
	return groups
}

// readTable reads the selected columns and row groups into an Arrow table
func (r *Reader) readRecordBatches(reader *pqarrow.FileReader, columnIndices []int, rowGroups []int) (*frame.DataFrame, error) {
	if len(rowGroups) == 0 || len(columnIndices) == 0 {
		return frame.NewDataFrame()
	}

	ctx := context.Background()

	recordReader, err := reader.GetRecordReader(ctx, columnIndices, rowGroups)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	defer recordReader.Release()

	schema := recordReader.Schema()
	fields := schema.Fields()
	if len(fields) == 0 {
		return frame.NewDataFrame()
	}

	builders := make([]seriesBuilder, len(fields))
	for i, field := range fields {
		builder, err := r.newSeriesBuilder(field)
		if err != nil {
			return nil, err
		}
		builders[i] = builder
	}

	var rowsRead int64
	for recordReader.Next() {
		rec := recordReader.Record()
		if rec == nil {
			continue
		}

		batchRows := rec.NumRows()
		rowsToUse := batchRows
		if r.opts.NumRows > 0 {
			remaining := r.opts.NumRows - rowsRead
			if remaining <= 0 {
				break
			}
			if rowsToUse > remaining {
				rowsToUse = remaining
			}
		}

		needSlice := rowsToUse < batchRows
		for i := 0; i < len(fields); i++ {
			col := rec.Column(i)
			releaseAfter := false
			if needSlice {
				col = array.NewSlice(col, 0, rowsToUse)
				releaseAfter = true
			}

			if err := builders[i].append(col); err != nil {
				if releaseAfter {
					col.Release()
				}
				return nil, fmt.Errorf("failed to append column %s: %w", fields[i].Name, err)
			}

			if releaseAfter {
				col.Release()
			}
		}

		rowsRead += rowsToUse
		if r.opts.NumRows > 0 && rowsRead >= r.opts.NumRows {
			break
		}
	}

	if err := recordReader.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read parquet batches: %w", err)
	}

	seriesList := make([]series.Series, len(builders))
	for i, builder := range builders {
		s, err := builder.finish()
		if err != nil {
			return nil, err
		}
		seriesList[i] = s
	}

	return frame.NewDataFrame(seriesList...)
}

type seriesBuilder struct {
	append func(arrow.Array) error
	finish func() (series.Series, error)
}

func (r *Reader) newSeriesBuilder(field arrow.Field) (seriesBuilder, error) {
	switch field.Type.ID() {
	case arrow.BOOL:
		ca := chunked.NewChunkedArray[bool](field.Name, datatypes.Boolean{})
		return seriesBuilder{
			append: ca.AppendArray,
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	case arrow.INT32:
		ca := chunked.NewChunkedArray[int32](field.Name, datatypes.Int32{})
		return seriesBuilder{
			append: ca.AppendArray,
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	case arrow.INT64:
		ca := chunked.NewChunkedArray[int64](field.Name, datatypes.Int64{})
		return seriesBuilder{
			append: ca.AppendArray,
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	case arrow.FLOAT32:
		ca := chunked.NewChunkedArray[float32](field.Name, datatypes.Float32{})
		return seriesBuilder{
			append: ca.AppendArray,
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	case arrow.FLOAT64:
		ca := chunked.NewChunkedArray[float64](field.Name, datatypes.Float64{})
		return seriesBuilder{
			append: ca.AppendArray,
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	case arrow.STRING:
		ca := chunked.NewChunkedArray[string](field.Name, datatypes.String{})
		return seriesBuilder{
			append: ca.AppendArray,
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	case arrow.LARGE_STRING:
		ca := chunked.NewChunkedArray[string](field.Name, datatypes.String{})
		return seriesBuilder{
			append: func(arr arrow.Array) error {
				return r.appendLargeString(ca, arr)
			},
			finish: func() (series.Series, error) { return series.NewSeriesFromChunkedArray(ca), nil },
		}, nil
	default:
		return seriesBuilder{}, fmt.Errorf("unsupported arrow type: %s", field.Type)
	}
}

func (r *Reader) appendLargeString(ca *chunked.ChunkedArray[string], arr arrow.Array) error {
	largeChunk, ok := arr.(*array.LargeString)
	if !ok {
		return fmt.Errorf("expected large string chunk, got %T", arr)
	}

	builder := array.NewStringBuilder(r.opts.Allocator)
	builder.Reserve(largeChunk.Len())
	for i := 0; i < largeChunk.Len(); i++ {
		if largeChunk.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(largeChunk.Value(i))
		}
	}
	strArr := builder.NewStringArray()
	builder.Release()

	if err := ca.AppendArray(strArr); err != nil {
		strArr.Release()
		return err
	}
	strArr.Release()
	return nil
}

// tableToDataFrame converts an Arrow table to a Golars DataFrame
func (r *Reader) tableToDataFrame(table arrow.Table) (*frame.DataFrame, error) {
	numCols := int(table.NumCols())
	if numCols == 0 {
		return frame.NewDataFrame()
	}

	seriesList := make([]series.Series, numCols)

	for i := 0; i < numCols; i++ {
		col := table.Column(i)
		field := table.Schema().Field(i)

		// Convert Arrow column to Series
		s, err := r.columnToSeries(col, field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", field.Name, err)
		}

		seriesList[i] = s
	}

	return frame.NewDataFrame(seriesList...)
}

// columnToSeries converts an Arrow column to a Golars Series
func (r *Reader) columnToSeries(col *arrow.Column, field arrow.Field) (series.Series, error) {
	// Create ChunkedArray from Arrow chunks
	chunks := col.Data().Chunks()

	switch field.Type.ID() {
	case arrow.BOOL:
		return r.boolColumnToSeries(field.Name, chunks)
	case arrow.INT32:
		return r.int32ColumnToSeries(field.Name, chunks)
	case arrow.INT64:
		return r.int64ColumnToSeries(field.Name, chunks)
	case arrow.FLOAT32:
		return r.float32ColumnToSeries(field.Name, chunks)
	case arrow.FLOAT64:
		return r.float64ColumnToSeries(field.Name, chunks)
	case arrow.STRING:
		return r.stringColumnToSeries(field.Name, chunks)
	case arrow.LARGE_STRING:
		return r.largeStringColumnToSeries(field.Name, chunks)
	default:
		return nil, fmt.Errorf("unsupported arrow type: %s", field.Type)
	}
}

// Type-specific conversion methods

func (r *Reader) boolColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[bool](name, datatypes.Boolean{})

	for _, chunk := range chunks {
		// Add the chunk directly
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) int32ColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[int32](name, datatypes.Int32{})

	for _, chunk := range chunks {
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) int64ColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[int64](name, datatypes.Int64{})

	for _, chunk := range chunks {
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) float32ColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[float32](name, datatypes.Float32{})

	for _, chunk := range chunks {
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) float64ColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[float64](name, datatypes.Float64{})

	for _, chunk := range chunks {
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) stringColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[string](name, datatypes.String{})

	for _, chunk := range chunks {
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) largeStringColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[string](name, datatypes.String{})

	for _, chunk := range chunks {
		largeChunk, ok := chunk.(*array.LargeString)
		if !ok {
			return nil, fmt.Errorf("expected large string chunk, got %T", chunk)
		}

		converted := array.NewStringBuilder(r.opts.Allocator)
		converted.Reserve(largeChunk.Len())
		for i := 0; i < largeChunk.Len(); i++ {
			if largeChunk.IsNull(i) {
				converted.AppendNull()
			} else {
				converted.Append(largeChunk.Value(i))
			}
		}
		strArr := converted.NewStringArray()
		converted.Release()

		if err := ca.AppendArray(strArr); err != nil {
			strArr.Release()
			return nil, err
		}
		strArr.Release()
	}

	return series.NewSeriesFromChunkedArray(ca), nil
}

// ReadParquet is a convenience function to read a Parquet file with default options
func ReadParquet(filename string) (*frame.DataFrame, error) {
	reader := NewReader(DefaultReaderOptions())
	return reader.ReadFile(filename)
}

// ReadParquetWithOptions reads a Parquet file with custom options
func ReadParquetWithOptions(filename string, opts ReaderOptions) (*frame.DataFrame, error) {
	reader := NewReader(opts)
	return reader.ReadFile(filename)
}
