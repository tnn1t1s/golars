package parquet

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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
		Columns:   nil,
		RowGroups: nil,
		NumRows:   0,
		Allocator: memory.NewGoAllocator(),
		Parallel:  false,
		BatchSize: 64 * 1024,
	}
}

// Reader reads Parquet files into DataFrames
type Reader struct {
	opts ReaderOptions
}

// NewReader creates a new Parquet reader
func NewReader(opts ReaderOptions) *Reader {
	if opts.Allocator == nil {
		opts.Allocator = memory.NewGoAllocator()
	}
	return &Reader{opts: opts}
}

// ReadFile reads a Parquet file into a DataFrame
func (r *Reader) ReadFile(filename string) (*frame.DataFrame, error) {
	// Open the file
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	// Create parquet file reader
	pqFile, err := file.NewParquetReader(f, file.WithReadProps(&pq.ReaderProperties{}))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqFile.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{
		Parallel:  r.opts.Parallel,
		BatchSize: r.opts.BatchSize,
	}, r.opts.Allocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Get schema
	arrowSchema, err := arrowReader.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	// Select columns if specified
	columnIndices := r.selectColumns(arrowSchema)

	// Select row groups if specified
	rowGroups := r.selectRowGroups(pqFile.NumRowGroups())

	// Read using the record reader for batch scanning
	return r.readRecordBatches(arrowReader, columnIndices, rowGroups)
}

// selectColumns returns the indices of columns to read
func (r *Reader) selectColumns(schema *arrow.Schema) []int {
	// Read all columns
	if len(r.opts.Columns) == 0 {
		indices := make([]int, schema.NumFields())
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Build column name to index map
	colMap := make(map[string]int, schema.NumFields())
	for i, field := range schema.Fields() {
		colMap[field.Name] = i
	}

	// Select requested columns
	var indices []int
	for _, name := range r.opts.Columns {
		if idx, ok := colMap[name]; ok {
			indices = append(indices, idx)
		}
	}
	return indices
}

// selectRowGroups returns the row groups to read
func (r *Reader) selectRowGroups(numRowGroups int) []int {
	// Read all row groups
	if len(r.opts.RowGroups) == 0 {
		groups := make([]int, numRowGroups)
		for i := range groups {
			groups[i] = i
		}
		return groups
	}

	// Filter valid row groups
	var valid []int
	for _, g := range r.opts.RowGroups {
		if g >= 0 && g < numRowGroups {
			valid = append(valid, g)
		}
	}
	return valid
}

// readRecordBatches reads the selected columns and row groups into a DataFrame
func (r *Reader) readRecordBatches(reader *pqarrow.FileReader, columnIndices []int, rowGroups []int) (*frame.DataFrame, error) {
	if len(columnIndices) == 0 {
		return frame.NewDataFrame()
	}

	ctx := context.Background()

	// Get schema to determine field types
	schema, err := reader.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Build series builders for each selected column
	builders := make([]seriesBuilder, len(columnIndices))
	selectedFields := make([]arrow.Field, len(columnIndices))
	for i, colIdx := range columnIndices {
		field := schema.Field(colIdx)
		selectedFields[i] = field
		b, err := r.newSeriesBuilder(field)
		if err != nil {
			return nil, fmt.Errorf("unsupported type for column %s: %w", field.Name, err)
		}
		builders[i] = b
	}

	// Read record batches
	rr, err := reader.GetRecordReader(ctx, columnIndices, rowGroups)
	if err != nil {
		return nil, fmt.Errorf("failed to get record reader: %w", err)
	}
	defer rr.Release()

	totalRows := int64(0)
	for rr.Next() {
		rec := rr.Record()

		// Check row limit
		if r.opts.NumRows > 0 {
			remaining := r.opts.NumRows - totalRows
			if remaining <= 0 {
				break
			}
			batchLen := int64(rec.NumRows())
			if batchLen > remaining {
				rec = rec.NewSlice(0, remaining)
				defer rec.Release()
			}
		}

		for i := range columnIndices {
			col := rec.Column(i)
			if err := builders[i].append(col); err != nil {
				return nil, fmt.Errorf("failed to append data for column %s: %w", selectedFields[i].Name, err)
			}
		}

		totalRows += int64(rec.NumRows())
		if r.opts.NumRows > 0 && totalRows >= r.opts.NumRows {
			break
		}
	}

	if err := rr.Err(); err != nil {
		return nil, fmt.Errorf("error reading records: %w", err)
	}

	// Build series from builders
	seriesList := make([]series.Series, len(builders))
	for i, b := range builders {
		s, err := b.finish()
		if err != nil {
			return nil, fmt.Errorf("failed to build series %s: %w", selectedFields[i].Name, err)
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
	name := field.Name

	switch field.Type.ID() {
	case arrow.BOOL:
		ca := chunked.NewChunkedArray[bool](name, datatypes.Boolean{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.INT8:
		ca := chunked.NewChunkedArray[int8](name, datatypes.Int8{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.INT16:
		ca := chunked.NewChunkedArray[int16](name, datatypes.Int16{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.INT32:
		ca := chunked.NewChunkedArray[int32](name, datatypes.Int32{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.INT64:
		ca := chunked.NewChunkedArray[int64](name, datatypes.Int64{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.UINT8:
		ca := chunked.NewChunkedArray[uint8](name, datatypes.UInt8{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.UINT16:
		ca := chunked.NewChunkedArray[uint16](name, datatypes.UInt16{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.UINT32:
		ca := chunked.NewChunkedArray[uint32](name, datatypes.UInt32{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.UINT64:
		ca := chunked.NewChunkedArray[uint64](name, datatypes.UInt64{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.FLOAT32:
		ca := chunked.NewChunkedArray[float32](name, datatypes.Float32{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.FLOAT64:
		ca := chunked.NewChunkedArray[float64](name, datatypes.Float64{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.STRING:
		ca := chunked.NewChunkedArray[string](name, datatypes.String{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return ca.AppendArray(arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.LARGE_STRING:
		ca := chunked.NewChunkedArray[string](name, datatypes.String{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return r.appendLargeString(ca, arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	case arrow.DATE32:
		ca := chunked.NewChunkedArray[int32](name, datatypes.Date{})
		return seriesBuilder{
			append: func(arr arrow.Array) error { return r.appendDate32(ca, arr) },
			finish: func() (series.Series, error) {
				return series.NewSeriesFromChunkedArray(ca), nil
			},
		}, nil

	default:
		return seriesBuilder{}, fmt.Errorf("unsupported arrow type: %s", field.Type.Name())
	}
}

func (r *Reader) appendLargeString(ca *chunked.ChunkedArray[string], arr arrow.Array) error {
	largeStr := arr.(*array.LargeString)
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < largeStr.Len(); i++ {
		if largeStr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(largeStr.Value(i))
		}
	}

	newArr := builder.NewArray()
	defer newArr.Release()
	return ca.AppendArray(newArr)
}

func (r *Reader) appendDate32(ca *chunked.ChunkedArray[int32], arr arrow.Array) error {
	// Date32 arrays can be appended directly - the chunked array stores int32 values
	// but expects Date32 Arrow arrays for type compatibility
	return ca.AppendArray(arr)
}

// tableToDataFrame converts an Arrow table to a Golars DataFrame
func (r *Reader) tableToDataFrame(table arrow.Table) (*frame.DataFrame, error) {
	numCols := int(table.NumCols())
	seriesList := make([]series.Series, numCols)

	for i := 0; i < numCols; i++ {
		col := table.Column(i)
		field := table.Schema().Field(i)
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
	case arrow.DATE32:
		return r.date32ColumnToSeries(field.Name, chunks)
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", field.Type.Name())
	}
}

// Type-specific conversion methods

func (r *Reader) boolColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[bool](name, datatypes.Boolean{})
	for _, chunk := range chunks {
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
		if err := r.appendLargeString(ca, chunk); err != nil {
			return nil, err
		}
	}
	return series.NewSeriesFromChunkedArray(ca), nil
}

func (r *Reader) date32ColumnToSeries(name string, chunks []arrow.Array) (series.Series, error) {
	ca := chunked.NewChunkedArray[int32](name, datatypes.Date{})
	for _, chunk := range chunks {
		// Date32 arrays can be appended directly
		if err := ca.AppendArray(chunk); err != nil {
			return nil, err
		}
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
