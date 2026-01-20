package parquet

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
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
}

// DefaultReaderOptions returns default reader options
func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		Allocator: memory.DefaultAllocator,
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
	return &Reader{opts: opts}
}

// ReadFile reads a Parquet file into a DataFrame
func (r *Reader) ReadFile(filename string) (*frame.DataFrame, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	// Create parquet file reader
	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader,
		pqarrow.ArrowReadProperties{
			BatchSize: 1024 * 1024, // 1MB batches
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

	// Read the table
	table, err := r.readTable(arrowReader, columnIndices, rowGroups)
	if err != nil {
		return nil, err
	}
	defer table.Release()

	// Convert to DataFrame
	return r.tableToDataFrame(table)
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
func (r *Reader) readTable(reader *pqarrow.FileReader, columnIndices []int, rowGroups []int) (arrow.Table, error) {
	if len(rowGroups) == 0 || len(columnIndices) == 0 {
		// Nothing to read
		schema := arrow.NewSchema([]arrow.Field{}, nil)
		return array.NewTable(schema, []arrow.Column{}, 0), nil
	}

	// Use context for the read operation
	ctx := context.Background()

	// Read the entire table
	table, err := reader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet table: %w", err)
	}

	// Filter columns if needed
	if len(columnIndices) < int(table.NumCols()) {
		// Need to select specific columns
		selectedCols := make([]arrow.Column, len(columnIndices))
		schema := table.Schema()

		selectedFields := make([]arrow.Field, len(columnIndices))
		for i, idx := range columnIndices {
			selectedCols[i] = *table.Column(idx)
			selectedFields[i] = schema.Field(idx)
		}

		newSchema := arrow.NewSchema(selectedFields, nil)
		table = array.NewTable(newSchema, selectedCols, table.NumRows())
	}

	// Apply row limit if specified
	if r.opts.NumRows > 0 && table.NumRows() > r.opts.NumRows {
		// Slice the table
		table = array.NewTableFromSlice(table.Schema(), sliceColumns(table, r.opts.NumRows))
	}

	return table, nil
}

// sliceColumns slices all columns to the specified number of rows
func sliceColumns(table arrow.Table, numRows int64) [][]arrow.Array {
	numCols := int(table.NumCols())
	result := make([][]arrow.Array, numCols)

	for i := 0; i < numCols; i++ {
		col := table.Column(i)
		chunks := make([]arrow.Array, 0)

		rowsRead := int64(0)
		for _, chunk := range col.Data().Chunks() {
			if rowsRead >= numRows {
				break
			}

			remaining := numRows - rowsRead
			if int64(chunk.Len()) <= remaining {
				// Take the whole chunk
				chunks = append(chunks, chunk)
				rowsRead += int64(chunk.Len())
			} else {
				// Slice the chunk
				sliced := array.NewSlice(chunk, 0, remaining)
				chunks = append(chunks, sliced)
				rowsRead = numRows
			}
		}

		result[i] = chunks
	}

	return result
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
