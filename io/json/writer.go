package json

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tnn1t1s/golars/frame"
)

// WriteOptions configures JSON writing behavior
type WriteOptions struct {
	Pretty      bool
	Orient      string // "records" (default), "columns", "values"
	Compression string // "", "gzip"
	Indent      string // For pretty printing
}

// DefaultWriteOptions returns default write options
func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		Pretty:      false,
		Orient:      "records",
		Compression: "",
		Indent:      "  ",
	}
}

// Writer writes DataFrames to JSON format
type Writer struct {
	options WriteOptions
}

// NewWriter creates a new JSON writer
func NewWriter(opts ...func(*WriteOptions)) *Writer {
	options := DefaultWriteOptions()
	for _, o := range opts {
		o(&options)
	}
	return &Writer{options: options}
}

// WriteFile writes a DataFrame to a JSON file
func (w *Writer) WriteFile(df *frame.DataFrame, filename string) error {
	// Create output file
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	var writer io.Writer = f

	// Handle compression if needed
	useGzip := w.options.Compression == "gzip" || strings.HasSuffix(filename, ".gz")
	if useGzip {
		gzWriter := gzip.NewWriter(f)
		defer gzWriter.Close()
		writer = gzWriter
	}

	return w.Write(df, writer)
}

// Write writes a DataFrame to a writer in JSON format
func (w *Writer) Write(df *frame.DataFrame, writer io.Writer) error {
	data, err := w.prepareData(df)
	if err != nil {
		return err
	}

	var output []byte
	if w.options.Pretty {
		output, err = json.MarshalIndent(data, "", w.options.Indent)
	} else {
		output, err = json.Marshal(data)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	output = append(output, '\n')
	_, err = writer.Write(output)
	return err
}

// prepareData converts DataFrame to appropriate structure based on orientation
func (w *Writer) prepareData(df *frame.DataFrame) (interface{}, error) {
	switch w.options.Orient {
	case "records", "":
		return w.toRecords(df), nil
	case "columns":
		return w.toColumns(df), nil
	case "values":
		return w.toValues(df), nil
	default:
		return nil, fmt.Errorf("unsupported orientation: %s", w.options.Orient)
	}
}

// toRecords converts DataFrame to array of objects (default format)
func (w *Writer) toRecords(df *frame.DataFrame) []map[string]interface{} {
	numRows := df.Height()
	cols := df.Columns()
	records := make([]map[string]interface{}, numRows)

	for i := 0; i < numRows; i++ {
		record := make(map[string]interface{}, len(cols))
		for _, colName := range cols {
			col, _ := df.Column(colName)
			if col.IsNull(i) {
				record[colName] = nil
			} else {
				record[colName] = col.Get(i)
			}
		}
		records[i] = record
	}

	return records
}

// toColumns converts DataFrame to column-oriented format
func (w *Writer) toColumns(df *frame.DataFrame) map[string][]interface{} {
	cols := df.Columns()
	result := make(map[string][]interface{}, len(cols))

	for _, colName := range cols {
		col, _ := df.Column(colName)
		values := make([]interface{}, col.Len())
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = col.Get(i)
			}
		}
		result[colName] = values
	}

	return result
}

// toValues converts DataFrame to array of arrays (values only)
func (w *Writer) toValues(df *frame.DataFrame) [][]interface{} {
	numRows := df.Height()
	numCols := df.Width()
	values := make([][]interface{}, numRows)

	for i := 0; i < numRows; i++ {
		row := make([]interface{}, numCols)
		for j := 0; j < numCols; j++ {
			col, _ := df.ColumnAt(j)
			if col.IsNull(i) {
				row[j] = nil
			} else {
				row[j] = col.Get(i)
			}
		}
		values[i] = row
	}

	return values
}

// NDJSONWriter writes DataFrames to NDJSON format
type NDJSONWriter struct {
	options WriteOptions
}

// NewNDJSONWriter creates a new NDJSON writer
func NewNDJSONWriter(opts ...func(*WriteOptions)) *NDJSONWriter {
	options := DefaultWriteOptions()
	for _, o := range opts {
		o(&options)
	}
	// NDJSON should not be pretty printed
	options.Pretty = false
	return &NDJSONWriter{options: options}
}

// WriteFile writes a DataFrame to an NDJSON file
func (w *NDJSONWriter) WriteFile(df *frame.DataFrame, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	var writer io.Writer = f

	// Handle compression
	if w.options.Compression == "gzip" || strings.HasSuffix(filename, ".gz") {
		gzWriter := gzip.NewWriter(f)
		defer gzWriter.Close()
		writer = gzWriter
	}

	return w.Write(df, writer)
}

// Write writes a DataFrame to a writer in NDJSON format
func (w *NDJSONWriter) Write(df *frame.DataFrame, writer io.Writer) error {
	numRows := df.Height()
	cols := df.Columns()

	// Write each row as a separate JSON object
	for i := 0; i < numRows; i++ {
		record := make(map[string]interface{}, len(cols))
		for _, colName := range cols {
			col, _ := df.Column(colName)
			if col.IsNull(i) {
				record[colName] = nil
			} else {
				record[colName] = col.Get(i)
			}
		}

		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal row %d: %w", i, err)
		}
		data = append(data, '\n')
		if _, err := writer.Write(data); err != nil {
			return fmt.Errorf("failed to write row %d: %w", i, err)
		}
	}

	return nil
}

// WriteStream writes a DataFrame to NDJSON in chunks
func (w *NDJSONWriter) WriteStream(df *frame.DataFrame, writer io.Writer, chunkSize int) error {
	numRows := df.Height()
	cols := df.Columns()

	for start := 0; start < numRows; start += chunkSize {
		end := start + chunkSize
		if end > numRows {
			end = numRows
		}

		// Write chunk
		for i := start; i < end; i++ {
			record := make(map[string]interface{}, len(cols))
			for _, colName := range cols {
				col, _ := df.Column(colName)
				if col.IsNull(i) {
					record[colName] = nil
				} else {
					record[colName] = col.Get(i)
				}
			}

			data, err := json.Marshal(record)
			if err != nil {
				return fmt.Errorf("failed to marshal row %d: %w", i, err)
			}
			data = append(data, '\n')
			if _, err := writer.Write(data); err != nil {
				return fmt.Errorf("failed to write row %d: %w", i, err)
			}
		}
	}

	return nil
}

// Option functions for writers
func WithPretty(pretty bool) func(*WriteOptions) {
	return func(o *WriteOptions) {
		o.Pretty = pretty
	}
}

func WithOrient(orient string) func(*WriteOptions) {
	return func(o *WriteOptions) {
		o.Orient = orient
	}
}

func WithCompression(compression string) func(*WriteOptions) {
	return func(o *WriteOptions) {
		o.Compression = compression
	}
}

func WithIndent(indent string) func(*WriteOptions) {
	return func(o *WriteOptions) {
		o.Indent = indent
	}
}
