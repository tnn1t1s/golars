package json

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/davidpalaitis/golars/frame"
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
		Pretty: false,
		Orient: "records",
		Indent: "  ",
	}
}

// Writer writes DataFrames to JSON format
type Writer struct {
	options WriteOptions
}

// NewWriter creates a new JSON writer
func NewWriter(opts ...func(*WriteOptions)) *Writer {
	options := DefaultWriteOptions()
	for _, opt := range opts {
		opt(&options)
	}
	return &Writer{options: options}
}

// WriteFile writes a DataFrame to a JSON file
func (w *Writer) WriteFile(df *frame.DataFrame, filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Handle compression if needed
	var writer io.Writer = file
	if w.options.Compression == "gzip" || strings.HasSuffix(filename, ".gz") {
		gzWriter := gzip.NewWriter(file)
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

	encoder := json.NewEncoder(writer)
	if w.options.Pretty {
		encoder.SetIndent("", w.options.Indent)
	}

	return encoder.Encode(data)
}

// prepareData converts DataFrame to appropriate structure based on orientation
func (w *Writer) prepareData(df *frame.DataFrame) (interface{}, error) {
	switch w.options.Orient {
	case "records":
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
	nrows, _ := df.Shape()
	records := make([]map[string]interface{}, nrows)
	columns := df.Columns()

	for i := 0; i < nrows; i++ {
		record := make(map[string]interface{})
		for _, col := range columns {
			series, err := df.Column(col)
			if err != nil {
				continue
			}
			if series.IsNull(i) {
				record[col] = nil
			} else {
				record[col] = series.Get(i)
			}
		}
		records[i] = record
	}

	return records
}

// toColumns converts DataFrame to column-oriented format
func (w *Writer) toColumns(df *frame.DataFrame) map[string][]interface{} {
	result := make(map[string][]interface{})
	
	for _, col := range df.Columns() {
		series, err := df.Column(col)
		if err != nil {
			continue
		}
		values := make([]interface{}, series.Len())
		
		for i := 0; i < series.Len(); i++ {
			if series.IsNull(i) {
				values[i] = nil
			} else {
				values[i] = series.Get(i)
			}
		}
		
		result[col] = values
	}

	return result
}

// toValues converts DataFrame to array of arrays (values only)
func (w *Writer) toValues(df *frame.DataFrame) [][]interface{} {
	nrows, _ := df.Shape()
	values := make([][]interface{}, nrows)
	columns := df.Columns()

	for i := 0; i < nrows; i++ {
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			series, err := df.Column(col)
			if err != nil {
				row[j] = nil
				continue
			}
			if series.IsNull(i) {
				row[j] = nil
			} else {
				row[j] = series.Get(i)
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
	options.Pretty = false // NDJSON should not be pretty printed
	for _, opt := range opts {
		opt(&options)
	}
	return &NDJSONWriter{options: options}
}

// WriteFile writes a DataFrame to an NDJSON file
func (w *NDJSONWriter) WriteFile(df *frame.DataFrame, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Handle compression
	var writer io.Writer = file
	if w.options.Compression == "gzip" || strings.HasSuffix(filename, ".gz") {
		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()
		writer = gzWriter
	}

	return w.Write(df, writer)
}

// Write writes a DataFrame to a writer in NDJSON format
func (w *NDJSONWriter) Write(df *frame.DataFrame, writer io.Writer) error {
	encoder := json.NewEncoder(writer)
	columns := df.Columns()
	nrows, _ := df.Shape()

	// Write each row as a separate JSON object
	for i := 0; i < nrows; i++ {
		record := make(map[string]interface{})
		for _, col := range columns {
			series, err := df.Column(col)
			if err != nil {
				continue
			}
			if series.IsNull(i) {
				record[col] = nil
			} else {
				record[col] = series.Get(i)
			}
		}
		
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("failed to encode record %d: %w", i, err)
		}
	}

	return nil
}

// WriteStream writes a DataFrame to NDJSON in chunks
func (w *NDJSONWriter) WriteStream(df *frame.DataFrame, writer io.Writer, chunkSize int) error {
	encoder := json.NewEncoder(writer)
	columns := df.Columns()
	nrows, _ := df.Shape()

	for start := 0; start < nrows; start += chunkSize {
		end := start + chunkSize
		if end > nrows {
			end = nrows
		}

		// Write chunk
		for i := start; i < end; i++ {
			record := make(map[string]interface{})
			for _, col := range columns {
				series, err := df.Column(col)
				if err != nil {
					continue
				}
				if series.IsNull(i) {
					record[col] = nil
				} else {
					record[col] = series.Get(i)
				}
			}
			
			if err := encoder.Encode(record); err != nil {
				return fmt.Errorf("failed to encode record %d: %w", i, err)
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