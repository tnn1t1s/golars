package json

import (
	_ "bufio"
	_ "encoding/json"
	_ "fmt"
	"io"
	_ "os"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	_ "github.com/tnn1t1s/golars/series"
)

// NDJSONReader reads NDJSON (newline-delimited JSON) data
type NDJSONReader struct {
	options   ReadOptions
	chunkSize int
}

// NewNDJSONReader creates a new NDJSON reader
func NewNDJSONReader(opts ...func(*ReadOptions)) *NDJSONReader {
	panic("not implemented")

	// Default chunk size

}

// WithChunkSize sets the chunk size for streaming
func (r *NDJSONReader) WithChunkSize(size int) *NDJSONReader {
	panic("not implemented")

}

// ReadFile reads an NDJSON file into a DataFrame
func (r *NDJSONReader) ReadFile(filename string) (*frame.DataFrame, error) {
	panic("not implemented")

}

// Read reads NDJSON data from a reader into a DataFrame
func (r *NDJSONReader) Read(reader io.Reader) (*frame.DataFrame, error) {
	panic("not implemented")

	// 1MB max line size

	// Read first chunk for schema inference

	// Break after first chunk for schema inference

	// Infer schema from first chunk

	// If we only needed schema inference sample, read the rest

	// Continue reading the rest of the file

	// Build DataFrame from all records

}

// ReadStream reads NDJSON data in chunks and calls a callback for each chunk
func (r *NDJSONReader) ReadStream(reader io.Reader, callback func(*frame.DataFrame) error) error {
	panic("not implemented")

	// Read a chunk

	// Infer schema from first chunk

	// Build DataFrame for this chunk

	// Call the callback

	// Check for scanner errors

}

// buildDataFrame builds a DataFrame from records using the schema
func (r *NDJSONReader) buildDataFrame(records []map[string]interface{}, schema map[string]datatypes.DataType, jsonReader *Reader) (*frame.DataFrame, error) {
	panic("not implemented")

	// If no schema provided, infer it

	// Build series

	// Filter columns if specified

}
