package json

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// NDJSONReader reads NDJSON (newline-delimited JSON) data
type NDJSONReader struct {
	options   ReadOptions
	chunkSize int
}

// NewNDJSONReader creates a new NDJSON reader
func NewNDJSONReader(opts ...func(*ReadOptions)) *NDJSONReader {
	options := DefaultReadOptions()
	for _, opt := range opts {
		opt(&options)
	}
	return &NDJSONReader{
		options:   options,
		chunkSize: 10000, // Default chunk size
	}
}

// WithChunkSize sets the chunk size for streaming
func (r *NDJSONReader) WithChunkSize(size int) *NDJSONReader {
	r.chunkSize = size
	return r
}

// ReadFile reads an NDJSON file into a DataFrame
func (r *NDJSONReader) ReadFile(filename string) (*frame.DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return r.Read(file)
}

// Read reads NDJSON data from a reader into a DataFrame
func (r *NDJSONReader) Read(reader io.Reader) (*frame.DataFrame, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB max line size

	// Read first chunk for schema inference
	var records []map[string]interface{}
	lineCount := 0
	
	for scanner.Scan() && (r.options.MaxRecords <= 0 || lineCount < r.options.MaxRecords) {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			if r.options.SkipInvalid {
				continue
			}
			return nil, fmt.Errorf("failed to parse JSON at line %d: %w", lineCount+1, err)
		}

		records = append(records, record)
		lineCount++

		// Break after first chunk for schema inference
		if r.options.InferSchema && len(records) >= r.chunkSize {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading input: %w", err)
	}

	if len(records) == 0 {
		return frame.NewDataFrame()
	}

	// Infer schema from first chunk
	var schema map[string]datatypes.DataType
	if r.options.InferSchema {
		jsonReader := &Reader{options: r.options}
		schema = jsonReader.inferSchema(records)
	}

	// If we only needed schema inference sample, read the rest
	if r.options.InferSchema && (r.options.MaxRecords <= 0 || lineCount < r.options.MaxRecords) {
		// Continue reading the rest of the file
		for scanner.Scan() && (r.options.MaxRecords <= 0 || lineCount < r.options.MaxRecords) {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}

			var record map[string]interface{}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				if r.options.SkipInvalid {
					continue
				}
				return nil, fmt.Errorf("failed to parse JSON at line %d: %w", lineCount+1, err)
			}

			records = append(records, record)
			lineCount++
		}
	}

	// Build DataFrame from all records
	jsonReader := &Reader{options: r.options}
	return r.buildDataFrame(records, schema, jsonReader)
}

// ReadStream reads NDJSON data in chunks and calls a callback for each chunk
func (r *NDJSONReader) ReadStream(reader io.Reader, callback func(*frame.DataFrame) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var schema map[string]datatypes.DataType
	jsonReader := &Reader{options: r.options}
	firstChunk := true
	
	for {
		records := make([]map[string]interface{}, 0, r.chunkSize)
		
		// Read a chunk
		for i := 0; i < r.chunkSize && scanner.Scan(); i++ {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}

			var record map[string]interface{}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				if r.options.SkipInvalid {
					continue
				}
				return fmt.Errorf("failed to parse JSON: %w", err)
			}

			records = append(records, record)
		}

		if len(records) == 0 {
			break
		}

		// Infer schema from first chunk
		if firstChunk && r.options.InferSchema {
			schema = jsonReader.inferSchema(records)
			firstChunk = false
		}

		// Build DataFrame for this chunk
		df, err := r.buildDataFrame(records, schema, jsonReader)
		if err != nil {
			return err
		}

		// Call the callback
		if err := callback(df); err != nil {
			return err
		}

		// Check for scanner errors
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}
	}

	return nil
}

// buildDataFrame builds a DataFrame from records using the schema
func (r *NDJSONReader) buildDataFrame(records []map[string]interface{}, schema map[string]datatypes.DataType, jsonReader *Reader) (*frame.DataFrame, error) {
	if len(records) == 0 {
		return frame.NewDataFrame()
	}

	// If no schema provided, infer it
	if schema == nil {
		schema = jsonReader.inferSchema(records)
	}

	// Build series
	seriesList, err := jsonReader.buildSeries(records, schema)
	if err != nil {
		return nil, err
	}

	// Filter columns if specified
	if len(r.options.Columns) > 0 {
		filtered := make([]series.Series, 0, len(r.options.Columns))
		for _, s := range seriesList {
			for _, col := range r.options.Columns {
				if s.Name() == col {
					filtered = append(filtered, s)
					break
				}
			}
		}
		seriesList = filtered
	}

	return frame.NewDataFrame(seriesList...)
}