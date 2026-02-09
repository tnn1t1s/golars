package json

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// NDJSONReader reads NDJSON (newline-delimited JSON) data
type NDJSONReader struct {
	options   ReadOptions
	chunkSize int
}

// NewNDJSONReader creates a new NDJSON reader
func NewNDJSONReader(opts ...func(*ReadOptions)) *NDJSONReader {
	options := DefaultReadOptions()
	for _, o := range opts {
		o(&options)
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
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()
	return r.Read(f)
}

// Read reads NDJSON data from a reader into a DataFrame
func (r *NDJSONReader) Read(reader io.Reader) (*frame.DataFrame, error) {
	scanner := bufio.NewScanner(reader)
	// 1MB max line size
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var allRecords []map[string]interface{}

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			if r.options.SkipInvalid {
				continue
			}
			return nil, fmt.Errorf("failed to parse NDJSON line: %w", err)
		}

		allRecords = append(allRecords, record)

		// Apply max records limit
		if r.options.MaxRecords > 0 && len(allRecords) >= r.options.MaxRecords {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading NDJSON: %w", err)
	}

	if len(allRecords) == 0 {
		return frame.NewDataFrame()
	}

	// Use a JSON reader to build the DataFrame
	jsonReader := &Reader{options: r.options}

	// Flatten records if needed
	if r.options.Flatten {
		for i, rec := range allRecords {
			allRecords[i] = jsonReader.flattenRecord(rec)
		}
	} else {
		for i, rec := range allRecords {
			for k, v := range rec {
				if isNested(v) {
					rec[k] = fmt.Sprintf("%v", v)
				}
			}
			allRecords[i] = rec
		}
	}

	return r.buildDataFrame(allRecords, nil, jsonReader)
}

// ReadStream reads NDJSON data in chunks and calls a callback for each chunk
func (r *NDJSONReader) ReadStream(reader io.Reader, callback func(*frame.DataFrame) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	jsonReader := &Reader{options: r.options}
	var chunk []map[string]interface{}
	var schema map[string]datatypes.DataType

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			if r.options.SkipInvalid {
				continue
			}
			return fmt.Errorf("failed to parse NDJSON line: %w", err)
		}

		// Flatten records if needed
		if r.options.Flatten {
			record = jsonReader.flattenRecord(record)
		} else {
			for k, v := range record {
				if isNested(v) {
					record[k] = fmt.Sprintf("%v", v)
				}
			}
		}

		chunk = append(chunk, record)

		if len(chunk) >= r.chunkSize {
			// Infer schema from first chunk
			if schema == nil {
				schema = jsonReader.inferSchema(chunk)
			}

			// Build DataFrame for this chunk
			df, err := r.buildDataFrame(chunk, schema, jsonReader)
			if err != nil {
				return err
			}

			// Call the callback
			if err := callback(df); err != nil {
				return err
			}

			chunk = nil
		}
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading NDJSON: %w", err)
	}

	// Process remaining records
	if len(chunk) > 0 {
		if schema == nil {
			schema = jsonReader.inferSchema(chunk)
		}
		df, err := r.buildDataFrame(chunk, schema, jsonReader)
		if err != nil {
			return err
		}
		if err := callback(df); err != nil {
			return err
		}
	}

	return nil
}

// buildDataFrame builds a DataFrame from records using the schema
func (r *NDJSONReader) buildDataFrame(records []map[string]interface{}, schema map[string]datatypes.DataType, jsonReader *Reader) (*frame.DataFrame, error) {
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
		colSet := make(map[string]bool, len(r.options.Columns))
		for _, c := range r.options.Columns {
			colSet[c] = true
		}
		var filtered []series.Series
		for _, s := range seriesList {
			if colSet[s.Name()] {
				filtered = append(filtered, s)
			}
		}
		seriesList = filtered
	}

	if len(seriesList) == 0 {
		return frame.NewDataFrame()
	}

	return frame.NewDataFrame(seriesList...)
}
