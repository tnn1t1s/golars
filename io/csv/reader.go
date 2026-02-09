package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Reader reads CSV data into a DataFrame
type Reader struct {
	reader     *csv.Reader
	options    ReadOptions
	inferTypes bool
}

// ReadOptions configures CSV reading behavior
type ReadOptions struct {
	Delimiter       rune                          // Field delimiter (default: ',')
	Header          bool                          // First row contains headers
	SkipRows        int                           // Number of rows to skip at start
	Columns         []string                      // Specific columns to read (nil = all)
	DataTypes       map[string]datatypes.DataType // Explicit column types
	NullValues      []string                      // Strings to treat as null
	ParseDates      bool                          // Try to parse date columns
	DateFormat      string                        // Date format for parsing
	InferSchemaRows int                           // Rows to scan for type inference (default: 100)
	Comment         rune                          // Comment character (0 = disabled)
}

// DefaultReadOptions returns default CSV read options
func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		Delimiter:       ',',
		Header:          true,
		SkipRows:        0,
		Columns:         nil,
		DataTypes:       nil,
		NullValues:      []string{""},
		ParseDates:      false,
		DateFormat:      "",
		InferSchemaRows: 100,
		Comment:         0,
	}
}

// NewReader creates a new CSV reader
func NewReader(r io.Reader, options ReadOptions) *Reader {
	csvReader := csv.NewReader(r)
	csvReader.Comma = options.Delimiter
	csvReader.Comment = options.Comment
	// Allow variable number of fields
	csvReader.FieldsPerRecord = -1

	return &Reader{
		reader:     csvReader,
		options:    options,
		inferTypes: true,
	}
}

// Read reads the entire CSV into a DataFrame
func (r *Reader) Read() (*frame.DataFrame, error) {
	// Skip rows if needed
	for i := 0; i < r.options.SkipRows; i++ {
		_, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				return frame.NewDataFrame()
			}
			return nil, fmt.Errorf("error skipping row %d: %w", i, err)
		}
	}

	// Read header
	var headers []string
	if r.options.Header {
		header, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				// Empty file
				return frame.NewDataFrame()
			}
			return nil, fmt.Errorf("error reading header: %w", err)
		}
		headers = make([]string, len(header))
		copy(headers, header)
	}

	// Read all records
	var records [][]string
	for {
		record, err := r.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading record: %w", err)
		}
		records = append(records, record)
	}

	// Empty DataFrame
	if len(headers) == 0 && len(records) == 0 {
		return frame.NewDataFrame()
	}

	// Generate headers if not provided
	if !r.options.Header && len(records) > 0 {
		numCols := len(records[0])
		headers = make([]string, numCols)
		for i := 0; i < numCols; i++ {
			headers[i] = fmt.Sprintf("column_%d", i)
		}
	}

	// Empty DataFrame with just headers
	if len(records) == 0 {
		cols := make([]series.Series, len(headers))
		for i, h := range headers {
			cols[i] = series.NewStringSeries(h, []string{})
		}
		return frame.NewDataFrame(cols...)
	}

	// Filter columns if specified
	columnIndices := r.getColumnIndices(headers)

	// Infer schema
	schema := r.inferSchema(headers, records, columnIndices)

	// Build DataFrame
	return r.buildDataFrame(headers, records, schema, columnIndices)
}

// getColumnIndices returns indices of columns to read
func (r *Reader) getColumnIndices(headers []string) []int {
	// Read all columns
	if len(r.options.Columns) == 0 {
		indices := make([]int, len(headers))
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Build map of header positions
	headerMap := make(map[string]int, len(headers))
	for i, h := range headers {
		headerMap[h] = i
	}

	// Get indices of requested columns
	var indices []int
	for _, col := range r.options.Columns {
		if idx, ok := headerMap[col]; ok {
			indices = append(indices, idx)
		}
	}
	return indices
}

// inferSchema infers data types from sample records
func (r *Reader) inferSchema(headers []string, records [][]string, columnIndices []int) []datatypes.DataType {
	schema := make([]datatypes.DataType, len(columnIndices))

	for i, colIdx := range columnIndices {
		colName := headers[colIdx]

		// Check if user specified type
		if r.options.DataTypes != nil {
			if dt, ok := r.options.DataTypes[colName]; ok {
				schema[i] = dt
				continue
			}
		}

		// Infer type from data
		sampleSize := r.options.InferSchemaRows
		if sampleSize <= 0 || sampleSize > len(records) {
			sampleSize = len(records)
		}
		schema[i] = r.inferColumnType(colIdx, records[:sampleSize])
	}

	return schema
}

// inferColumnType infers the type of a single column
func (r *Reader) inferColumnType(colIdx int, samples [][]string) datatypes.DataType {
	hasBool := true
	hasInt := true
	hasFloat := true
	allNull := true

	for _, row := range samples {
		var val string
		if colIdx < len(row) {
			val = row[colIdx]
		}

		if r.isNull(val) {
			continue
		}
		allNull = false

		// Try boolean
		lower := strings.ToLower(strings.TrimSpace(val))
		if lower != "true" && lower != "false" && lower != "yes" && lower != "no" && lower != "1" && lower != "0" {
			hasBool = false
		}

		// Try integer
		if _, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err != nil {
			hasInt = false
		}

		// Try float
		if _, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err != nil {
			hasFloat = false
		}
	}

	// Default for all nulls
	if allNull {
		return datatypes.String{}
	}

	// Return most specific type that works
	if hasBool {
		return datatypes.Boolean{}
	}
	if hasInt {
		return datatypes.Int64{}
	}
	if hasFloat {
		return datatypes.Float64{}
	}
	return datatypes.String{}
}

// isNull checks if a value should be treated as null
func (r *Reader) isNull(val string) bool {
	for _, nv := range r.options.NullValues {
		if val == nv {
			return true
		}
	}
	return false
}

// buildDataFrame builds the final DataFrame from records
func (r *Reader) buildDataFrame(headers []string, records [][]string, schema []datatypes.DataType, columnIndices []int) (*frame.DataFrame, error) {
	cols := make([]series.Series, len(columnIndices))

	for i, colIdx := range columnIndices {
		colName := headers[colIdx]

		// Extract column data
		data := make([]string, len(records))
		validity := make([]bool, len(records))
		for rowIdx, row := range records {
			// Ensure colIdx is valid
			if colIdx < len(row) {
				val := row[colIdx]
				if r.isNull(val) {
					validity[rowIdx] = false
				} else {
					data[rowIdx] = val
					validity[rowIdx] = true
				}
			} else {
				validity[rowIdx] = false
			}
		}

		// Parse according to schema
		s, err := r.parseColumn(colName, data, validity, schema[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing column %s: %w", colName, err)
		}
		cols[i] = s
	}

	return frame.NewDataFrame(cols...)
}

// parseColumn parses string data into the appropriate Series type
func (r *Reader) parseColumn(name string, data []string, validity []bool, dtype datatypes.DataType) (series.Series, error) {
	switch dtype.(type) {
	case datatypes.Boolean:
		values := make([]bool, len(data))
		for i, val := range data {
			if !validity[i] {
				continue
			}
			lower := strings.ToLower(strings.TrimSpace(val))
			switch lower {
			case "true", "yes", "1":
				values[i] = true
			case "false", "no", "0":
				values[i] = false
			}
		}
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Boolean{}), nil

	case datatypes.Int64:
		values := make([]int64, len(data))
		for i, val := range data {
			if !validity[i] {
				continue
			}
			v, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			if err != nil {
				// Treat as null on parse error
				validity[i] = false
				continue
			}
			values[i] = v
		}
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Int64{}), nil

	case datatypes.Float64:
		values := make([]float64, len(data))
		for i, val := range data {
			if !validity[i] {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
			if err != nil {
				// Treat as null on parse error
				validity[i] = false
				continue
			}
			values[i] = v
		}
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Float64{}), nil

	case datatypes.String:
		// Already have string data
		return series.NewSeriesWithValidity(name, data, validity, datatypes.String{}), nil

	default:
		return series.NewSeriesWithValidity(name, data, validity, datatypes.String{}), nil
	}
}
