package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/frame"
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
		NullValues:      []string{"", "NA", "N/A", "null", "NULL", "NaN", "nan"},
		ParseDates:      false,
		DateFormat:      "2006-01-02",
		InferSchemaRows: 100,
		Comment:         0,
	}
}

// NewReader creates a new CSV reader
func NewReader(r io.Reader, options ReadOptions) *Reader {
	csvReader := csv.NewReader(r)
	csvReader.Comma = options.Delimiter
	csvReader.ReuseRecord = true
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields
	if options.Comment != 0 {
		csvReader.Comment = options.Comment
	}

	return &Reader{
		reader:     csvReader,
		options:    options,
		inferTypes: len(options.DataTypes) == 0,
	}
}

// Read reads the entire CSV into a DataFrame
func (r *Reader) Read() (*frame.DataFrame, error) {
	// Skip rows if needed
	for i := 0; i < r.options.SkipRows; i++ {
		if _, err := r.reader.Read(); err != nil {
			return nil, fmt.Errorf("error skipping row %d: %w", i, err)
		}
	}

	// Read header
	var headers []string
	if r.options.Header {
		record, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				return frame.NewDataFrame() // Empty DataFrame
			}
			return nil, fmt.Errorf("error reading header: %w", err)
		}
		headers = append([]string(nil), record...) // Make a copy
	}

	// Collect data for type inference and building
	allRecords := make([][]string, 0)
	
	// Read all records
	for {
		record, err := r.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading record: %w", err)
		}
		allRecords = append(allRecords, append([]string(nil), record...))
	}

	if len(allRecords) == 0 {
		// Empty DataFrame with just headers
		if len(headers) > 0 {
			columns := make([]series.Series, len(headers))
			for i, h := range headers {
				columns[i] = series.NewStringSeries(h, []string{})
			}
			return frame.NewDataFrame(columns...)
		}
		return frame.NewDataFrame()
	}

	// Generate headers if not provided
	if !r.options.Header {
		headers = make([]string, len(allRecords[0]))
		for i := range headers {
			headers[i] = fmt.Sprintf("column_%d", i)
		}
	}

	// Filter columns if specified
	columnIndices := r.getColumnIndices(headers)
	if columnIndices == nil {
		return nil, fmt.Errorf("none of the specified columns found")
	}

	// Infer schema
	schema := r.inferSchema(headers, allRecords, columnIndices)

	// Build DataFrame
	return r.buildDataFrame(headers, allRecords, schema, columnIndices)
}

// getColumnIndices returns indices of columns to read
func (r *Reader) getColumnIndices(headers []string) []int {
	if len(r.options.Columns) == 0 {
		// Read all columns
		indices := make([]int, len(headers))
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Build map of header positions
	headerMap := make(map[string]int)
	for i, h := range headers {
		headerMap[h] = i
	}

	// Get indices of requested columns
	indices := make([]int, 0, len(r.options.Columns))
	for _, col := range r.options.Columns {
		if idx, exists := headerMap[col]; exists {
			indices = append(indices, idx)
		}
	}

	if len(indices) == 0 {
		return nil
	}
	return indices
}

// inferSchema infers data types from sample records
func (r *Reader) inferSchema(headers []string, records [][]string, columnIndices []int) []datatypes.DataType {
	schema := make([]datatypes.DataType, len(columnIndices))

	for i, colIdx := range columnIndices {
		// Check if user specified type
		if r.options.DataTypes != nil {
			if dtype, exists := r.options.DataTypes[headers[colIdx]]; exists {
				schema[i] = dtype
				continue
			}
		}

		// Infer type from data
		maxRows := r.options.InferSchemaRows
		if maxRows > len(records) {
			maxRows = len(records)
		}

		schema[i] = r.inferColumnType(colIdx, records[:maxRows])
	}

	return schema
}

// inferColumnType infers the type of a single column
func (r *Reader) inferColumnType(colIdx int, samples [][]string) datatypes.DataType {
	isInt := true
	isFloat := true
	isBool := true
	allNull := true

	for _, row := range samples {
		if colIdx >= len(row) {
			continue
		}

		val := row[colIdx]
		if r.isNull(val) {
			continue
		}

		allNull = false

		// Try integer
		if isInt {
			if _, err := strconv.ParseInt(val, 10, 64); err != nil {
				isInt = false
			}
		}

		// Try float
		if isFloat && !isInt {
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				isFloat = false
			}
		}

		// Try boolean
		if isBool {
			lower := strings.ToLower(val)
			if lower != "true" && lower != "false" &&
				lower != "1" && lower != "0" &&
				lower != "yes" && lower != "no" {
				isBool = false
			}
		}
	}

	// Return most specific type that works
	if allNull {
		return datatypes.String{} // Default for all nulls
	}
	if isInt {
		return datatypes.Int64{}
	}
	if isFloat {
		return datatypes.Float64{}
	}
	if isBool {
		return datatypes.Boolean{}
	}

	return datatypes.String{}
}

// isNull checks if a value should be treated as null
func (r *Reader) isNull(val string) bool {
	for _, nullVal := range r.options.NullValues {
		if val == nullVal {
			return true
		}
	}
	return false
}

// buildDataFrame builds the final DataFrame from records
func (r *Reader) buildDataFrame(headers []string, records [][]string, schema []datatypes.DataType, columnIndices []int) (*frame.DataFrame, error) {
	columns := make([]series.Series, len(columnIndices))

	for i, colIdx := range columnIndices {
		// Ensure colIdx is valid
		if colIdx >= len(headers) {
			return nil, fmt.Errorf("column index %d out of range", colIdx)
		}
		
		colData := make([]string, len(records))
		validity := make([]bool, len(records))

		for j, row := range records {
			if colIdx < len(row) {
				colData[j] = row[colIdx]
				validity[j] = !r.isNull(row[colIdx])
			} else {
				colData[j] = ""
				validity[j] = false
			}
		}

		// Parse according to schema
		col, err := r.parseColumn(headers[colIdx], colData, validity, schema[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing column %s: %w", headers[colIdx], err)
		}
		columns[i] = col
	}

	return frame.NewDataFrame(columns...)
}

// parseColumn parses string data into the appropriate Series type
func (r *Reader) parseColumn(name string, data []string, validity []bool, dtype datatypes.DataType) (series.Series, error) {
	switch dtype := dtype.(type) {
	case datatypes.Int64:
		values := make([]int64, len(data))
		for i, str := range data {
			if validity[i] {
				v, err := strconv.ParseInt(str, 10, 64)
				if err != nil {
					// Treat as null on parse error
					validity[i] = false
				} else {
					values[i] = v
				}
			}
		}
		return series.NewSeriesWithValidity(name, values, validity, dtype), nil

	case datatypes.Float64:
		values := make([]float64, len(data))
		for i, str := range data {
			if validity[i] {
				v, err := strconv.ParseFloat(str, 64)
				if err != nil {
					validity[i] = false
				} else {
					values[i] = v
				}
			}
		}
		return series.NewSeriesWithValidity(name, values, validity, dtype), nil

	case datatypes.Boolean:
		values := make([]bool, len(data))
		for i, str := range data {
			if validity[i] {
				lower := strings.ToLower(str)
				switch lower {
				case "true", "1", "yes":
					values[i] = true
				case "false", "0", "no":
					values[i] = false
				default:
					validity[i] = false
				}
			}
		}
		return series.NewSeriesWithValidity(name, values, validity, dtype), nil

	case datatypes.String:
		// Already have string data
		return series.NewSeriesWithValidity(name, data, validity, dtype), nil

	default:
		return nil, fmt.Errorf("unsupported data type: %v", dtype)
	}
}