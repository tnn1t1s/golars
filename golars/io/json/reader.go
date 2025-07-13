package json

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// Options for JSON reading
type ReadOptions struct {
	InferSchema  bool
	MaxRecords   int
	Columns      []string
	SkipInvalid  bool
	Flatten      bool
	DateFormat   string
}

// DefaultReadOptions returns default options for JSON reading
func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		InferSchema: true,
		MaxRecords:  0, // 0 means no limit
		SkipInvalid: false,
		Flatten:     true,
	}
}

// Reader reads JSON data into DataFrames
type Reader struct {
	options ReadOptions
}

// NewReader creates a new JSON reader with options
func NewReader(opts ...func(*ReadOptions)) *Reader {
	options := DefaultReadOptions()
	for _, opt := range opts {
		opt(&options)
	}
	return &Reader{options: options}
}

// ReadFile reads a JSON file into a DataFrame
func (r *Reader) ReadFile(filename string) (*frame.DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Handle gzip compression if file ends with .gz
	var reader io.Reader = file
	if strings.HasSuffix(filename, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	return r.Read(reader)
}

// Read reads JSON data from a reader into a DataFrame
func (r *Reader) Read(reader io.Reader) (*frame.DataFrame, error) {
	// Decode JSON array
	var records []map[string]interface{}
	decoder := json.NewDecoder(reader)
	
	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	if len(records) == 0 {
		return frame.NewDataFrame()
	}

	// Infer schema if needed
	var schema map[string]datatypes.DataType
	if r.options.InferSchema {
		inferLimit := r.options.MaxRecords
		if inferLimit <= 0 || inferLimit > len(records) {
			inferLimit = len(records)
		}
		schema = r.inferSchema(records[:inferLimit])
	}

	// Apply max records limit
	if r.options.MaxRecords > 0 && r.options.MaxRecords < len(records) {
		records = records[:r.options.MaxRecords]
	}
	
	// Build series from records
	seriesList, err := r.buildSeries(records, schema)
	if err != nil {
		return nil, err
	}

	return frame.NewDataFrame(seriesList...)
}

// inferSchema infers the schema from sample records
func (r *Reader) inferSchema(records []map[string]interface{}) map[string]datatypes.DataType {
	schema := make(map[string]datatypes.DataType)
	
	// Collect all column names
	columns := make(map[string]bool)
	for _, record := range records {
		for col := range record {
			if r.options.Flatten && isNested(record[col]) {
				// Handle nested objects
				flattened := r.flattenObject(col, record[col])
				for flatCol := range flattened {
					columns[flatCol] = true
				}
			} else {
				columns[col] = true
			}
		}
	}

	// Infer type for each column
	for col := range columns {
		values := make([]interface{}, 0, len(records))
		
		for _, record := range records {
			if r.options.Flatten {
				// Check flattened values
				flattened := r.flattenRecord(record)
				if val, exists := flattened[col]; exists {
					values = append(values, val)
				} else {
					values = append(values, nil)
				}
			} else {
				if val, exists := record[col]; exists {
					values = append(values, val)
				} else {
					values = append(values, nil)
				}
			}
		}
		
		schema[col] = inferType(values)
	}

	return schema
}

// inferType infers the data type from a slice of values
func inferType(values []interface{}) datatypes.DataType {
	// Count non-null values and track types
	var (
		boolCount  int
		intCount   int
		floatCount int
		hasFloat   bool
		allNull    = true
	)

	for _, val := range values {
		if val == nil {
			continue
		}
		allNull = false

		switch v := val.(type) {
		case bool:
			boolCount++
		case float64:
			// JSON numbers are always float64
			if v == float64(int64(v)) {
				intCount++
			} else {
				floatCount++
				hasFloat = true
			}
		case string:
			// If any value is string, use string type
			return datatypes.String{}
		}
	}

	if allNull {
		// Default to string for all-null columns
		return datatypes.String{}
	}

	// Determine type based on counts
	if boolCount > 0 && intCount == 0 && floatCount == 0 {
		return datatypes.Boolean{}
	}
	
	if hasFloat || floatCount > 0 {
		return datatypes.Float64{}
	}
	
	if intCount > 0 {
		return datatypes.Int64{}
	}

	return datatypes.String{}
}

// buildSeries builds series from records using the inferred schema
func (r *Reader) buildSeries(records []map[string]interface{}, schema map[string]datatypes.DataType) ([]series.Series, error) {
	// Flatten records if needed
	var processedRecords []map[string]interface{}
	if r.options.Flatten {
		processedRecords = make([]map[string]interface{}, len(records))
		for i, record := range records {
			processedRecords[i] = r.flattenRecord(record)
		}
	} else {
		processedRecords = records
	}

	// Create builders for each column
	builders := make(map[string]*seriesBuilder)
	for col, dtype := range schema {
		builders[col] = newSeriesBuilder(col, dtype, len(records))
	}

	// Populate builders
	for _, record := range processedRecords {
		for col, builder := range builders {
			if val, exists := record[col]; exists {
				builder.append(val)
			} else {
				builder.appendNull()
			}
		}
	}

	// Build series
	seriesList := make([]series.Series, 0, len(builders))
	for _, builder := range builders {
		s, err := builder.build()
		if err != nil {
			return nil, fmt.Errorf("failed to build series %s: %w", builder.name, err)
		}
		seriesList = append(seriesList, s)
	}

	return seriesList, nil
}

// flattenRecord flattens nested objects in a record
func (r *Reader) flattenRecord(record map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})
	
	for key, value := range record {
		if isNested(value) {
			for flatKey, flatVal := range r.flattenObject(key, value) {
				flattened[flatKey] = flatVal
			}
		} else {
			flattened[key] = value
		}
	}
	
	return flattened
}

// flattenObject recursively flattens nested objects
func (r *Reader) flattenObject(prefix string, value interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})
	
	switch v := value.(type) {
	case map[string]interface{}:
		for key, val := range v {
			newKey := prefix + "." + key
			if isNested(val) {
				for k, v := range r.flattenObject(newKey, val) {
					flattened[k] = v
				}
			} else {
				flattened[newKey] = val
			}
		}
	case []interface{}:
		// For arrays, convert to string representation
		flattened[prefix] = fmt.Sprintf("%v", v)
	default:
		flattened[prefix] = v
	}
	
	return flattened
}

// isNested checks if a value is a nested object or array
func isNested(value interface{}) bool {
	switch value.(type) {
	case map[string]interface{}, []interface{}:
		return true
	default:
		return false
	}
}

// seriesBuilder helps build series with proper type conversion
type seriesBuilder struct {
	name     string
	dtype    datatypes.DataType
	values   interface{}
	nullMask []bool
	size     int
	index    int
}

func newSeriesBuilder(name string, dtype datatypes.DataType, capacity int) *seriesBuilder {
	sb := &seriesBuilder{
		name:     name,
		dtype:    dtype,
		nullMask: make([]bool, 0, capacity),
		size:     capacity,
		index:    0,
	}

	// Initialize typed slice based on data type
	switch dtype.(type) {
	case datatypes.Boolean:
		sb.values = make([]bool, 0, capacity)
	case datatypes.Int32:
		sb.values = make([]int32, 0, capacity)
	case datatypes.Int64:
		sb.values = make([]int64, 0, capacity)
	case datatypes.Float64:
		sb.values = make([]float64, 0, capacity)
	case datatypes.String:
		sb.values = make([]string, 0, capacity)
	default:
		sb.values = make([]string, 0, capacity)
	}

	return sb
}

func (sb *seriesBuilder) append(value interface{}) {
	if value == nil {
		sb.appendNull()
		return
	}

	switch vals := sb.values.(type) {
	case []bool:
		if v, ok := value.(bool); ok {
			sb.values = append(vals, v)
			sb.nullMask = append(sb.nullMask, false)
		} else {
			sb.appendNull()
		}
	case []int32:
		if v, ok := toInt32(value); ok {
			sb.values = append(vals, v)
			sb.nullMask = append(sb.nullMask, false)
		} else {
			sb.appendNull()
		}
	case []int64:
		if v, ok := toInt64(value); ok {
			sb.values = append(vals, v)
			sb.nullMask = append(sb.nullMask, false)
		} else {
			sb.appendNull()
		}
	case []float64:
		if v, ok := toFloat64(value); ok {
			sb.values = append(vals, v)
			sb.nullMask = append(sb.nullMask, false)
		} else {
			sb.appendNull()
		}
	case []string:
		sb.values = append(vals, toString(value))
		sb.nullMask = append(sb.nullMask, false)
	}
	sb.index++
}

func (sb *seriesBuilder) appendNull() {
	switch vals := sb.values.(type) {
	case []bool:
		sb.values = append(vals, false)
	case []int32:
		sb.values = append(vals, 0)
	case []int64:
		sb.values = append(vals, 0)
	case []float64:
		sb.values = append(vals, 0.0)
	case []string:
		sb.values = append(vals, "")
	}
	sb.nullMask = append(sb.nullMask, true)
	sb.index++
}

func (sb *seriesBuilder) build() (series.Series, error) {
	// For now, we'll create series without null mask support
	// TODO: Add null mask support when series API supports it
	switch vals := sb.values.(type) {
	case []bool:
		return series.NewBooleanSeries(sb.name, vals), nil
	case []int32:
		return series.NewInt32Series(sb.name, vals), nil
	case []int64:
		return series.NewInt64Series(sb.name, vals), nil
	case []float64:
		return series.NewFloat64Series(sb.name, vals), nil
	case []string:
		return series.NewStringSeries(sb.name, vals), nil
	default:
		return nil, fmt.Errorf("unsupported type for series builder")
	}
}

// Type conversion helpers
func toInt32(value interface{}) (int32, bool) {
	switch v := value.(type) {
	case float64:
		if v == float64(int32(v)) {
			return int32(v), true
		}
	case int:
		return int32(v), true
	case int32:
		return v, true
	case int64:
		if v >= -2147483648 && v <= 2147483647 {
			return int32(v), true
		}
	}
	return 0, false
}

func toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case float64:
		if v == float64(int64(v)) {
			return int64(v), true
		}
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	}
	return 0, false
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Option functions
func WithInferSchema(infer bool) func(*ReadOptions) {
	return func(o *ReadOptions) {
		o.InferSchema = infer
	}
}

func WithMaxRecords(max int) func(*ReadOptions) {
	return func(o *ReadOptions) {
		o.MaxRecords = max
	}
}

func WithColumns(columns []string) func(*ReadOptions) {
	return func(o *ReadOptions) {
		o.Columns = columns
	}
}

func WithFlatten(flatten bool) func(*ReadOptions) {
	return func(o *ReadOptions) {
		o.Flatten = flatten
	}
}

func WithSkipInvalid(skip bool) func(*ReadOptions) {
	return func(o *ReadOptions) {
		o.SkipInvalid = skip
	}
}