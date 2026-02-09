package json

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Options for JSON reading
type ReadOptions struct {
	InferSchema bool
	MaxRecords  int
	Columns     []string
	SkipInvalid bool
	Flatten     bool
	DateFormat  string
}

// DefaultReadOptions returns default options for JSON reading
func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		InferSchema: true,
		MaxRecords:  0, // 0 means no limit
		Columns:     nil,
		SkipInvalid: false,
		Flatten:     false,
		DateFormat:  "",
	}
}

// Reader reads JSON data into DataFrames
type Reader struct {
	options ReadOptions
}

// NewReader creates a new JSON reader with options
func NewReader(opts ...func(*ReadOptions)) *Reader {
	options := DefaultReadOptions()
	for _, o := range opts {
		o(&options)
	}
	return &Reader{options: options}
}

// ReadFile reads a JSON file into a DataFrame
func (r *Reader) ReadFile(filename string) (*frame.DataFrame, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	var reader io.Reader = f

	// Handle gzip compression if file ends with .gz
	if strings.HasSuffix(filename, ".gz") {
		gzReader, err := gzip.NewReader(f)
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

	// Apply max records limit
	if r.options.MaxRecords > 0 && len(records) > r.options.MaxRecords {
		records = records[:r.options.MaxRecords]
	}

	// Flatten records if needed
	if r.options.Flatten {
		for i, rec := range records {
			records[i] = r.flattenRecord(rec)
		}
	} else {
		// Convert nested objects to string representation
		for i, rec := range records {
			for k, v := range rec {
				if isNested(v) {
					rec[k] = fmt.Sprintf("%v", v)
				}
			}
			records[i] = rec
		}
	}

	// Infer schema if needed
	schema := r.inferSchema(records)

	// Build series from records
	seriesList, err := r.buildSeries(records, schema)
	if err != nil {
		return nil, err
	}

	if len(seriesList) == 0 {
		return frame.NewDataFrame()
	}

	return frame.NewDataFrame(seriesList...)
}

// inferSchema infers the schema from sample records
func (r *Reader) inferSchema(records []map[string]interface{}) map[string]datatypes.DataType {
	schema := make(map[string]datatypes.DataType)

	// Collect all column names and their values
	columnValues := make(map[string][]interface{})
	for _, rec := range records {
		for k, v := range rec {
			columnValues[k] = append(columnValues[k], v)
		}
	}

	// Infer type for each column
	for colName, values := range columnValues {
		schema[colName] = inferType(values)
	}

	return schema
}

// inferType infers the data type from a slice of values
func inferType(values []interface{}) datatypes.DataType {
	// Count non-null values and track types
	hasFloat := false
	hasInt := false
	hasBool := false
	hasString := false
	nonNullCount := 0

	for _, v := range values {
		if v == nil {
			continue
		}
		nonNullCount++

		// JSON numbers are always float64
		switch v.(type) {
		case float64:
			fv := v.(float64)
			if fv == float64(int64(fv)) {
				hasInt = true
			} else {
				hasFloat = true
			}
		case bool:
			hasBool = true
		case string:
			hasString = true
		default:
			hasString = true
		}
	}

	// Default to string for all-null columns
	if nonNullCount == 0 {
		return datatypes.String{}
	}

	// If any value is string, use string type
	if hasString {
		return datatypes.String{}
	}

	// Determine type based on counts
	if hasBool && !hasInt && !hasFloat {
		return datatypes.Boolean{}
	}
	if hasFloat {
		return datatypes.Float64{}
	}
	if hasInt {
		return datatypes.Int64{}
	}

	return datatypes.String{}
}

// buildSeries builds series from records using the inferred schema
func (r *Reader) buildSeries(records []map[string]interface{}, schema map[string]datatypes.DataType) ([]series.Series, error) {
	// Collect ordered column names from first record, then fill in others
	seen := make(map[string]bool)
	var colNames []string
	for _, rec := range records {
		for k := range rec {
			if !seen[k] {
				seen[k] = true
				colNames = append(colNames, k)
			}
		}
	}

	// Create builders for each column
	builders := make([]*seriesBuilder, len(colNames))
	for i, name := range colNames {
		dtype, ok := schema[name]
		if !ok {
			dtype = datatypes.String{}
		}
		builders[i] = newSeriesBuilder(name, dtype, len(records))
	}

	// Populate builders
	for _, rec := range records {
		for i, name := range colNames {
			val, exists := rec[name]
			if !exists || val == nil {
				builders[i].appendNull()
			} else {
				builders[i].append(val)
			}
		}
	}

	// Build series
	result := make([]series.Series, len(colNames))
	for i, b := range builders {
		s, err := b.build()
		if err != nil {
			return nil, fmt.Errorf("error building series %s: %w", colNames[i], err)
		}
		result[i] = s
	}

	return result, nil
}

// flattenRecord flattens nested objects in a record
func (r *Reader) flattenRecord(record map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range record {
		if m, ok := v.(map[string]interface{}); ok {
			for fk, fv := range r.flattenObject(k, m) {
				result[fk] = fv
			}
		} else if _, ok := v.([]interface{}); ok {
			result[k] = fmt.Sprintf("%v", v)
		} else {
			result[k] = v
		}
	}
	return result
}

// flattenObject recursively flattens nested objects
func (r *Reader) flattenObject(prefix string, value interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	switch v := value.(type) {
	case map[string]interface{}:
		for k, val := range v {
			key := prefix + "." + k
			for fk, fv := range r.flattenObject(key, val) {
				result[fk] = fv
			}
		}
	case []interface{}:
		// For arrays, convert to string representation
		result[prefix] = fmt.Sprintf("%v", v)
	default:
		result[prefix] = v
	}

	return result
}

// isNested checks if a value is a nested object or array
func isNested(value interface{}) bool {
	switch value.(type) {
	case map[string]interface{}, []interface{}:
		return true
	}
	return false
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
		nullMask: make([]bool, capacity),
		size:     capacity,
		index:    0,
	}

	// Initialize typed slice based on data type
	switch dtype.(type) {
	case datatypes.Boolean:
		sb.values = make([]bool, capacity)
	case datatypes.Int64:
		sb.values = make([]int64, capacity)
	case datatypes.Float64:
		sb.values = make([]float64, capacity)
	case datatypes.String:
		sb.values = make([]string, capacity)
	default:
		sb.values = make([]string, capacity)
	}

	return sb
}

func (sb *seriesBuilder) append(value interface{}) {
	if sb.index >= sb.size {
		return
	}

	switch sb.dtype.(type) {
	case datatypes.Boolean:
		vals := sb.values.([]bool)
		if b, ok := value.(bool); ok {
			vals[sb.index] = b
			sb.nullMask[sb.index] = true
		} else {
			sb.nullMask[sb.index] = false
		}
	case datatypes.Int64:
		vals := sb.values.([]int64)
		if v, ok := toInt64(value); ok {
			vals[sb.index] = v
			sb.nullMask[sb.index] = true
		} else {
			sb.nullMask[sb.index] = false
		}
	case datatypes.Float64:
		vals := sb.values.([]float64)
		if v, ok := toFloat64(value); ok {
			vals[sb.index] = v
			sb.nullMask[sb.index] = true
		} else {
			sb.nullMask[sb.index] = false
		}
	case datatypes.String:
		vals := sb.values.([]string)
		vals[sb.index] = toString(value)
		sb.nullMask[sb.index] = true
	}

	sb.index++
}

func (sb *seriesBuilder) appendNull() {
	if sb.index >= sb.size {
		return
	}
	sb.nullMask[sb.index] = false
	sb.index++
}

func (sb *seriesBuilder) build() (series.Series, error) {
	// For now, we create series without null mask support
	// Null positions get zero values as placeholders
	switch sb.dtype.(type) {
	case datatypes.Boolean:
		vals := sb.values.([]bool)
		return series.NewSeries(sb.name, vals[:sb.index], datatypes.Boolean{}), nil
	case datatypes.Int64:
		vals := sb.values.([]int64)
		return series.NewSeries(sb.name, vals[:sb.index], datatypes.Int64{}), nil
	case datatypes.Float64:
		vals := sb.values.([]float64)
		return series.NewSeries(sb.name, vals[:sb.index], datatypes.Float64{}), nil
	case datatypes.String:
		vals := sb.values.([]string)
		return series.NewSeries(sb.name, vals[:sb.index], datatypes.String{}), nil
	default:
		vals := sb.values.([]string)
		return series.NewSeries(sb.name, vals[:sb.index], datatypes.String{}), nil
	}
}

// Type conversion helpers
func toInt32(value interface{}) (int32, bool) {
	switch v := value.(type) {
	case float64:
		return int32(v), true
	case int:
		return int32(v), true
	case int32:
		return v, true
	case int64:
		return int32(v), true
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return 0, false
		}
		return int32(i), true
	}
	return 0, false
}

func toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case nil:
		return ""
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
