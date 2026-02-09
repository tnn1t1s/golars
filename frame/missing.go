package frame

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// FillNullOptions configures null filling operations
type FillNullOptions struct {
	Value   interface{} // Value to fill nulls with
	Method  string      // Method: "forward", "backward", "value"
	Limit   int         // Maximum number of consecutive nulls to fill
	Columns []string    // Specific columns to fill (empty means all)
}

// FillNull fills null values in the DataFrame
func (df *DataFrame) FillNull(options FillNullOptions) (*DataFrame, error) {
	if options.Method == "" {
		options.Method = "value"
	}

	colSet := make(map[string]bool, len(options.Columns))
	for _, name := range options.Columns {
		colSet[name] = true
	}
	allColumns := len(options.Columns) == 0

	newCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		if !allColumns && !colSet[col.Name()] {
			newCols[i] = col
			continue
		}

		switch options.Method {
		case "forward":
			newCols[i] = forwardFillSeries(col, options.Limit)
		case "backward":
			newCols[i] = backwardFillSeries(col, options.Limit)
		case "value":
			newCols[i] = valueFillSeries(col, options.Value)
		default:
			return nil, fmt.Errorf("unsupported fill method: %s", options.Method)
		}
	}
	return NewDataFrame(newCols...)
}

// ForwardFill fills null values with the previous non-null value
func (df *DataFrame) ForwardFill(columns ...string) (*DataFrame, error) {
	return df.FillNull(FillNullOptions{
		Method:  "forward",
		Columns: columns,
	})
}

// BackwardFill fills null values with the next non-null value
func (df *DataFrame) BackwardFill(columns ...string) (*DataFrame, error) {
	return df.FillNull(FillNullOptions{
		Method:  "backward",
		Columns: columns,
	})
}

// Helper function to forward fill a series
func forwardFillSeries(s series.Series, limit int) series.Series {
	n := s.Len()
	values := make([]interface{}, n)
	validity := make([]bool, n)

	var lastValid interface{}
	hasLast := false
	consecutive := 0

	for i := 0; i < n; i++ {
		if !s.IsNull(i) {
			values[i] = s.Get(i)
			validity[i] = true
			lastValid = s.Get(i)
			hasLast = true
			consecutive = 0
		} else if hasLast {
			consecutive++
			if limit <= 0 || consecutive <= limit {
				values[i] = lastValid
				validity[i] = true
			}
		}
	}

	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper function to backward fill a series
func backwardFillSeries(s series.Series, limit int) series.Series {
	n := s.Len()
	values := make([]interface{}, n)
	validity := make([]bool, n)

	var nextValid interface{}
	hasNext := false
	consecutive := 0

	for i := n - 1; i >= 0; i-- {
		if !s.IsNull(i) {
			values[i] = s.Get(i)
			validity[i] = true
			nextValid = s.Get(i)
			hasNext = true
			consecutive = 0
		} else if hasNext {
			consecutive++
			if limit <= 0 || consecutive <= limit {
				values[i] = nextValid
				validity[i] = true
			}
		}
	}

	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper function to fill with a specific value
func valueFillSeries(s series.Series, fillValue interface{}) series.Series {
	n := s.Len()
	values := make([]interface{}, n)
	validity := make([]bool, n)

	// Coerce fill value to match series type
	coerced := coerceFillValue(fillValue, s.DataType())

	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			values[i] = coerced
		} else {
			values[i] = s.Get(i)
		}
		validity[i] = true
	}

	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// coerceFillValue converts a fill value to match the target data type
func coerceFillValue(value interface{}, dtype datatypes.DataType) interface{} {
	switch dtype.(type) {
	case datatypes.Int64:
		switch v := value.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		}
	case datatypes.Int32:
		switch v := value.(type) {
		case int32:
			return v
		case int:
			return int32(v)
		case int64:
			return int32(v)
		case float64:
			return int32(v)
		}
	case datatypes.Float64:
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	case datatypes.Float32:
		switch v := value.(type) {
		case float32:
			return v
		case float64:
			return float32(v)
		}
	case datatypes.String:
		switch v := value.(type) {
		case string:
			return v
		}
	case datatypes.Boolean:
		switch v := value.(type) {
		case bool:
			return v
		}
	}
	return value
}

// Helper to create series from interface values
func createSeriesFromInterface(name string, values []interface{}, validity []bool, dataType datatypes.DataType) series.Series {
	if validity == nil {
		validity = make([]bool, len(values))
		for i := range validity {
			validity[i] = true
		}
	}

	switch dataType.(type) {
	case datatypes.Int32:
		typed := make([]int32, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				typed[i] = toInt32Value(v)
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	case datatypes.Int64:
		typed := make([]int64, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				typed[i] = toInt64Value(v)
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	case datatypes.Float64:
		typed := make([]float64, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				typed[i] = toFloat64Value(v)
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	case datatypes.Float32:
		typed := make([]float32, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				typed[i] = toFloat32Value(v)
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	case datatypes.String:
		typed := make([]string, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				typed[i] = toStringValue(v)
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	case datatypes.Boolean:
		typed := make([]bool, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				if bv, ok := v.(bool); ok {
					typed[i] = bv
				}
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	default:
		// Fallback: convert to string
		typed := make([]string, len(values))
		for i, v := range values {
			if v != nil && validity[i] {
				typed[i] = fmt.Sprintf("%v", v)
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, datatypes.String{})
	}
}

func toInt32Value(v interface{}) int32 {
	switch val := v.(type) {
	case int32:
		return val
	case int64:
		return int32(val)
	case int:
		return int32(val)
	case float64:
		return int32(val)
	case float32:
		return int32(val)
	default:
		return 0
	}
}

func toInt64Value(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	default:
		return 0
	}
}

func toFloat32Value(v interface{}) float32 {
	switch val := v.(type) {
	case float32:
		return val
	case float64:
		return float32(val)
	case int:
		return float32(val)
	case int32:
		return float32(val)
	case int64:
		return float32(val)
	default:
		return 0
	}
}

func toStringValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// DropNull removes rows with null values
func (df *DataFrame) DropNull(subset ...string) (*DataFrame, error) {
	checkCols := subset
	if len(checkCols) == 0 {
		checkCols = df.Columns()
	}

	// Find column series to check
	colsToCheck := make([]series.Series, len(checkCols))
	for i, name := range checkCols {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		colsToCheck[i] = col
	}

	// Find rows to keep
	keepIndices := make([]int, 0, df.height)
	for row := 0; row < df.height; row++ {
		hasNull := false
		for _, col := range colsToCheck {
			if col.IsNull(row) {
				hasNull = true
				break
			}
		}
		if !hasNull {
			keepIndices = append(keepIndices, row)
		}
	}

	if len(keepIndices) == df.height {
		return df.Clone(), nil
	}

	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		taken, ok := series.TakeFast(col, keepIndices)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(keepIndices)
		}
	}
	return NewDataFrame(cols...)
}

// DropDuplicates removes duplicate rows
type DropDuplicatesOptions struct {
	Subset []string // Columns to consider for duplicates (empty means all)
	Keep   string   // Which duplicate to keep: "first", "last", "none"
}

// DropDuplicates removes duplicate rows from the DataFrame
func (df *DataFrame) DropDuplicates(options DropDuplicatesOptions) (*DataFrame, error) {
	if options.Keep == "" {
		options.Keep = "first"
	}

	// Validate keep option
	switch options.Keep {
	case "first", "last", "none":
	default:
		return nil, fmt.Errorf("invalid keep option: %s", options.Keep)
	}

	// Determine which columns to check
	subset := options.Subset
	if len(subset) == 0 {
		subset = df.Columns()
	}

	checkCols := make([]series.Series, len(subset))
	for i, name := range subset {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		checkCols[i] = col
	}

	// Track seen rows
	seen := make(map[string][]int)
	for row := 0; row < df.height; row++ {
		var parts []string
		for _, col := range checkCols {
			parts = append(parts, col.GetAsString(row))
		}
		key := strings.Join(parts, "\x00")
		seen[key] = append(seen[key], row)
	}

	// Determine which rows to keep
	keepSet := make(map[int]bool)
	for _, indices := range seen {
		switch options.Keep {
		case "first":
			keepSet[indices[0]] = true
		case "last":
			keepSet[indices[len(indices)-1]] = true
		case "none":
			if len(indices) == 1 {
				keepSet[indices[0]] = true
			}
		}
	}

	keepRows := make([]int, 0, len(keepSet))
	for idx := range keepSet {
		keepRows = append(keepRows, idx)
	}
	sort.Ints(keepRows)

	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		taken, ok := series.TakeFast(col, keepRows)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(keepRows)
		}
	}
	return NewDataFrame(cols...)
}
