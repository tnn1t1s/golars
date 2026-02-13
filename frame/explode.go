package frame

import (
	"fmt"
	"reflect"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Explode expands list-like values into multiple rows.
func (df *DataFrame) Explode(column string) (*DataFrame, error) {
	colIdx := -1
	for i, col := range df.columns {
		if col.Name() == column {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return nil, fmt.Errorf("column %q not found", column)
	}

	explodeCol := df.columns[colIdx]

	// Calculate total rows after explode
	totalRows := 0
	rowLengths := make([]int, df.height)
	for i := 0; i < df.height; i++ {
		if explodeCol.IsNull(i) {
			rowLengths[i] = 1 // null produces one row with null
			totalRows++
			continue
		}
		val := explodeCol.Get(i)
		length, err := listLength(val)
		if err != nil {
			// Not a list, treat as single value
			rowLengths[i] = 1
			totalRows++
			continue
		}
		if length == 0 {
			rowLengths[i] = 1 // empty list produces one row with null
			totalRows++
		} else {
			rowLengths[i] = length
			totalRows += length
		}
	}

	// Build result columns
	var resultCols []series.Series

	for ci, col := range df.columns {
		if ci == colIdx {
			// Explode this column
			buf, err := newColumnBuffer(col.Name(), inferExplodedType(explodeCol), totalRows)
			if err != nil {
				return nil, fmt.Errorf("creating buffer for exploded column: %w", err)
			}

			outIdx := 0
			for row := 0; row < df.height; row++ {
				if explodeCol.IsNull(row) {
					// null -> null
					buf.validity[outIdx] = false
					outIdx++
					continue
				}
				val := explodeCol.Get(row)
				length, err := listLength(val)
				if err != nil || length == 0 {
					// scalar or empty list -> single value or null
					if err != nil {
						_ = buf.set(outIdx, val)
						buf.validity[outIdx] = true
					}
					outIdx++
					continue
				}
				for li := 0; li < length; li++ {
					lv, err := listValue(val, li)
					if err != nil || lv == nil {
						buf.validity[outIdx] = false
					} else {
						_ = buf.set(outIdx, lv)
						buf.validity[outIdx] = true
					}
					outIdx++
				}
			}

			s, err := buf.build()
			if err != nil {
				return nil, err
			}
			resultCols = append(resultCols, s)
		} else {
			// Replicate this column based on row lengths
			buf, err := newColumnBuffer(col.Name(), col.DataType(), totalRows)
			if err != nil {
				return nil, fmt.Errorf("creating buffer for column %q: %w", col.Name(), err)
			}

			outIdx := 0
			for row := 0; row < df.height; row++ {
				for rep := 0; rep < rowLengths[row]; rep++ {
					if col.IsNull(row) {
						buf.validity[outIdx] = false
					} else {
						_ = buf.set(outIdx, col.Get(row))
						buf.validity[outIdx] = true
					}
					outIdx++
				}
			}

			s, err := buf.build()
			if err != nil {
				return nil, err
			}
			resultCols = append(resultCols, s)
		}
	}

	return NewDataFrame(resultCols...)
}

// inferExplodedType tries to determine the element type of a list column
func inferExplodedType(col series.Series) datatypes.DataType {
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		val := col.Get(i)
		_, err := listLength(val)
		if err != nil {
			return col.DataType()
		}
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Slice && rv.Len() > 0 {
			elem := rv.Index(0).Interface()
			switch elem.(type) {
			case int64:
				return datatypes.Int64{}
			case int32:
				return datatypes.Int32{}
			case float64:
				return datatypes.Float64{}
			case float32:
				return datatypes.Float32{}
			case string:
				return datatypes.String{}
			case bool:
				return datatypes.Boolean{}
			}
		}
		break
	}
	return datatypes.Float64{} // default
}

type columnBuffer struct {
	name     string
	dtype    datatypes.DataType
	values   interface{}
	validity []bool
}

func newColumnBuffer(name string, dtype datatypes.DataType, size int) (*columnBuffer, error) {
	buf := &columnBuffer{
		name:     name,
		dtype:    dtype,
		validity: make([]bool, size),
	}

	switch dtype.(type) {
	case datatypes.Int32:
		buf.values = make([]int32, size)
	case datatypes.Int64:
		buf.values = make([]int64, size)
	case datatypes.Float32:
		buf.values = make([]float32, size)
	case datatypes.Float64:
		buf.values = make([]float64, size)
	case datatypes.String:
		buf.values = make([]string, size)
	case datatypes.Boolean:
		buf.values = make([]bool, size)
	default:
		// Fall back to float64
		buf.values = make([]float64, size)
		buf.dtype = datatypes.Float64{}
	}

	return buf, nil
}

func (b *columnBuffer) set(idx int, value interface{}) error {
	switch vals := b.values.(type) {
	case []int32:
		switch v := value.(type) {
		case int32:
			vals[idx] = v
		case int64:
			vals[idx] = int32(v)
		case float64:
			vals[idx] = int32(v)
		default:
			vals[idx] = int32(toFloat64Value(value))
		}
	case []int64:
		switch v := value.(type) {
		case int64:
			vals[idx] = v
		case int32:
			vals[idx] = int64(v)
		case float64:
			vals[idx] = int64(v)
		default:
			vals[idx] = int64(toFloat64Value(value))
		}
	case []float32:
		switch v := value.(type) {
		case float32:
			vals[idx] = v
		case float64:
			vals[idx] = float32(v)
		default:
			vals[idx] = float32(toFloat64Value(value))
		}
	case []float64:
		switch v := value.(type) {
		case float64:
			vals[idx] = v
		case float32:
			vals[idx] = float64(v)
		case int64:
			vals[idx] = float64(v)
		case int32:
			vals[idx] = float64(v)
		default:
			vals[idx] = toFloat64Value(value)
		}
	case []string:
		switch v := value.(type) {
		case string:
			vals[idx] = v
		default:
			vals[idx] = fmt.Sprintf("%v", value)
		}
	case []bool:
		switch v := value.(type) {
		case bool:
			vals[idx] = v
		}
	}
	return nil
}

func (b *columnBuffer) build() (series.Series, error) {
	switch vals := b.values.(type) {
	case []int32:
		return series.NewSeriesWithValidity(b.name, vals, b.validity, b.dtype), nil
	case []int64:
		return series.NewSeriesWithValidity(b.name, vals, b.validity, b.dtype), nil
	case []float32:
		return series.NewSeriesWithValidity(b.name, vals, b.validity, b.dtype), nil
	case []float64:
		return series.NewSeriesWithValidity(b.name, vals, b.validity, b.dtype), nil
	case []string:
		return series.NewSeriesWithValidity(b.name, vals, b.validity, b.dtype), nil
	case []bool:
		return series.NewSeriesWithValidity(b.name, vals, b.validity, b.dtype), nil
	default:
		return nil, fmt.Errorf("unsupported buffer type for column %q", b.name)
	}
}

func listLength(value interface{}) (int, error) {
	if value == nil {
		return 0, fmt.Errorf("nil value")
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return rv.Len(), nil
	default:
		return 0, fmt.Errorf("not a list type: %T", value)
	}
}

func listValue(value interface{}, idx int) (interface{}, error) {
	if value == nil {
		return nil, fmt.Errorf("nil value")
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if idx < 0 || idx >= rv.Len() {
			return nil, fmt.Errorf("index %d out of range for list of length %d", idx, rv.Len())
		}
		return rv.Index(idx).Interface(), nil
	default:
		return nil, fmt.Errorf("not a list type: %T", value)
	}
}
