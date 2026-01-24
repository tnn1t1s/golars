package frame

import (
	"fmt"
	"reflect"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Explode expands list-like values into multiple rows.
func (df *DataFrame) Explode(column string) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	colIdx := -1
	for i, field := range df.schema.Fields {
		if field.Name == column {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return nil, fmt.Errorf("column %q not found", column)
	}

	listCol := df.columns[colIdx]
	listType, ok := listCol.DataType().(datatypes.List)
	if !ok {
		return nil, fmt.Errorf("column %q is not a list type", column)
	}

	rowCounts := make([]int, df.height)
	totalRows := 0
	for i := 0; i < df.height; i++ {
		val := listCol.Get(i)
		if val == nil {
			rowCounts[i] = 1
			totalRows++
			continue
		}
		length, err := listLength(val)
		if err != nil {
			return nil, err
		}
		rowCounts[i] = length
		totalRows += length
	}

	buffers := make([]*columnBuffer, len(df.columns))
	for i, col := range df.columns {
		dt := col.DataType()
		if i == colIdx {
			dt = listType.Inner
		}
		buf, err := newColumnBuffer(col.Name(), dt, totalRows)
		if err != nil {
			return nil, err
		}
		buffers[i] = buf
	}

	outIdx := 0
	for row := 0; row < df.height; row++ {
		count := rowCounts[row]
		if count == 0 {
			continue
		}
		listVal := listCol.Get(row)
		for i := 0; i < count; i++ {
			for colIndex, col := range df.columns {
				if colIndex == colIdx {
					continue
				}
				if err := buffers[colIndex].set(outIdx, col.Get(row)); err != nil {
					return nil, err
				}
			}
			var exploded interface{}
			if listVal == nil {
				exploded = nil
			} else {
				value, err := listValue(listVal, i)
				if err != nil {
					return nil, err
				}
				exploded = value
			}
			if err := buffers[colIdx].set(outIdx, exploded); err != nil {
				return nil, err
			}
			outIdx++
		}
	}

	cols := make([]series.Series, len(buffers))
	for i, buf := range buffers {
		col, err := buf.build()
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}

	return NewDataFrame(cols...)
}

type columnBuffer struct {
	name     string
	dtype    datatypes.DataType
	values   interface{}
	validity []bool
}

func newColumnBuffer(name string, dtype datatypes.DataType, size int) (*columnBuffer, error) {
	switch dtype.(type) {
	case datatypes.Int64:
		return &columnBuffer{name: name, dtype: dtype, values: make([]int64, size), validity: make([]bool, size)}, nil
	case datatypes.Int32:
		return &columnBuffer{name: name, dtype: dtype, values: make([]int32, size), validity: make([]bool, size)}, nil
	case datatypes.Float64:
		return &columnBuffer{name: name, dtype: dtype, values: make([]float64, size), validity: make([]bool, size)}, nil
	case datatypes.Float32:
		return &columnBuffer{name: name, dtype: dtype, values: make([]float32, size), validity: make([]bool, size)}, nil
	case datatypes.String:
		return &columnBuffer{name: name, dtype: dtype, values: make([]string, size), validity: make([]bool, size)}, nil
	case datatypes.Boolean:
		return &columnBuffer{name: name, dtype: dtype, values: make([]bool, size), validity: make([]bool, size)}, nil
	default:
		return nil, fmt.Errorf("unsupported explode type %s", dtype.String())
	}
}

func (b *columnBuffer) set(idx int, value interface{}) error {
	if value == nil {
		return nil
	}
	switch b.dtype.(type) {
	case datatypes.Int64:
		val, ok := value.(int64)
		if !ok {
			return fmt.Errorf("cannot cast %T to int64", value)
		}
		b.values.([]int64)[idx] = val
	case datatypes.Int32:
		val, ok := value.(int32)
		if !ok {
			return fmt.Errorf("cannot cast %T to int32", value)
		}
		b.values.([]int32)[idx] = val
	case datatypes.Float64:
		val, ok := value.(float64)
		if !ok {
			return fmt.Errorf("cannot cast %T to float64", value)
		}
		b.values.([]float64)[idx] = val
	case datatypes.Float32:
		val, ok := value.(float32)
		if !ok {
			return fmt.Errorf("cannot cast %T to float32", value)
		}
		b.values.([]float32)[idx] = val
	case datatypes.String:
		val, ok := value.(string)
		if !ok {
			return fmt.Errorf("cannot cast %T to string", value)
		}
		b.values.([]string)[idx] = val
	case datatypes.Boolean:
		val, ok := value.(bool)
		if !ok {
			return fmt.Errorf("cannot cast %T to bool", value)
		}
		b.values.([]bool)[idx] = val
	default:
		return fmt.Errorf("unsupported explode type %s", b.dtype.String())
	}
	b.validity[idx] = true
	return nil
}

func (b *columnBuffer) build() (series.Series, error) {
	switch b.dtype.(type) {
	case datatypes.Int64:
		return series.NewSeriesWithValidity(b.name, b.values.([]int64), b.validity, b.dtype), nil
	case datatypes.Int32:
		return series.NewSeriesWithValidity(b.name, b.values.([]int32), b.validity, b.dtype), nil
	case datatypes.Float64:
		return series.NewSeriesWithValidity(b.name, b.values.([]float64), b.validity, b.dtype), nil
	case datatypes.Float32:
		return series.NewSeriesWithValidity(b.name, b.values.([]float32), b.validity, b.dtype), nil
	case datatypes.String:
		return series.NewSeriesWithValidity(b.name, b.values.([]string), b.validity, b.dtype), nil
	case datatypes.Boolean:
		return series.NewSeriesWithValidity(b.name, b.values.([]bool), b.validity, b.dtype), nil
	default:
		return nil, fmt.Errorf("unsupported explode type %s", b.dtype.String())
	}
}

func listLength(value interface{}) (int, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice {
		return 0, fmt.Errorf("expected list value, got %T", value)
	}
	return rv.Len(), nil
}

func listValue(value interface{}, idx int) (interface{}, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected list value, got %T", value)
	}
	if idx < 0 || idx >= rv.Len() {
		return nil, fmt.Errorf("list index %d out of range", idx)
	}
	return rv.Index(idx).Interface(), nil
}
