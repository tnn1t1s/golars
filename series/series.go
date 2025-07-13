package series

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/davidpalaitis/golars/chunked"
	"github.com/davidpalaitis/golars/datatypes"
)

// Series is a named column with a data type
// It provides a type-erased interface over typed ChunkedArrays
type Series interface {
	// Name returns the name of the series
	Name() string
	
	// Rename returns a new series with a different name
	Rename(name string) Series
	
	// DataType returns the data type of the series
	DataType() datatypes.DataType
	
	// Len returns the number of elements
	Len() int
	
	// IsNull returns true if the value at index i is null
	IsNull(i int) bool
	
	// IsValid returns true if the value at index i is valid (not null)
	IsValid(i int) bool
	
	// NullCount returns the number of null values
	NullCount() int
	
	// Slice returns a new series with elements from start to end (exclusive)
	Slice(start, end int) (Series, error)
	
	// Head returns the first n elements
	Head(n int) Series
	
	// Tail returns the last n elements  
	Tail(n int) Series
	
	// Cast attempts to cast the series to a different data type
	Cast(dt datatypes.DataType) (Series, error)
	
	// Equals returns true if the series are equal
	Equals(other Series) bool
	
	// Clone returns a copy of the series
	Clone() Series
	
	// Get returns the value at index i as an interface{}
	Get(i int) interface{}
	
	// GetAsString returns the value at index i as a string representation
	GetAsString(i int) string
	
	// ToSlice returns the underlying data as a slice
	ToSlice() interface{}
	
	// String returns a string representation of the series
	String() string
	
	// Sorting
	Sort(ascending bool) Series
	ArgSort(config SortConfig) []int
	Take(indices []int) Series
	
	// Aggregation methods
	Sum() float64
	Mean() float64
	Min() interface{}
	Max() interface{}
	Count() int
	Std() float64
	Var() float64
	Median() float64
}

// TypedSeries is a generic series implementation for specific types
type TypedSeries[T datatypes.ArrayValue] struct {
	chunkedArray *chunked.ChunkedArray[T]
	name         string
}

// NewSeries creates a new series from a slice of values
func NewSeries[T datatypes.ArrayValue](name string, values []T, dt datatypes.DataType) Series {
	ca := chunked.NewChunkedArray[T](name, dt)
	if len(values) > 0 {
		_ = ca.AppendSlice(values, nil)
	}
	
	return &TypedSeries[T]{
		chunkedArray: ca,
		name:         name,
	}
}

// NewSeriesWithValidity creates a new series with explicit null values
func NewSeriesWithValidity[T datatypes.ArrayValue](name string, values []T, validity []bool, dt datatypes.DataType) Series {
	ca := chunked.NewChunkedArray[T](name, dt)
	if len(values) > 0 {
		_ = ca.AppendSlice(values, validity)
	}
	
	return &TypedSeries[T]{
		chunkedArray: ca,
		name:         name,
	}
}

// NewSeriesFromChunkedArray creates a series from an existing ChunkedArray
func NewSeriesFromChunkedArray[T datatypes.ArrayValue](ca *chunked.ChunkedArray[T]) Series {
	return &TypedSeries[T]{
		chunkedArray: ca,
		name:         ca.Name(),
	}
}

// Implementation of Series interface for TypedSeries

func (s *TypedSeries[T]) Name() string {
	return s.name
}

func (s *TypedSeries[T]) Rename(name string) Series {
	newCA := chunked.NewChunkedArray[T](name, s.chunkedArray.DataType())
	for _, chunk := range s.chunkedArray.Chunks() {
		_ = newCA.AppendArray(chunk)
	}
	
	return &TypedSeries[T]{
		chunkedArray: newCA,
		name:         name,
	}
}

func (s *TypedSeries[T]) DataType() datatypes.DataType {
	return s.chunkedArray.DataType()
}

func (s *TypedSeries[T]) Len() int {
	return int(s.chunkedArray.Len())
}

func (s *TypedSeries[T]) IsNull(i int) bool {
	return !s.chunkedArray.IsValid(int64(i))
}

func (s *TypedSeries[T]) IsValid(i int) bool {
	return s.chunkedArray.IsValid(int64(i))
}

func (s *TypedSeries[T]) NullCount() int {
	return int(s.chunkedArray.NullCount())
}

func (s *TypedSeries[T]) Slice(start, end int) (Series, error) {
	sliced, err := s.chunkedArray.Slice(int64(start), int64(end))
	if err != nil {
		return nil, err
	}
	
	return &TypedSeries[T]{
		chunkedArray: sliced,
		name:         s.name,
	}, nil
}

func (s *TypedSeries[T]) Head(n int) Series {
	if n < 0 || n > s.Len() {
		n = s.Len()
	}
	result, _ := s.Slice(0, n)
	return result
}

func (s *TypedSeries[T]) Tail(n int) Series {
	length := s.Len()
	if n < 0 || n > length {
		n = length
	}
	result, _ := s.Slice(length-n, length)
	return result
}

func (s *TypedSeries[T]) Cast(dt datatypes.DataType) (Series, error) {
	// TODO: Implement type casting
	return nil, fmt.Errorf("casting not yet implemented")
}

func (s *TypedSeries[T]) Equals(other Series) bool {
	if other == nil {
		return false
	}
	
	if s.Len() != other.Len() {
		return false
	}
	
	if !s.DataType().Equals(other.DataType()) {
		return false
	}
	
	// Compare values
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) != other.IsNull(i) {
			return false
		}
		if !s.IsNull(i) {
			v1 := s.Get(i)
			v2 := other.Get(i)
			if !reflect.DeepEqual(v1, v2) {
				return false
			}
		}
	}
	
	return true
}

func (s *TypedSeries[T]) Clone() Series {
	return s.Rename(s.name)
}

func (s *TypedSeries[T]) Get(i int) interface{} {
	val, valid := s.chunkedArray.Get(int64(i))
	if !valid {
		return nil
	}
	return val
}

func (s *TypedSeries[T]) GetAsString(i int) string {
	if s.IsNull(i) {
		return "null"
	}
	val := s.Get(i)
	return fmt.Sprintf("%v", val)
}

func (s *TypedSeries[T]) ToSlice() interface{} {
	values, _ := s.chunkedArray.ToSlice()
	return values
}

func (s *TypedSeries[T]) String() string {
	const maxDisplay = 10
	
	str := fmt.Sprintf("Series: %s [%s]\n", s.name, s.DataType())
	str += "[\n"
	
	displayLen := s.Len()
	if displayLen > maxDisplay {
		displayLen = maxDisplay
	}
	
	for i := 0; i < displayLen; i++ {
		str += fmt.Sprintf("\t%s\n", s.GetAsString(i))
	}
	
	if s.Len() > maxDisplay {
		str += fmt.Sprintf("\t... %d more values\n", s.Len()-maxDisplay)
	}
	
	str += "]"
	return str
}

// Helper functions to create series of specific types

func NewBooleanSeries(name string, values []bool) Series {
	return NewSeries(name, values, datatypes.Boolean{})
}

func NewInt8Series(name string, values []int8) Series {
	return NewSeries(name, values, datatypes.Int8{})
}

func NewInt16Series(name string, values []int16) Series {
	return NewSeries(name, values, datatypes.Int16{})
}

func NewInt32Series(name string, values []int32) Series {
	return NewSeries(name, values, datatypes.Int32{})
}

func NewInt64Series(name string, values []int64) Series {
	return NewSeries(name, values, datatypes.Int64{})
}

func NewUInt8Series(name string, values []uint8) Series {
	return NewSeries(name, values, datatypes.UInt8{})
}

func NewUInt16Series(name string, values []uint16) Series {
	return NewSeries(name, values, datatypes.UInt16{})
}

func NewUInt32Series(name string, values []uint32) Series {
	return NewSeries(name, values, datatypes.UInt32{})
}

func NewUInt64Series(name string, values []uint64) Series {
	return NewSeries(name, values, datatypes.UInt64{})
}

func NewFloat32Series(name string, values []float32) Series {
	return NewSeries(name, values, datatypes.Float32{})
}

func NewFloat64Series(name string, values []float64) Series {
	return NewSeries(name, values, datatypes.Float64{})
}

func NewStringSeries(name string, values []string) Series {
	return NewSeries(name, values, datatypes.String{})
}

func NewBinarySeries(name string, values [][]byte) Series {
	return NewSeries(name, values, datatypes.Binary{})
}

// SeriesFromArrowArray creates a Series from an Arrow array
func SeriesFromArrowArray(name string, arr arrow.Array) (Series, error) {
	dt := datatypes.FromArrowType(arr.DataType())
	
	switch dt.(type) {
	case datatypes.Boolean:
		ca := chunked.NewChunkedArray[bool](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Int8:
		ca := chunked.NewChunkedArray[int8](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Int16:
		ca := chunked.NewChunkedArray[int16](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Int32:
		ca := chunked.NewChunkedArray[int32](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Int64:
		ca := chunked.NewChunkedArray[int64](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.UInt8:
		ca := chunked.NewChunkedArray[uint8](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.UInt16:
		ca := chunked.NewChunkedArray[uint16](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.UInt32:
		ca := chunked.NewChunkedArray[uint32](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.UInt64:
		ca := chunked.NewChunkedArray[uint64](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Float32:
		ca := chunked.NewChunkedArray[float32](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Float64:
		ca := chunked.NewChunkedArray[float64](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.String:
		ca := chunked.NewChunkedArray[string](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	case datatypes.Binary:
		ca := chunked.NewChunkedArray[[]byte](name, dt)
		_ = ca.AppendArray(arr)
		return NewSeriesFromChunkedArray(ca), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dt)
	}
}