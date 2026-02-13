package series

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/tnn1t1s/golars/internal/chunked"
	"github.com/tnn1t1s/golars/internal/datatypes"
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
		ca.AppendSlice(values, nil)
	}
	return &TypedSeries[T]{
		chunkedArray: ca,
		name:         name,
	}
}

// NewSeriesWithValidity creates a new series with explicit null values
func NewSeriesWithValidity[T datatypes.ArrayValue](name string, values []T, validity []bool, dt datatypes.DataType) Series {
	ca := chunked.NewChunkedArray[T](name, dt)
	ca.AppendSlice(values, validity)
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
	cloned := s.Clone().(*TypedSeries[T])
	cloned.name = name
	return cloned
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
	if start < 0 || end > s.Len() || start > end {
		return nil, fmt.Errorf("invalid slice bounds [%d:%d] for length %d", start, end, s.Len())
	}
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
	if n <= 0 {
		ca := chunked.NewChunkedArray[T](s.name, s.DataType())
		return &TypedSeries[T]{chunkedArray: ca, name: s.name}
	}
	if n > s.Len() {
		n = s.Len()
	}
	result, _ := s.Slice(0, n)
	return result
}

func (s *TypedSeries[T]) Tail(n int) Series {
	if n <= 0 {
		ca := chunked.NewChunkedArray[T](s.name, s.DataType())
		return &TypedSeries[T]{chunkedArray: ca, name: s.name}
	}
	length := s.Len()
	if n > length {
		n = length
	}
	result, _ := s.Slice(length-n, length)
	return result
}

func (s *TypedSeries[T]) Cast(dt datatypes.DataType) (Series, error) {
	return castSeries(s, dt)
}

func (s *TypedSeries[T]) Equals(other Series) bool {
	if other == nil {
		return false
	}
	if s.Len() != other.Len() {
		return false
	}
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) != other.IsNull(i) {
			return false
		}
		if s.IsNull(i) {
			continue
		}
		v1 := s.Get(i)
		v2 := other.Get(i)
		if !reflect.DeepEqual(v1, v2) {
			return false
		}
	}
	return true
}

func (s *TypedSeries[T]) Clone() Series {
	values, validity := s.chunkedArray.ToSlice()
	ca := chunked.NewChunkedArray[T](s.name, s.DataType())
	ca.AppendSlice(values, validity)
	return &TypedSeries[T]{
		chunkedArray: ca,
		name:         s.name,
	}
}

func (s *TypedSeries[T]) Get(i int) interface{} {
	val, ok := s.chunkedArray.Get(int64(i))
	if !ok {
		return nil
	}
	return val
}

func (s *TypedSeries[T]) GetAsString(i int) string {
	if i < 0 || i >= s.Len() || s.IsNull(i) {
		return "null"
	}
	return fmt.Sprintf("%v", s.Get(i))
}

func (s *TypedSeries[T]) ToSlice() interface{} {
	values, _ := s.chunkedArray.ToSlice()
	return values
}

func (s *TypedSeries[T]) ValuesWithValidity() ([]T, []bool) {
	return s.chunkedArray.ToSlice()
}

func (s *TypedSeries[T]) String() string {
	dt := s.DataType()
	length := s.Len()
	maxShow := 10
	result := fmt.Sprintf("Series: %s [%s]\n", s.name, dt.String())

	show := length
	if show > maxShow {
		show = maxShow
	}
	for i := 0; i < show; i++ {
		if s.IsNull(i) {
			result += "null\n"
		} else {
			result += fmt.Sprintf("%v\n", s.Get(i))
		}
	}
	if length > maxShow {
		result += fmt.Sprintf("... %d more values\n", length-maxShow)
	}
	return result
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
		ca.AppendArray(arr)
		return &TypedSeries[bool]{chunkedArray: ca, name: name}, nil
	case datatypes.Int8:
		ca := chunked.NewChunkedArray[int8](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[int8]{chunkedArray: ca, name: name}, nil
	case datatypes.Int16:
		ca := chunked.NewChunkedArray[int16](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[int16]{chunkedArray: ca, name: name}, nil
	case datatypes.Int32:
		ca := chunked.NewChunkedArray[int32](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[int32]{chunkedArray: ca, name: name}, nil
	case datatypes.Int64:
		ca := chunked.NewChunkedArray[int64](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[int64]{chunkedArray: ca, name: name}, nil
	case datatypes.UInt8:
		ca := chunked.NewChunkedArray[uint8](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[uint8]{chunkedArray: ca, name: name}, nil
	case datatypes.UInt16:
		ca := chunked.NewChunkedArray[uint16](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[uint16]{chunkedArray: ca, name: name}, nil
	case datatypes.UInt32:
		ca := chunked.NewChunkedArray[uint32](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[uint32]{chunkedArray: ca, name: name}, nil
	case datatypes.UInt64:
		ca := chunked.NewChunkedArray[uint64](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[uint64]{chunkedArray: ca, name: name}, nil
	case datatypes.Float32:
		ca := chunked.NewChunkedArray[float32](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[float32]{chunkedArray: ca, name: name}, nil
	case datatypes.Float64:
		ca := chunked.NewChunkedArray[float64](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[float64]{chunkedArray: ca, name: name}, nil
	case datatypes.String:
		ca := chunked.NewChunkedArray[string](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[string]{chunkedArray: ca, name: name}, nil
	case datatypes.Binary:
		ca := chunked.NewChunkedArray[[]byte](name, dt)
		ca.AppendArray(arr)
		return &TypedSeries[[]byte]{chunkedArray: ca, name: name}, nil
	default:
		return nil, fmt.Errorf("unsupported arrow type: %v", arr.DataType())
	}
}

// SeriesFromArrowChunked creates a Series from an Arrow chunked array.
// The caller should release the chunked array after this returns.
func SeriesFromArrowChunked(name string, chunkedArr *arrow.Chunked) (Series, error) {
	if chunkedArr.Len() == 0 {
		dt := datatypes.FromArrowType(chunkedArr.DataType())
		switch dt.(type) {
		case datatypes.Int64:
			ca := chunked.NewChunkedArray[int64](name, dt)
			return &TypedSeries[int64]{chunkedArray: ca, name: name}, nil
		case datatypes.Float64:
			ca := chunked.NewChunkedArray[float64](name, dt)
			return &TypedSeries[float64]{chunkedArray: ca, name: name}, nil
		case datatypes.String:
			ca := chunked.NewChunkedArray[string](name, dt)
			return &TypedSeries[string]{chunkedArray: ca, name: name}, nil
		default:
			ca := chunked.NewChunkedArray[int64](name, dt)
			return &TypedSeries[int64]{chunkedArray: ca, name: name}, nil
		}
	}
	// Use the first chunk to create the series, then append remaining
	chunks := chunkedArr.Chunks()
	s, err := SeriesFromArrowArray(name, chunks[0])
	if err != nil {
		return nil, err
	}
	// Append remaining chunks
	for i := 1; i < len(chunks); i++ {
		// We need to create a new series from each additional chunk
		// For simplicity, get the typed series and append
		switch ts := s.(type) {
		case *TypedSeries[bool]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[int8]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[int16]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[int32]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[int64]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[uint8]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[uint16]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[uint32]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[uint64]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[float32]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[float64]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[string]:
			ts.chunkedArray.AppendArray(chunks[i])
		case *TypedSeries[[]byte]:
			ts.chunkedArray.AppendArray(chunks[i])
		}
	}
	return s, nil
}

// ArrowChunked exposes the underlying Arrow chunks for a Series.
// The caller must Release the returned chunked array.
func ArrowChunked(s Series) (*arrow.Chunked, bool) {
	switch ts := s.(type) {
	case *TypedSeries[bool]:
		return typedArrowChunked(ts), true
	case *TypedSeries[int8]:
		return typedArrowChunked(ts), true
	case *TypedSeries[int16]:
		return typedArrowChunked(ts), true
	case *TypedSeries[int32]:
		return typedArrowChunked(ts), true
	case *TypedSeries[int64]:
		return typedArrowChunked(ts), true
	case *TypedSeries[uint8]:
		return typedArrowChunked(ts), true
	case *TypedSeries[uint16]:
		return typedArrowChunked(ts), true
	case *TypedSeries[uint32]:
		return typedArrowChunked(ts), true
	case *TypedSeries[uint64]:
		return typedArrowChunked(ts), true
	case *TypedSeries[float32]:
		return typedArrowChunked(ts), true
	case *TypedSeries[float64]:
		return typedArrowChunked(ts), true
	case *TypedSeries[string]:
		return typedArrowChunked(ts), true
	case *TypedSeries[[]byte]:
		return typedArrowChunked(ts), true
	default:
		return nil, false
	}
}

func typedArrowChunked[T datatypes.ArrayValue](s *TypedSeries[T]) *arrow.Chunked {
	chunks := s.chunkedArray.Chunks()
	if len(chunks) == 0 {
		// Create an empty array to build a valid chunked
		pt := datatypes.GetPolarsType(s.DataType())
		builder := pt.NewBuilder(nil)
		arr := builder.NewArray()
		defer arr.Release()
		chk := arrow.NewChunked(pt.ArrowType(), []arrow.Array{arr})
		return chk
	}
	pt := datatypes.GetPolarsType(s.DataType())
	chk := arrow.NewChunked(pt.ArrowType(), chunks)
	return chk
}

// InterfaceSeries holds arbitrary interface{} values (e.g., slices from TopK)
// This is used for list-like columns that can be exploded
type InterfaceSeries struct {
	name     string
	data     []interface{}
	validity []bool
	dtype    datatypes.DataType
}

// NewInterfaceSeries creates a series that holds interface{} values
func NewInterfaceSeries(name string, data []interface{}, validity []bool, dtype datatypes.DataType) Series {
	v := validity
	if v == nil {
		v = make([]bool, len(data))
		for i := range v {
			v[i] = true
		}
	}
	return &InterfaceSeries{
		name:     name,
		data:     data,
		validity: v,
		dtype:    dtype,
	}
}

func (s *InterfaceSeries) Name() string { return s.name }
func (s *InterfaceSeries) Rename(name string) Series {
	cloned := s.Clone().(*InterfaceSeries)
	cloned.name = name
	return cloned
}
func (s *InterfaceSeries) DataType() datatypes.DataType { return s.dtype }
func (s *InterfaceSeries) Len() int                     { return len(s.data) }
func (s *InterfaceSeries) IsNull(i int) bool {
	if i < 0 || i >= len(s.data) {
		return true
	}
	return !s.validity[i]
}
func (s *InterfaceSeries) IsValid(i int) bool {
	if i < 0 || i >= len(s.data) {
		return false
	}
	return s.validity[i]
}
func (s *InterfaceSeries) NullCount() int {
	count := 0
	for _, v := range s.validity {
		if !v {
			count++
		}
	}
	return count
}
func (s *InterfaceSeries) Slice(start, end int) (Series, error) {
	if start < 0 || end > len(s.data) || start > end {
		return nil, fmt.Errorf("invalid slice bounds [%d:%d] for length %d", start, end, len(s.data))
	}
	newData := make([]interface{}, end-start)
	copy(newData, s.data[start:end])
	newValidity := make([]bool, end-start)
	copy(newValidity, s.validity[start:end])
	return &InterfaceSeries{name: s.name, data: newData, validity: newValidity, dtype: s.dtype}, nil
}
func (s *InterfaceSeries) Head(n int) Series {
	if n <= 0 {
		return &InterfaceSeries{name: s.name, data: nil, validity: nil, dtype: s.dtype}
	}
	if n > len(s.data) {
		n = len(s.data)
	}
	result, _ := s.Slice(0, n)
	return result
}
func (s *InterfaceSeries) Tail(n int) Series {
	if n <= 0 {
		return &InterfaceSeries{name: s.name, data: nil, validity: nil, dtype: s.dtype}
	}
	length := len(s.data)
	if n > length {
		n = length
	}
	result, _ := s.Slice(length-n, length)
	return result
}
func (s *InterfaceSeries) Cast(dt datatypes.DataType) (Series, error) {
	return nil, fmt.Errorf("cast not supported for InterfaceSeries")
}
func (s *InterfaceSeries) Equals(other Series) bool {
	if other == nil {
		return false
	}
	if s.Len() != other.Len() {
		return false
	}
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) != other.IsNull(i) {
			return false
		}
		if s.IsNull(i) {
			continue
		}
		if !reflect.DeepEqual(s.Get(i), other.Get(i)) {
			return false
		}
	}
	return true
}
func (s *InterfaceSeries) Clone() Series {
	newData := make([]interface{}, len(s.data))
	copy(newData, s.data)
	newValidity := make([]bool, len(s.validity))
	copy(newValidity, s.validity)
	return &InterfaceSeries{name: s.name, data: newData, validity: newValidity, dtype: s.dtype}
}
func (s *InterfaceSeries) Get(i int) interface{} {
	if i < 0 || i >= len(s.data) || !s.validity[i] {
		return nil
	}
	return s.data[i]
}
func (s *InterfaceSeries) GetAsString(i int) string {
	if i < 0 || i >= len(s.data) || !s.validity[i] {
		return "null"
	}
	return fmt.Sprintf("%v", s.data[i])
}
func (s *InterfaceSeries) ToSlice() interface{} {
	return s.data
}
func (s *InterfaceSeries) String() string {
	result := fmt.Sprintf("Series: %s [%s]\n", s.name, s.dtype.String())
	maxShow := 10
	show := len(s.data)
	if show > maxShow {
		show = maxShow
	}
	for i := 0; i < show; i++ {
		if !s.validity[i] {
			result += "null\n"
		} else {
			result += fmt.Sprintf("%v\n", s.data[i])
		}
	}
	if len(s.data) > maxShow {
		result += fmt.Sprintf("... %d more values\n", len(s.data)-maxShow)
	}
	return result
}
func (s *InterfaceSeries) Sort(ascending bool) Series { return s.Clone() }
func (s *InterfaceSeries) ArgSort(config SortConfig) []int {
	indices := make([]int, len(s.data))
	for i := range indices {
		indices[i] = i
	}
	return indices
}
func (s *InterfaceSeries) Take(indices []int) Series {
	newData := make([]interface{}, 0, len(indices))
	newValidity := make([]bool, 0, len(indices))
	for _, idx := range indices {
		if idx < 0 || idx >= len(s.data) {
			continue
		}
		newData = append(newData, s.data[idx])
		newValidity = append(newValidity, s.validity[idx])
	}
	return &InterfaceSeries{name: s.name, data: newData, validity: newValidity, dtype: s.dtype}
}
func (s *InterfaceSeries) Sum() float64     { return 0 }
func (s *InterfaceSeries) Mean() float64    { return 0 }
func (s *InterfaceSeries) Min() interface{} { return nil }
func (s *InterfaceSeries) Max() interface{} { return nil }
func (s *InterfaceSeries) Count() int       { return len(s.data) - s.NullCount() }
func (s *InterfaceSeries) Std() float64     { return 0 }
func (s *InterfaceSeries) Var() float64     { return 0 }
func (s *InterfaceSeries) Median() float64  { return 0 }

// arrowMemAllocator returns the default Go memory allocator for arrow arrays
func arrowArrayFromTypedSeries[T datatypes.ArrayValue](ts *TypedSeries[T]) []arrow.Array {
	return ts.chunkedArray.Chunks()
}

// extractArrowArraysFromSeries extracts arrow arrays from series based on concrete type
func extractArrowArrayFromSeries(s Series) ([]arrow.Array, arrow.DataType, bool) {
	switch ts := s.(type) {
	case *TypedSeries[bool]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[int8]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[int16]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[int32]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[int64]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[uint8]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[uint16]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[uint32]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[uint64]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[float32]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[float64]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[string]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	case *TypedSeries[[]byte]:
		return ts.chunkedArray.Chunks(), datatypes.GetPolarsType(ts.DataType()).ArrowType(), true
	default:
		return nil, nil, false
	}
}

// ensure unused imports are used
var _ = array.NewBooleanBuilder
