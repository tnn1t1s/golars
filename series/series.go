package series

import (
	_ "fmt"
	_ "reflect"

	"github.com/apache/arrow-go/v18/arrow"
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
	panic("not implemented")

}

// NewSeriesWithValidity creates a new series with explicit null values
func NewSeriesWithValidity[T datatypes.ArrayValue](name string, values []T, validity []bool, dt datatypes.DataType) Series {
	panic("not implemented")

}

// NewSeriesFromChunkedArray creates a series from an existing ChunkedArray
func NewSeriesFromChunkedArray[T datatypes.ArrayValue](ca *chunked.ChunkedArray[T]) Series {
	panic("not implemented")

}

// Implementation of Series interface for TypedSeries

func (s *TypedSeries[T]) Name() string {
	panic("not implemented")

}

func (s *TypedSeries[T]) Rename(name string) Series {
	panic("not implemented")

}

func (s *TypedSeries[T]) DataType() datatypes.DataType {
	panic("not implemented")

}

func (s *TypedSeries[T]) Len() int {
	panic("not implemented")

}

func (s *TypedSeries[T]) IsNull(i int) bool {
	panic("not implemented")

}

func (s *TypedSeries[T]) IsValid(i int) bool {
	panic("not implemented")

}

func (s *TypedSeries[T]) NullCount() int {
	panic("not implemented")

}

func (s *TypedSeries[T]) Slice(start, end int) (Series, error) {
	panic("not implemented")

}

func (s *TypedSeries[T]) Head(n int) Series {
	panic("not implemented")

}

func (s *TypedSeries[T]) Tail(n int) Series {
	panic("not implemented")

}

func (s *TypedSeries[T]) Cast(dt datatypes.DataType) (Series, error) {
	panic("not implemented")

}

func (s *TypedSeries[T]) Equals(other Series) bool {
	panic("not implemented")

	// Compare values

}

func (s *TypedSeries[T]) Clone() Series {
	panic("not implemented")

}

func (s *TypedSeries[T]) Get(i int) interface{} {
	panic("not implemented")

}

func (s *TypedSeries[T]) GetAsString(i int) string {
	panic("not implemented")

}

func (s *TypedSeries[T]) ToSlice() interface{} {
	panic("not implemented")

}

func (s *TypedSeries[T]) ValuesWithValidity() ([]T, []bool) {
	panic("not implemented")

}

func (s *TypedSeries[T]) String() string {
	panic("not implemented")

}

// Helper functions to create series of specific types

func NewBooleanSeries(name string, values []bool) Series {
	panic("not implemented")

}

func NewInt8Series(name string, values []int8) Series {
	panic("not implemented")

}

func NewInt16Series(name string, values []int16) Series {
	panic("not implemented")

}

func NewInt32Series(name string, values []int32) Series {
	panic("not implemented")

}

func NewInt64Series(name string, values []int64) Series {
	panic("not implemented")

}

func NewUInt8Series(name string, values []uint8) Series {
	panic("not implemented")

}

func NewUInt16Series(name string, values []uint16) Series {
	panic("not implemented")

}

func NewUInt32Series(name string, values []uint32) Series {
	panic("not implemented")

}

func NewUInt64Series(name string, values []uint64) Series {
	panic("not implemented")

}

func NewFloat32Series(name string, values []float32) Series {
	panic("not implemented")

}

func NewFloat64Series(name string, values []float64) Series {
	panic("not implemented")

}

func NewStringSeries(name string, values []string) Series {
	panic("not implemented")

}

func NewBinarySeries(name string, values [][]byte) Series {
	panic("not implemented")

}

// SeriesFromArrowArray creates a Series from an Arrow array
func SeriesFromArrowArray(name string, arr arrow.Array) (Series, error) {
	panic("not implemented")

}

// SeriesFromArrowChunked creates a Series from an Arrow chunked array.
// The caller should release the chunked array after this returns.
func SeriesFromArrowChunked(name string, chunkedArr *arrow.Chunked) (Series, error) {
	panic("not implemented")

}

// ArrowChunked exposes the underlying Arrow chunks for a Series.
// The caller must Release the returned chunked array.
func ArrowChunked(s Series) (*arrow.Chunked, bool) {
	panic("not implemented")

}

func typedArrowChunked[T datatypes.ArrayValue](s *TypedSeries[T]) *arrow.Chunked {
	panic("not implemented")

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
	panic("not implemented")

}

func (s *InterfaceSeries) Name() string { panic("not implemented") }
func (s *InterfaceSeries) Rename(name string) Series {
	panic("not implemented")

}
func (s *InterfaceSeries) DataType() datatypes.DataType { panic("not implemented") }
func (s *InterfaceSeries) Len() int                     { panic("not implemented") }
func (s *InterfaceSeries) IsNull(i int) bool            { panic("not implemented") }
func (s *InterfaceSeries) IsValid(i int) bool           { panic("not implemented") }
func (s *InterfaceSeries) NullCount() int {
	panic("not implemented")

}
func (s *InterfaceSeries) Slice(start, end int) (Series, error) {
	panic("not implemented")

}
func (s *InterfaceSeries) Head(n int) Series {
	panic("not implemented")

}
func (s *InterfaceSeries) Tail(n int) Series {
	panic("not implemented")

}
func (s *InterfaceSeries) Cast(dt datatypes.DataType) (Series, error) {
	panic("not implemented")

}
func (s *InterfaceSeries) Equals(other Series) bool { panic("not implemented") }
func (s *InterfaceSeries) Clone() Series {
	panic("not implemented")

}
func (s *InterfaceSeries) Get(i int) interface{}    { panic("not implemented") }
func (s *InterfaceSeries) GetAsString(i int) string { panic("not implemented") }
func (s *InterfaceSeries) ToSlice() interface{}     { panic("not implemented") }
func (s *InterfaceSeries) String() string {
	panic("not implemented")

}
func (s *InterfaceSeries) Sort(ascending bool) Series { panic("not implemented") }
func (s *InterfaceSeries) ArgSort(config SortConfig) []int {
	panic("not implemented")

}
func (s *InterfaceSeries) Take(indices []int) Series {
	panic("not implemented")

}
func (s *InterfaceSeries) Sum() float64     { panic("not implemented") }
func (s *InterfaceSeries) Mean() float64    { panic("not implemented") }
func (s *InterfaceSeries) Min() interface{} { panic("not implemented") }
func (s *InterfaceSeries) Max() interface{} { panic("not implemented") }
func (s *InterfaceSeries) Count() int       { panic("not implemented") }
func (s *InterfaceSeries) Std() float64     { panic("not implemented") }
func (s *InterfaceSeries) Var() float64     { panic("not implemented") }
func (s *InterfaceSeries) Median() float64  { panic("not implemented") }
