package series

import (
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	_ "github.com/tnn1t1s/golars/internal/parallel"
	_ "os"
	_ "strings"
	"sync"
)

// Int64ValuesWithValidity returns typed values and validity for int64 series.
func Int64ValuesWithValidity(s Series) ([]int64, []bool, bool) {
	panic("not implemented")

}

// Int16ValuesWithValidity returns typed values and validity for int16 series.
func Int16ValuesWithValidity(s Series) ([]int16, []bool, bool) {
	panic("not implemented")

}

// Int8ValuesWithValidity returns typed values and validity for int8 series.
func Int8ValuesWithValidity(s Series) ([]int8, []bool, bool) {
	panic("not implemented")

}

// Int32ValuesWithValidity returns typed values and validity for int32 series.
func Int32ValuesWithValidity(s Series) ([]int32, []bool, bool) {
	panic("not implemented")

}

// Uint64ValuesWithValidity returns typed values and validity for uint64 series.
func Uint64ValuesWithValidity(s Series) ([]uint64, []bool, bool) {
	panic("not implemented")

}

// Uint16ValuesWithValidity returns typed values and validity for uint16 series.
func Uint16ValuesWithValidity(s Series) ([]uint16, []bool, bool) {
	panic("not implemented")

}

// Uint8ValuesWithValidity returns typed values and validity for uint8 series.
func Uint8ValuesWithValidity(s Series) ([]uint8, []bool, bool) {
	panic("not implemented")

}

// Uint32ValuesWithValidity returns typed values and validity for uint32 series.
func Uint32ValuesWithValidity(s Series) ([]uint32, []bool, bool) {
	panic("not implemented")

}

// Float64ValuesWithValidity returns typed values and validity for float64 series.
func Float64ValuesWithValidity(s Series) ([]float64, []bool, bool) {
	panic("not implemented")

}

// Float32ValuesWithValidity returns typed values and validity for float32 series.
func Float32ValuesWithValidity(s Series) ([]float32, []bool, bool) {
	panic("not implemented")

}

// BoolValuesWithValidity returns typed values and validity for bool series.
func BoolValuesWithValidity(s Series) ([]bool, []bool, bool) {
	panic("not implemented")

}

// StringValuesWithValidity returns typed values and validity for string series.
func StringValuesWithValidity(s Series) ([]string, []bool, bool) {
	panic("not implemented")

}

// =============================================================================
// Fast Take operations - avoid per-element method calls
// =============================================================================

// TakeInt64Fast gathers values at indices using direct slice access.
// Returns new series. indices with -1 are treated as null.
func TakeInt64Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

	// idx < 0 means null, leave as zero/false

}

// TakeInt16Fast gathers values at indices using direct slice access.
func TakeInt16Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeInt8Fast gathers values at indices using direct slice access.
func TakeInt8Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeInt32Fast gathers values at indices using direct slice access.
func TakeInt32Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeUint16Fast gathers values at indices using direct slice access.
func TakeUint16Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeUint8Fast gathers values at indices using direct slice access.
func TakeUint8Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeUint64Fast gathers values at indices using direct slice access.
func TakeUint64Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeUint32Fast gathers values at indices using direct slice access.
func TakeUint32Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeFloat32Fast gathers values at indices using direct slice access.
func TakeFloat32Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeFloat64Fast gathers values at indices using direct slice access.
func TakeFloat64Fast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeBoolFast gathers values at indices using direct slice access.
func TakeBoolFast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeStringFast gathers values at indices using direct slice access.
func TakeStringFast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

// TakeFast attempts fast take for any supported type.
func TakeFast(s Series, indices []int) (Series, bool) {
	panic("not implemented")

}

func shouldParallelTake(n int) bool {
	panic("not implemented")

}

const (
	takeParallelMin = 200_000
	takeMinChunk    = 50_000
)

var (
	takeParallelOnce  sync.Once
	takeParallelAllow = true
)

func takeParallelEnabled() bool {
	panic("not implemented")

}

func takeParallelParts(n int) int {
	panic("not implemented")

}

func takeRange(n, parts, p int) (int, int) {
	panic("not implemented")

}

func takeParallelFor(n int, fn func(start, end int)) {
	panic("not implemented")

}

// =============================================================================
// Constructors with validity
// =============================================================================

// NewInt64SeriesWithValidity creates an int64 series with explicit validity.
func NewInt64SeriesWithValidity(name string, values []int64, validity []bool) Series {
	panic("not implemented")

}

// NewInt16SeriesWithValidity creates an int16 series with explicit validity.
func NewInt16SeriesWithValidity(name string, values []int16, validity []bool) Series {
	panic("not implemented")

}

// NewInt8SeriesWithValidity creates an int8 series with explicit validity.
func NewInt8SeriesWithValidity(name string, values []int8, validity []bool) Series {
	panic("not implemented")

}

// NewInt32SeriesWithValidity creates an int32 series with explicit validity.
func NewInt32SeriesWithValidity(name string, values []int32, validity []bool) Series {
	panic("not implemented")

}

// NewUint64SeriesWithValidity creates a uint64 series with explicit validity.
func NewUint64SeriesWithValidity(name string, values []uint64, validity []bool) Series {
	panic("not implemented")

}

// NewUint16SeriesWithValidity creates a uint16 series with explicit validity.
func NewUint16SeriesWithValidity(name string, values []uint16, validity []bool) Series {
	panic("not implemented")

}

// NewUint8SeriesWithValidity creates a uint8 series with explicit validity.
func NewUint8SeriesWithValidity(name string, values []uint8, validity []bool) Series {
	panic("not implemented")

}

// NewUint32SeriesWithValidity creates a uint32 series with explicit validity.
func NewUint32SeriesWithValidity(name string, values []uint32, validity []bool) Series {
	panic("not implemented")

}

// NewFloat32SeriesWithValidity creates a float32 series with explicit validity.
func NewFloat32SeriesWithValidity(name string, values []float32, validity []bool) Series {
	panic("not implemented")

}

// NewFloat64SeriesWithValidity creates a float64 series with explicit validity.
func NewFloat64SeriesWithValidity(name string, values []float64, validity []bool) Series {
	panic("not implemented")

}

// NewStringSeriesWithValidity creates a string series with explicit validity.
func NewStringSeriesWithValidity(name string, values []string, validity []bool) Series {
	panic("not implemented")

}

// NewBooleanSeriesWithValidity creates a bool series with explicit validity.
func NewBooleanSeriesWithValidity(name string, values []bool, validity []bool) Series {
	panic("not implemented")

}
