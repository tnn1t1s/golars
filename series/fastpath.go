package series

import (
	"os"
	"strings"
	"sync"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// Int64ValuesWithValidity returns typed values and validity for int64 series.
func Int64ValuesWithValidity(s Series) ([]int64, []bool, bool) {
	ts, ok := s.(*TypedSeries[int64])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Int16ValuesWithValidity returns typed values and validity for int16 series.
func Int16ValuesWithValidity(s Series) ([]int16, []bool, bool) {
	ts, ok := s.(*TypedSeries[int16])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Int8ValuesWithValidity returns typed values and validity for int8 series.
func Int8ValuesWithValidity(s Series) ([]int8, []bool, bool) {
	ts, ok := s.(*TypedSeries[int8])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Int32ValuesWithValidity returns typed values and validity for int32 series.
func Int32ValuesWithValidity(s Series) ([]int32, []bool, bool) {
	ts, ok := s.(*TypedSeries[int32])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Uint64ValuesWithValidity returns typed values and validity for uint64 series.
func Uint64ValuesWithValidity(s Series) ([]uint64, []bool, bool) {
	ts, ok := s.(*TypedSeries[uint64])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Uint16ValuesWithValidity returns typed values and validity for uint16 series.
func Uint16ValuesWithValidity(s Series) ([]uint16, []bool, bool) {
	ts, ok := s.(*TypedSeries[uint16])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Uint8ValuesWithValidity returns typed values and validity for uint8 series.
func Uint8ValuesWithValidity(s Series) ([]uint8, []bool, bool) {
	ts, ok := s.(*TypedSeries[uint8])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Uint32ValuesWithValidity returns typed values and validity for uint32 series.
func Uint32ValuesWithValidity(s Series) ([]uint32, []bool, bool) {
	ts, ok := s.(*TypedSeries[uint32])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Float64ValuesWithValidity returns typed values and validity for float64 series.
func Float64ValuesWithValidity(s Series) ([]float64, []bool, bool) {
	ts, ok := s.(*TypedSeries[float64])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// Float32ValuesWithValidity returns typed values and validity for float32 series.
func Float32ValuesWithValidity(s Series) ([]float32, []bool, bool) {
	ts, ok := s.(*TypedSeries[float32])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// BoolValuesWithValidity returns typed values and validity for bool series.
func BoolValuesWithValidity(s Series) ([]bool, []bool, bool) {
	ts, ok := s.(*TypedSeries[bool])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// StringValuesWithValidity returns typed values and validity for string series.
func StringValuesWithValidity(s Series) ([]string, []bool, bool) {
	ts, ok := s.(*TypedSeries[string])
	if !ok {
		return nil, nil, false
	}
	values, validity := ts.ValuesWithValidity()
	return values, validity, true
}

// =============================================================================
// Fast Take operations - avoid per-element method calls
// =============================================================================

func takeFastTyped[T datatypes.ArrayValue](s Series, indices []int, dt datatypes.DataType) (Series, bool) {
	ts, ok := s.(*TypedSeries[T])
	if !ok {
		return nil, false
	}
	values, validity := ts.ValuesWithValidity()
	n := len(values)

	newValues := make([]T, len(indices))
	newValidity := make([]bool, len(indices))
	outIdx := 0

	for _, idx := range indices {
		if idx < 0 {
			// null marker
			newValues[outIdx] = newValues[0] // zero value
			newValidity[outIdx] = false
			outIdx++
		} else if idx < n {
			newValues[outIdx] = values[idx]
			newValidity[outIdx] = validity[idx]
			outIdx++
		}
	}

	newValues = newValues[:outIdx]
	newValidity = newValidity[:outIdx]

	return NewSeriesWithValidity(s.Name(), newValues, newValidity, dt), true
}

// TakeInt64Fast gathers values at indices using direct slice access.
func TakeInt64Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[int64](s, indices, datatypes.Int64{})
}

// TakeInt16Fast gathers values at indices using direct slice access.
func TakeInt16Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[int16](s, indices, datatypes.Int16{})
}

// TakeInt8Fast gathers values at indices using direct slice access.
func TakeInt8Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[int8](s, indices, datatypes.Int8{})
}

// TakeInt32Fast gathers values at indices using direct slice access.
func TakeInt32Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[int32](s, indices, datatypes.Int32{})
}

// TakeUint16Fast gathers values at indices using direct slice access.
func TakeUint16Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[uint16](s, indices, datatypes.UInt16{})
}

// TakeUint8Fast gathers values at indices using direct slice access.
func TakeUint8Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[uint8](s, indices, datatypes.UInt8{})
}

// TakeUint64Fast gathers values at indices using direct slice access.
func TakeUint64Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[uint64](s, indices, datatypes.UInt64{})
}

// TakeUint32Fast gathers values at indices using direct slice access.
func TakeUint32Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[uint32](s, indices, datatypes.UInt32{})
}

// TakeFloat32Fast gathers values at indices using direct slice access.
func TakeFloat32Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[float32](s, indices, datatypes.Float32{})
}

// TakeFloat64Fast gathers values at indices using direct slice access.
func TakeFloat64Fast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[float64](s, indices, datatypes.Float64{})
}

// TakeBoolFast gathers values at indices using direct slice access.
func TakeBoolFast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[bool](s, indices, datatypes.Boolean{})
}

// TakeStringFast gathers values at indices using direct slice access.
func TakeStringFast(s Series, indices []int) (Series, bool) {
	return takeFastTyped[string](s, indices, datatypes.String{})
}

// TakeFast attempts fast take for any supported type.
func TakeFast(s Series, indices []int) (Series, bool) {
	switch s.DataType().(type) {
	case datatypes.Int64:
		return TakeInt64Fast(s, indices)
	case datatypes.Int32:
		return TakeInt32Fast(s, indices)
	case datatypes.Int16:
		return TakeInt16Fast(s, indices)
	case datatypes.Int8:
		return TakeInt8Fast(s, indices)
	case datatypes.UInt64:
		return TakeUint64Fast(s, indices)
	case datatypes.UInt32:
		return TakeUint32Fast(s, indices)
	case datatypes.UInt16:
		return TakeUint16Fast(s, indices)
	case datatypes.UInt8:
		return TakeUint8Fast(s, indices)
	case datatypes.Float64:
		return TakeFloat64Fast(s, indices)
	case datatypes.Float32:
		return TakeFloat32Fast(s, indices)
	case datatypes.Boolean:
		return TakeBoolFast(s, indices)
	case datatypes.String:
		return TakeStringFast(s, indices)
	default:
		return nil, false
	}
}

func shouldParallelTake(n int) bool {
	return takeParallelEnabled() && n >= takeParallelMin
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
	takeParallelOnce.Do(func() {
		raw := strings.TrimSpace(strings.ToLower(os.Getenv("GOLARS_NO_PARALLEL")))
		if raw == "1" || raw == "true" || raw == "yes" {
			takeParallelAllow = false
		}
	})
	return takeParallelAllow
}

func takeParallelParts(n int) int {
	parts := n / takeMinChunk
	if parts < 2 {
		return 1
	}
	if parts > 8 {
		return 8
	}
	return parts
}

func takeRange(n, parts, p int) (int, int) {
	chunkSize := (n + parts - 1) / parts
	start := p * chunkSize
	end := start + chunkSize
	if end > n {
		end = n
	}
	return start, end
}

func takeParallelFor(n int, fn func(start, end int)) {
	parts := takeParallelParts(n)
	if parts <= 1 || !shouldParallelTake(n) {
		fn(0, n)
		return
	}
	var wg sync.WaitGroup
	for p := 0; p < parts; p++ {
		start, end := takeRange(n, parts, p)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			fn(s, e)
		}(start, end)
	}
	wg.Wait()
}

// =============================================================================
// Constructors with validity
// =============================================================================

// NewInt64SeriesWithValidity creates an int64 series with explicit validity.
func NewInt64SeriesWithValidity(name string, values []int64, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Int64{})
}

// NewInt16SeriesWithValidity creates an int16 series with explicit validity.
func NewInt16SeriesWithValidity(name string, values []int16, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Int16{})
}

// NewInt8SeriesWithValidity creates an int8 series with explicit validity.
func NewInt8SeriesWithValidity(name string, values []int8, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Int8{})
}

// NewInt32SeriesWithValidity creates an int32 series with explicit validity.
func NewInt32SeriesWithValidity(name string, values []int32, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

// NewUint64SeriesWithValidity creates a uint64 series with explicit validity.
func NewUint64SeriesWithValidity(name string, values []uint64, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.UInt64{})
}

// NewUint16SeriesWithValidity creates a uint16 series with explicit validity.
func NewUint16SeriesWithValidity(name string, values []uint16, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.UInt16{})
}

// NewUint8SeriesWithValidity creates a uint8 series with explicit validity.
func NewUint8SeriesWithValidity(name string, values []uint8, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.UInt8{})
}

// NewUint32SeriesWithValidity creates a uint32 series with explicit validity.
func NewUint32SeriesWithValidity(name string, values []uint32, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.UInt32{})
}

// NewFloat32SeriesWithValidity creates a float32 series with explicit validity.
func NewFloat32SeriesWithValidity(name string, values []float32, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Float32{})
}

// NewFloat64SeriesWithValidity creates a float64 series with explicit validity.
func NewFloat64SeriesWithValidity(name string, values []float64, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Float64{})
}

// NewStringSeriesWithValidity creates a string series with explicit validity.
func NewStringSeriesWithValidity(name string, values []string, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.String{})
}

// NewBooleanSeriesWithValidity creates a bool series with explicit validity.
func NewBooleanSeriesWithValidity(name string, values []bool, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Boolean{})
}
