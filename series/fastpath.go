package series

import (
	"os"
	"strings"
	"sync"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/parallel"
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

// TakeInt64Fast gathers values at indices using direct slice access.
// Returns new series. indices with -1 are treated as null.
func TakeInt64Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Int64ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]int64, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
			// idx < 0 means null, leave as zero/false
		}
	}

	return NewInt64SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeInt16Fast gathers values at indices using direct slice access.
func TakeInt16Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Int16ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]int16, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewInt16SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeInt8Fast gathers values at indices using direct slice access.
func TakeInt8Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Int8ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]int8, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewInt8SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeInt32Fast gathers values at indices using direct slice access.
func TakeInt32Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Int32ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]int32, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewInt32SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeUint16Fast gathers values at indices using direct slice access.
func TakeUint16Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Uint16ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]uint16, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewUint16SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeUint8Fast gathers values at indices using direct slice access.
func TakeUint8Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Uint8ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]uint8, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewUint8SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeUint64Fast gathers values at indices using direct slice access.
func TakeUint64Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Uint64ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]uint64, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewUint64SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeUint32Fast gathers values at indices using direct slice access.
func TakeUint32Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Uint32ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]uint32, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewUint32SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeFloat32Fast gathers values at indices using direct slice access.
func TakeFloat32Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Float32ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]float32, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewFloat32SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeFloat64Fast gathers values at indices using direct slice access.
func TakeFloat64Fast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := Float64ValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]float64, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewFloat64SeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeBoolFast gathers values at indices using direct slice access.
func TakeBoolFast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := BoolValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]bool, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewBooleanSeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeStringFast gathers values at indices using direct slice access.
func TakeStringFast(s Series, indices []int) (Series, bool) {
	srcValues, srcValidity, ok := StringValuesWithValidity(s)
	if !ok {
		return nil, false
	}

	n := len(indices)
	dstValues := make([]string, n)
	dstValidity := make([]bool, n)

	if shouldParallelTake(n) {
		takeParallelFor(n, func(start, end int) {
			for i := start; i < end; i++ {
				idx := indices[i]
				if idx >= 0 {
					dstValues[i] = srcValues[idx]
					dstValidity[i] = srcValidity[idx]
				}
			}
		})
	} else {
		for i, idx := range indices {
			if idx >= 0 {
				dstValues[i] = srcValues[idx]
				dstValidity[i] = srcValidity[idx]
			}
		}
	}

	return NewStringSeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeFast attempts fast take for any supported type.
func TakeFast(s Series, indices []int) (Series, bool) {
	switch s.DataType().(type) {
	case datatypes.Int64:
		return TakeInt64Fast(s, indices)
	case datatypes.Int16:
		return TakeInt16Fast(s, indices)
	case datatypes.Int8:
		return TakeInt8Fast(s, indices)
	case datatypes.Int32:
		return TakeInt32Fast(s, indices)
	case datatypes.UInt64:
		return TakeUint64Fast(s, indices)
	case datatypes.UInt16:
		return TakeUint16Fast(s, indices)
	case datatypes.UInt8:
		return TakeUint8Fast(s, indices)
	case datatypes.UInt32:
		return TakeUint32Fast(s, indices)
	case datatypes.Float32:
		return TakeFloat32Fast(s, indices)
	case datatypes.Float64:
		return TakeFloat64Fast(s, indices)
	case datatypes.String:
		return TakeStringFast(s, indices)
	case datatypes.Boolean:
		return TakeBoolFast(s, indices)
	}
	return nil, false
}

func shouldParallelTake(n int) bool {
	return takeParallelParts(n) > 1
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
		if raw, ok := os.LookupEnv("GOLARS_TAKE_PARALLEL"); ok {
			val := strings.ToLower(strings.TrimSpace(raw))
			switch val {
			case "0", "false", "off", "no":
				takeParallelAllow = false
			default:
				takeParallelAllow = true
			}
		}
	})
	return takeParallelAllow
}

func takeParallelParts(n int) int {
	if !parallel.Enabled() || !takeParallelEnabled() || n < takeParallelMin {
		return 1
	}
	workers := parallel.MaxThreads()
	if workers < 1 {
		workers = 1
	}
	parts := n / takeMinChunk
	if parts < 1 {
		parts = 1
	}
	if parts > workers {
		parts = workers
	}
	if parts > n {
		parts = n
	}
	return parts
}

func takeRange(n, parts, p int) (int, int) {
	start := p * n / parts
	end := (p + 1) * n / parts
	return start, end
}

func takeParallelFor(n int, fn func(start, end int)) {
	parts := takeParallelParts(n)
	if parts <= 1 {
		fn(0, n)
		return
	}

	var wg sync.WaitGroup
	for p := 0; p < parts; p++ {
		start, end := takeRange(n, parts, p)
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
