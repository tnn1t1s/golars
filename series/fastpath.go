package series

import "github.com/tnn1t1s/golars/internal/datatypes"

// Int64ValuesWithValidity returns typed values and validity for int64 series.
func Int64ValuesWithValidity(s Series) ([]int64, []bool, bool) {
	ts, ok := s.(*TypedSeries[int64])
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

	for i, idx := range indices {
		if idx >= 0 {
			dstValues[i] = srcValues[idx]
			dstValidity[i] = srcValidity[idx]
		}
		// idx < 0 means null, leave as zero/false
	}

	return NewInt64SeriesWithValidity(s.Name(), dstValues, dstValidity), true
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

	for i, idx := range indices {
		if idx >= 0 {
			dstValues[i] = srcValues[idx]
			dstValidity[i] = srcValidity[idx]
		}
	}

	return NewInt32SeriesWithValidity(s.Name(), dstValues, dstValidity), true
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

	for i, idx := range indices {
		if idx >= 0 {
			dstValues[i] = srcValues[idx]
			dstValidity[i] = srcValidity[idx]
		}
	}

	return NewFloat64SeriesWithValidity(s.Name(), dstValues, dstValidity), true
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

	for i, idx := range indices {
		if idx >= 0 {
			dstValues[i] = srcValues[idx]
			dstValidity[i] = srcValidity[idx]
		}
	}

	return NewStringSeriesWithValidity(s.Name(), dstValues, dstValidity), true
}

// TakeFast attempts fast take for any supported type.
func TakeFast(s Series, indices []int) (Series, bool) {
	switch s.DataType().(type) {
	case datatypes.Int64:
		return TakeInt64Fast(s, indices)
	case datatypes.Int32:
		return TakeInt32Fast(s, indices)
	case datatypes.Float64:
		return TakeFloat64Fast(s, indices)
	case datatypes.String:
		return TakeStringFast(s, indices)
	}
	return nil, false
}

// =============================================================================
// Constructors with validity
// =============================================================================

// NewInt64SeriesWithValidity creates an int64 series with explicit validity.
func NewInt64SeriesWithValidity(name string, values []int64, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Int64{})
}

// NewInt32SeriesWithValidity creates an int32 series with explicit validity.
func NewInt32SeriesWithValidity(name string, values []int32, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Int32{})
}

// NewFloat64SeriesWithValidity creates a float64 series with explicit validity.
func NewFloat64SeriesWithValidity(name string, values []float64, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.Float64{})
}

// NewStringSeriesWithValidity creates a string series with explicit validity.
func NewStringSeriesWithValidity(name string, values []string, validity []bool) Series {
	return NewSeriesWithValidity(name, values, validity, datatypes.String{})
}
