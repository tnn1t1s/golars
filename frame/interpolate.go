package frame

import (
	"fmt"
	"math"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// InterpolateOptions configures interpolation
type InterpolateOptions struct {
	Method    string
	Columns   []string
	Limit     int
	LimitArea string
}

// Interpolate fills null values using interpolation
func (df *DataFrame) Interpolate(options InterpolateOptions) (*DataFrame, error) {
	if options.Method == "" {
		options.Method = "linear"
	}

	columns := options.Columns
	if len(columns) == 0 {
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				columns = append(columns, col.Name())
			}
		}
	}

	colSet := make(map[string]bool, len(columns))
	for _, name := range columns {
		colSet[name] = true
	}

	newCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		if !colSet[col.Name()] {
			newCols[i] = col
			continue
		}
		switch options.Method {
		case "linear":
			newCols[i] = linearInterpolate(col, options.Limit, options.LimitArea)
		case "nearest":
			newCols[i] = nearestInterpolate(col, options.Limit, options.LimitArea)
		case "zero":
			newCols[i] = zeroInterpolate(col, options.Limit, options.LimitArea)
		case "forward":
			newCols[i] = forwardFillSeries(col, options.Limit)
		case "backward":
			newCols[i] = backwardFillSeries(col, options.Limit)
		default:
			return nil, fmt.Errorf("invalid interpolation method: %s", options.Method)
		}
	}
	return NewDataFrame(newCols...)
}

func toFloat64Value(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case bool:
		if val {
			return 1.0
		}
		return 0.0
	default:
		return math.NaN()
	}
}

func convertToOriginalType(val float64, dataType datatypes.DataType) interface{} {
	switch dataType.(type) {
	case datatypes.Int64:
		return int64(val)
	case datatypes.Int32:
		return int32(val)
	case datatypes.Float64:
		return val
	case datatypes.Float32:
		return float32(val)
	default:
		return val
	}
}

func linearInterpolate(s series.Series, limit int, limitArea string) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !s.IsNull(i) {
			values[i] = toFloat64Value(s.Get(i))
			validity[i] = true
		}
	}

	// Identify null gaps and their sizes for limit enforcement
	type nullGap struct {
		start, end int // inclusive range of null indices
	}
	var gaps []nullGap
	i := 0
	for i < n {
		if s.IsNull(i) {
			gapStart := i
			for i < n && s.IsNull(i) {
				i++
			}
			gaps = append(gaps, nullGap{gapStart, i - 1})
		} else {
			i++
		}
	}

	for _, gap := range gaps {
		gapSize := gap.end - gap.start + 1

		// If limit is set and gap exceeds limit, skip the entire gap
		if limit > 0 && gapSize > limit {
			continue
		}

		for j := gap.start; j <= gap.end; j++ {
			prevIdx := -1
			nextIdx := -1
			for k := j - 1; k >= 0; k-- {
				if !s.IsNull(k) {
					prevIdx = k
					break
				}
			}
			for k := j + 1; k < n; k++ {
				if !s.IsNull(k) {
					nextIdx = k
					break
				}
			}

			if limitArea == "inside" && (prevIdx < 0 || nextIdx < 0) {
				continue
			}
			if limitArea == "outside" && prevIdx >= 0 && nextIdx >= 0 {
				continue
			}

			if prevIdx >= 0 && nextIdx >= 0 {
				pv := toFloat64Value(s.Get(prevIdx))
				nv := toFloat64Value(s.Get(nextIdx))
				frac := float64(j-prevIdx) / float64(nextIdx-prevIdx)
				values[j] = pv + frac*(nv-pv)
				validity[j] = true
			}
		}
	}

	return convertInterpolatedSeries(s.Name(), values, validity, s.DataType())
}

// convertInterpolatedSeries converts float64 results back to the original series type
func convertInterpolatedSeries(name string, values []float64, validity []bool, dtype datatypes.DataType) series.Series {
	switch dtype.(type) {
	case datatypes.Int32:
		typed := make([]int32, len(values))
		for i, v := range values {
			typed[i] = int32(math.Round(v))
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Int64:
		typed := make([]int64, len(values))
		for i, v := range values {
			typed[i] = int64(math.Round(v))
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	default:
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Float64{})
	}
}

func nearestInterpolate(s series.Series, limit int, limitArea string) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !s.IsNull(i) {
			values[i] = toFloat64Value(s.Get(i))
			validity[i] = true
		}
	}

	for i := 0; i < n; i++ {
		if validity[i] {
			continue
		}
		prevIdx := -1
		nextIdx := -1
		for j := i - 1; j >= 0; j-- {
			if !s.IsNull(j) {
				prevIdx = j
				break
			}
		}
		for j := i + 1; j < n; j++ {
			if !s.IsNull(j) {
				nextIdx = j
				break
			}
		}

		if limitArea == "inside" && (prevIdx < 0 || nextIdx < 0) {
			continue
		}

		bestIdx := -1
		if prevIdx >= 0 && nextIdx >= 0 {
			if i-prevIdx <= nextIdx-i {
				bestIdx = prevIdx
			} else {
				bestIdx = nextIdx
			}
		} else if prevIdx >= 0 {
			bestIdx = prevIdx
		} else if nextIdx >= 0 {
			bestIdx = nextIdx
		}

		if bestIdx >= 0 {
			values[i] = toFloat64Value(s.Get(bestIdx))
			validity[i] = true
		}
	}

	return series.NewSeriesWithValidity(s.Name(), values, validity, datatypes.Float64{})
}

func zeroInterpolate(s series.Series, limit int, limitArea string) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)

	// Zero-order hold: forward fill from last known value
	lastVal := 0.0
	hasLast := false
	for i := 0; i < n; i++ {
		if !s.IsNull(i) {
			lastVal = toFloat64Value(s.Get(i))
			hasLast = true
			values[i] = lastVal
			validity[i] = true
		} else if hasLast {
			values[i] = lastVal
			validity[i] = true
		}
		// If no previous value, leave as invalid
	}

	return series.NewSeriesWithValidity(s.Name(), values, validity, datatypes.Float64{})
}
