package frame

import (
	"fmt"
	"math"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// CumulativeOptions configures cumulative operations
type CumulativeOptions struct {
	Axis      int      // 0 for index (rows), 1 for columns
	SkipNulls bool     // Whether to skip null values
	Columns   []string // Specific columns to apply operation to
}

// CumSum calculates cumulative sum for numeric columns
func (df *DataFrame) CumSum(options CumulativeOptions) (*DataFrame, error) {
	return df.applyCumulative(options, cumSumSeries)
}

// CumProd calculates cumulative product for numeric columns
func (df *DataFrame) CumProd(options CumulativeOptions) (*DataFrame, error) {
	return df.applyCumulative(options, cumProdSeries)
}

// CumMax calculates cumulative maximum for numeric columns
func (df *DataFrame) CumMax(options CumulativeOptions) (*DataFrame, error) {
	return df.applyCumulative(options, cumMaxSeries)
}

// CumMin calculates cumulative minimum for numeric columns
func (df *DataFrame) CumMin(options CumulativeOptions) (*DataFrame, error) {
	return df.applyCumulative(options, cumMinSeries)
}

// CumCount calculates cumulative count of non-null values
func (df *DataFrame) CumCount(options CumulativeOptions) (*DataFrame, error) {
	return df.applyCumulative(options, cumCountSeries)
}

// Generic function to apply cumulative operations
func (df *DataFrame) applyCumulative(options CumulativeOptions, fn func(series.Series, bool) series.Series) (*DataFrame, error) {
	colSet := make(map[string]bool)
	if len(options.Columns) > 0 {
		for _, name := range options.Columns {
			if !df.HasColumn(name) {
				return nil, fmt.Errorf("column %q not found", name)
			}
			colSet[name] = true
		}
	}
	allColumns := len(options.Columns) == 0

	newCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		shouldProcess := false
		if allColumns {
			shouldProcess = isNumericType(col.DataType())
		} else {
			shouldProcess = colSet[col.Name()]
		}

		if shouldProcess {
			newCols[i] = fn(col, options.SkipNulls)
		} else {
			newCols[i] = col
		}
	}

	return NewDataFrame(newCols...)
}

// Helper function for cumulative sum
func cumSumSeries(s series.Series, skipNulls bool) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)
	sum := 0.0

	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			if skipNulls {
				values[i] = sum
				validity[i] = true
			}
			// else leave validity[i] = false
			continue
		}
		sum += toFloat64Value(s.Get(i))
		values[i] = sum
		validity[i] = true
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative product
func cumProdSeries(s series.Series, skipNulls bool) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)
	prod := 1.0

	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			if skipNulls {
				values[i] = prod
				validity[i] = true
			}
			continue
		}
		prod *= toFloat64Value(s.Get(i))
		values[i] = prod
		validity[i] = true
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative maximum
func cumMaxSeries(s series.Series, skipNulls bool) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)
	curMax := math.Inf(-1)
	started := false

	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			if skipNulls && started {
				values[i] = curMax
				validity[i] = true
			}
			continue
		}
		v := toFloat64Value(s.Get(i))
		if !started || v > curMax {
			curMax = v
			started = true
		}
		values[i] = curMax
		validity[i] = true
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative minimum
func cumMinSeries(s series.Series, skipNulls bool) series.Series {
	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)
	curMin := math.Inf(1)
	started := false

	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			if skipNulls && started {
				values[i] = curMin
				validity[i] = true
			}
			continue
		}
		v := toFloat64Value(s.Get(i))
		if !started || v < curMin {
			curMin = v
			started = true
		}
		values[i] = curMin
		validity[i] = true
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative count
func cumCountSeries(s series.Series, skipNulls bool) series.Series {
	n := s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)
	count := int64(0)

	for i := 0; i < n; i++ {
		if !s.IsNull(i) {
			count++
		}
		values[i] = count
		validity[i] = true
	}

	return series.NewSeriesWithValidity(s.Name(), values, validity, datatypes.Int64{})
}

// Helper to convert float64 array back to original series type
func convertToOriginalSeriesType(name string, values []float64, validity []bool, dataType datatypes.DataType) series.Series {
	switch dataType.(type) {
	case datatypes.Int32:
		typed := make([]int32, len(values))
		for i, v := range values {
			typed[i] = int32(v)
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	case datatypes.Int64:
		typed := make([]int64, len(values))
		for i, v := range values {
			typed[i] = int64(v)
		}
		return series.NewSeriesWithValidity(name, typed, validity, dataType)
	default:
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Float64{})
	}
}
