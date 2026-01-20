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
	if options.Axis != 0 {
		return nil, fmt.Errorf("only axis=0 is currently supported")
	}

	// Get columns to process
	columnsToProcess := options.Columns
	if len(columnsToProcess) == 0 {
		// Default to all numeric columns for most operations
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				columnsToProcess = append(columnsToProcess, col.Name())
			}
		}
	}

	// Create result columns
	resultColumns := make([]series.Series, len(df.columns))

	for i, col := range df.columns {
		// Check if this column should be processed
		shouldProcess := false
		for _, name := range columnsToProcess {
			if col.Name() == name {
				shouldProcess = true
				break
			}
		}

		if shouldProcess {
			// Apply cumulative function
			resultColumns[i] = fn(col, options.SkipNulls)
		} else {
			// Keep column as is
			resultColumns[i] = col
		}
	}

	return NewDataFrame(resultColumns...)
}

// Helper function for cumulative sum
func cumSumSeries(s series.Series, skipNulls bool) series.Series {
	length := s.Len()
	values := make([]float64, length)
	validity := make([]bool, length)

	sum := 0.0
	hasValidValue := false

	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			if skipNulls {
				// Keep current sum
				if hasValidValue {
					values[i] = sum
					validity[i] = true
				} else {
					validity[i] = false
				}
			} else {
				// Propagate null
				validity[i] = false
			}
		} else {
			val := toFloat64Value(s.Get(i))
			sum += val
			values[i] = sum
			validity[i] = true
			hasValidValue = true
		}
	}

	// Convert back to original type if needed
	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative product
func cumProdSeries(s series.Series, skipNulls bool) series.Series {
	length := s.Len()
	values := make([]float64, length)
	validity := make([]bool, length)

	prod := 1.0
	hasValidValue := false

	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			if skipNulls {
				// Keep current product
				if hasValidValue {
					values[i] = prod
					validity[i] = true
				} else {
					validity[i] = false
				}
			} else {
				// Propagate null
				validity[i] = false
			}
		} else {
			val := toFloat64Value(s.Get(i))
			prod *= val
			values[i] = prod
			validity[i] = true
			hasValidValue = true
		}
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative maximum
func cumMaxSeries(s series.Series, skipNulls bool) series.Series {
	length := s.Len()
	values := make([]float64, length)
	validity := make([]bool, length)

	max := math.Inf(-1)
	hasValidValue := false

	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			if skipNulls {
				// Keep current max
				if hasValidValue {
					values[i] = max
					validity[i] = true
				} else {
					validity[i] = false
				}
			} else {
				// Propagate null
				validity[i] = false
			}
		} else {
			val := toFloat64Value(s.Get(i))
			if val > max || !hasValidValue {
				max = val
			}
			values[i] = max
			validity[i] = true
			hasValidValue = true
		}
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative minimum
func cumMinSeries(s series.Series, skipNulls bool) series.Series {
	length := s.Len()
	values := make([]float64, length)
	validity := make([]bool, length)

	min := math.Inf(1)
	hasValidValue := false

	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			if skipNulls {
				// Keep current min
				if hasValidValue {
					values[i] = min
					validity[i] = true
				} else {
					validity[i] = false
				}
			} else {
				// Propagate null
				validity[i] = false
			}
		} else {
			val := toFloat64Value(s.Get(i))
			if val < min || !hasValidValue {
				min = val
			}
			values[i] = min
			validity[i] = true
			hasValidValue = true
		}
	}

	return convertToOriginalSeriesType(s.Name(), values, validity, s.DataType())
}

// Helper function for cumulative count
func cumCountSeries(s series.Series, skipNulls bool) series.Series {
	length := s.Len()
	values := make([]int64, length)
	validity := make([]bool, length)

	count := int64(0)

	for i := 0; i < length; i++ {
		if !s.IsNull(i) {
			count++
		}
		values[i] = count
		validity[i] = true // Count is always valid
	}

	return series.NewSeriesWithValidity(s.Name(), values, validity, datatypes.Int64{})
}

// Helper to convert float64 array back to original series type
func convertToOriginalSeriesType(name string, values []float64, validity []bool, dataType datatypes.DataType) series.Series {
	switch dataType.(type) {
	case datatypes.Float64:
		return series.NewSeriesWithValidity(name, values, validity, dataType)
	case datatypes.Float32:
		float32Values := make([]float32, len(values))
		for i, v := range values {
			float32Values[i] = float32(v)
		}
		return series.NewSeriesWithValidity(name, float32Values, validity, dataType)
	case datatypes.Int64:
		intValues := make([]int64, len(values))
		for i, v := range values {
			intValues[i] = int64(math.Round(v))
		}
		return series.NewSeriesWithValidity(name, intValues, validity, dataType)
	case datatypes.Int32:
		intValues := make([]int32, len(values))
		for i, v := range values {
			intValues[i] = int32(math.Round(v))
		}
		return series.NewSeriesWithValidity(name, intValues, validity, dataType)
	case datatypes.Int16:
		intValues := make([]int16, len(values))
		for i, v := range values {
			intValues[i] = int16(math.Round(v))
		}
		return series.NewSeriesWithValidity(name, intValues, validity, dataType)
	case datatypes.Int8:
		intValues := make([]int8, len(values))
		for i, v := range values {
			intValues[i] = int8(math.Round(v))
		}
		return series.NewSeriesWithValidity(name, intValues, validity, dataType)
	case datatypes.UInt64:
		uintValues := make([]uint64, len(values))
		for i, v := range values {
			uintValues[i] = uint64(math.Round(math.Max(0, v)))
		}
		return series.NewSeriesWithValidity(name, uintValues, validity, dataType)
	case datatypes.UInt32:
		uintValues := make([]uint32, len(values))
		for i, v := range values {
			uintValues[i] = uint32(math.Round(math.Max(0, v)))
		}
		return series.NewSeriesWithValidity(name, uintValues, validity, dataType)
	case datatypes.UInt16:
		uintValues := make([]uint16, len(values))
		for i, v := range values {
			uintValues[i] = uint16(math.Round(math.Max(0, v)))
		}
		return series.NewSeriesWithValidity(name, uintValues, validity, dataType)
	case datatypes.UInt8:
		uintValues := make([]uint8, len(values))
		for i, v := range values {
			uintValues[i] = uint8(math.Round(math.Max(0, v)))
		}
		return series.NewSeriesWithValidity(name, uintValues, validity, dataType)
	default:
		// Default to float64
		return series.NewSeriesWithValidity(name, values, validity, datatypes.Float64{})
	}
}
