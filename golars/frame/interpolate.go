package frame

import (
	"fmt"
	"math"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// InterpolateOptions configures interpolation operations
type InterpolateOptions struct {
	Method    string   // Method: "linear", "nearest", "zero", "slinear", "quadratic", "cubic"
	Axis      int      // Axis: 0 for index, 1 for columns (only 0 supported for now)
	Limit     int      // Maximum number of consecutive NaNs to fill
	LimitArea string   // "inside", "outside", or empty for both
	Columns   []string // Specific columns to interpolate (empty means all numeric)
}

// Interpolate fills null values using interpolation
func (df *DataFrame) Interpolate(options InterpolateOptions) (*DataFrame, error) {
	// Default to linear interpolation
	if options.Method == "" {
		options.Method = "linear"
	}
	
	// Validate method
	validMethods := map[string]bool{
		"linear": true, "nearest": true, "zero": true,
		"slinear": true, "quadratic": true, "cubic": true,
	}
	if !validMethods[options.Method] {
		return nil, fmt.Errorf("invalid interpolation method: %s", options.Method)
	}
	
	// Get columns to interpolate
	columnsToInterp := options.Columns
	if len(columnsToInterp) == 0 {
		// Default to all numeric columns
		columnsToInterp = make([]string, 0)
		for _, col := range df.columns {
			switch col.DataType().(type) {
			case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64,
			     datatypes.UInt8, datatypes.UInt16, datatypes.UInt32, datatypes.UInt64,
			     datatypes.Float32, datatypes.Float64:
				columnsToInterp = append(columnsToInterp, col.Name())
			}
		}
	}
	
	// Create result columns
	resultColumns := make([]series.Series, len(df.columns))
	
	// Process each column
	for i, col := range df.columns {
		colName := col.Name()
		
		// Check if this column should be interpolated
		shouldInterp := false
		for _, name := range columnsToInterp {
			if name == colName {
				shouldInterp = true
				break
			}
		}
		
		if shouldInterp {
			// Interpolate based on method
			switch options.Method {
			case "linear", "slinear":
				resultColumns[i] = linearInterpolate(col, options.Limit, options.LimitArea)
			case "nearest":
				resultColumns[i] = nearestInterpolate(col, options.Limit, options.LimitArea)
			case "zero":
				resultColumns[i] = zeroInterpolate(col, options.Limit, options.LimitArea)
			default:
				// For now, fall back to linear for unsupported methods
				resultColumns[i] = linearInterpolate(col, options.Limit, options.LimitArea)
			}
		} else {
			// Keep column as is
			resultColumns[i] = col
		}
	}
	
	return NewDataFrame(resultColumns...)
}

// Helper function for linear interpolation
func linearInterpolate(s series.Series, limit int, limitArea string) series.Series {
	length := s.Len()
	values := make([]interface{}, length)
	validity := make([]bool, length)
	
	// First pass: copy all valid values
	validIndices := make([]int, 0)
	for i := 0; i < length; i++ {
		if !s.IsNull(i) {
			values[i] = s.Get(i)
			validity[i] = true
			validIndices = append(validIndices, i)
		}
	}
	
	// Second pass: interpolate null values
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			// Find surrounding valid values
			prevIdx, nextIdx := -1, -1
			
			// Find previous valid value
			for j := len(validIndices) - 1; j >= 0; j-- {
				if validIndices[j] < i {
					prevIdx = validIndices[j]
					break
				}
			}
			
			// Find next valid value
			for _, idx := range validIndices {
				if idx > i {
					nextIdx = idx
					break
				}
			}
			
			// Check limit area constraints
			if limitArea == "inside" && (prevIdx == -1 || nextIdx == -1) {
				continue
			}
			if limitArea == "outside" && prevIdx != -1 && nextIdx != -1 {
				continue
			}
			
			// Interpolate
			if prevIdx != -1 && nextIdx != -1 {
				// Check consecutive null limit
				nullCount := nextIdx - prevIdx - 1
				if limit > 0 && nullCount > limit {
					continue
				}
				
				// Linear interpolation
				prevVal := toFloat64Value(s.Get(prevIdx))
				nextVal := toFloat64Value(s.Get(nextIdx))
				fraction := float64(i-prevIdx) / float64(nextIdx-prevIdx)
				interpVal := prevVal + fraction*(nextVal-prevVal)
				
				values[i] = convertToOriginalType(interpVal, s.DataType())
				validity[i] = true
			}
		}
	}
	
	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper function for nearest neighbor interpolation
func nearestInterpolate(s series.Series, limit int, limitArea string) series.Series {
	length := s.Len()
	values := make([]interface{}, length)
	validity := make([]bool, length)
	
	// Copy all values
	for i := 0; i < length; i++ {
		values[i] = s.Get(i)
		validity[i] = !s.IsNull(i)
	}
	
	// Find null ranges and interpolate
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			// Find nearest valid values
			prevDist, nextDist := length, length
			prevIdx, nextIdx := -1, -1
			
			// Search backward
			for j := i - 1; j >= 0; j-- {
				if !s.IsNull(j) {
					prevIdx = j
					prevDist = i - j
					break
				}
			}
			
			// Search forward
			for j := i + 1; j < length; j++ {
				if !s.IsNull(j) {
					nextIdx = j
					nextDist = j - i
					break
				}
			}
			
			// Check limit area constraints
			if limitArea == "inside" && (prevIdx == -1 || nextIdx == -1) {
				continue
			}
			if limitArea == "outside" && prevIdx != -1 && nextIdx != -1 {
				continue
			}
			
			// Choose nearest
			if prevIdx != -1 && (nextIdx == -1 || prevDist <= nextDist) {
				// Check limit
				if limit <= 0 || prevDist <= limit {
					values[i] = s.Get(prevIdx)
					validity[i] = true
				}
			} else if nextIdx != -1 {
				// Check limit
				if limit <= 0 || nextDist <= limit {
					values[i] = s.Get(nextIdx)
					validity[i] = true
				}
			}
		}
	}
	
	return createSeriesFromInterface(s.Name(), values, validity, s.DataType())
}

// Helper function for zero-order hold interpolation
func zeroInterpolate(s series.Series, limit int, limitArea string) series.Series {
	// Zero-order hold is essentially forward fill
	return forwardFillSeries(s, limit)
}

// Helper to convert value to float64
func toFloat64Value(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case int16:
		return float64(val)
	case int8:
		return float64(val)
	case int:
		return float64(val)
	case uint64:
		return float64(val)
	case uint32:
		return float64(val)
	case uint16:
		return float64(val)
	case uint8:
		return float64(val)
	default:
		return 0
	}
}

// Helper to convert float64 back to original type
func convertToOriginalType(val float64, dataType datatypes.DataType) interface{} {
	switch dataType.(type) {
	case datatypes.Float64:
		return val
	case datatypes.Float32:
		return float32(val)
	case datatypes.Int64:
		return int64(math.Round(val))
	case datatypes.Int32:
		return int32(math.Round(val))
	case datatypes.Int16:
		return int16(math.Round(val))
	case datatypes.Int8:
		return int8(math.Round(val))
	case datatypes.UInt64:
		return uint64(math.Round(val))
	case datatypes.UInt32:
		return uint32(math.Round(val))
	case datatypes.UInt16:
		return uint16(math.Round(val))
	case datatypes.UInt8:
		return uint8(math.Round(val))
	default:
		return val
	}
}