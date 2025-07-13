package strings

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// ToInteger parses strings to integers with optional base
func (so *StringOps) ToInteger(base ...int) (series.Series, error) {
	b := 10
	if len(base) > 0 {
		b = base[0]
	}
	
	// Count valid values first to determine output type
	var maxVal int64
	var minVal int64
	first := true
	
	for i := 0; i < so.s.Len(); i++ {
		if !so.s.IsNull(i) {
			str := so.s.Get(i).(string)
			str = strings.TrimSpace(str)
			if str == "" {
				continue
			}
			
			val, err := strconv.ParseInt(str, b, 64)
			if err == nil {
				if first {
					maxVal = val
					minVal = val
					first = false
				} else {
					if val > maxVal {
						maxVal = val
					}
					if val < minVal {
						minVal = val
					}
				}
			}
		}
	}
	
	// Choose appropriate integer type
	var dtype datatypes.DataType
	if minVal >= 0 && maxVal <= 255 {
		dtype = datatypes.UInt8{}
	} else if minVal >= -128 && maxVal <= 127 {
		dtype = datatypes.Int8{}
	} else if minVal >= 0 && maxVal <= 65535 {
		dtype = datatypes.UInt16{}
	} else if minVal >= -32768 && maxVal <= 32767 {
		dtype = datatypes.Int16{}
	} else if minVal >= 0 && maxVal <= 4294967295 {
		dtype = datatypes.UInt32{}
	} else if minVal >= -2147483648 && maxVal <= 2147483647 {
		dtype = datatypes.Int32{}
	} else {
		dtype = datatypes.Int64{}
	}
	
	// Parse and convert
	values := make([]interface{}, so.s.Len())
	validity := make([]bool, so.s.Len())
	
	for i := 0; i < so.s.Len(); i++ {
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}
		
		str := so.s.Get(i).(string)
		str = strings.TrimSpace(str)
		if str == "" {
			validity[i] = false
			continue
		}
		
		val, err := strconv.ParseInt(str, b, 64)
		if err != nil {
			validity[i] = false
		} else {
			validity[i] = true
			// Convert to appropriate type
			switch dtype.(type) {
			case datatypes.UInt8:
				values[i] = uint8(val)
			case datatypes.Int8:
				values[i] = int8(val)
			case datatypes.UInt16:
				values[i] = uint16(val)
			case datatypes.Int16:
				values[i] = int16(val)
			case datatypes.UInt32:
				values[i] = uint32(val)
			case datatypes.Int32:
				values[i] = int32(val)
			default:
				values[i] = val
			}
		}
	}
	
	// Create appropriate slice based on type
	switch dtype.(type) {
	case datatypes.UInt8:
		typedValues := make([]uint8, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(uint8)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	case datatypes.Int8:
		typedValues := make([]int8, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(int8)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	case datatypes.UInt16:
		typedValues := make([]uint16, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(uint16)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	case datatypes.Int16:
		typedValues := make([]int16, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(int16)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	case datatypes.UInt32:
		typedValues := make([]uint32, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(uint32)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	case datatypes.Int32:
		typedValues := make([]int32, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(int32)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	default:
		typedValues := make([]int64, len(values))
		for i, v := range values {
			if v != nil {
				typedValues[i] = v.(int64)
			}
		}
		return series.NewSeriesWithValidity(so.s.Name()+"_int", typedValues, validity, dtype), nil
	}
}

// ToFloat parses strings to floating point numbers
func (so *StringOps) ToFloat() (series.Series, error) {
	values := make([]float64, so.s.Len())
	validity := make([]bool, so.s.Len())
	
	for i := 0; i < so.s.Len(); i++ {
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}
		
		str := so.s.Get(i).(string)
		str = strings.TrimSpace(str)
		if str == "" {
			validity[i] = false
			continue
		}
		
		// Handle special cases
		switch strings.ToLower(str) {
		case "inf", "+inf", "infinity", "+infinity":
			values[i] = math.Inf(1) // +Inf
			validity[i] = true
		case "-inf", "-infinity":
			values[i] = math.Inf(-1) // -Inf
			validity[i] = true
		case "nan":
			values[i] = math.NaN() // NaN
			validity[i] = true
		default:
			val, err := strconv.ParseFloat(str, 64)
			if err != nil {
				validity[i] = false
			} else {
				values[i] = val
				validity[i] = true
			}
		}
	}
	
	return series.NewSeriesWithValidity(so.s.Name()+"_float", values, validity, datatypes.Float64{}), nil
}

// ToBoolean parses strings to boolean values
func (so *StringOps) ToBoolean() (series.Series, error) {
	values := make([]bool, so.s.Len())
	validity := make([]bool, so.s.Len())
	
	for i := 0; i < so.s.Len(); i++ {
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}
		
		str := so.s.Get(i).(string)
		str = strings.TrimSpace(strings.ToLower(str))
		
		switch str {
		case "true", "t", "yes", "y", "1", "on":
			values[i] = true
			validity[i] = true
		case "false", "f", "no", "n", "0", "off", "":
			values[i] = false
			validity[i] = true
		default:
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity(so.s.Name()+"_bool", values, validity, datatypes.Boolean{}), nil
}

// ToDateTime parses strings to DateTime with optional format
func (so *StringOps) ToDateTime(format ...string) (series.Series, error) {
	// Import datetime package functions
	parseFunc := func(str string) (int64, error) {
		// Default formats to try
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
			"01/02/2006",
			"02-Jan-2006",
			"Jan 2, 2006",
		}
		
		if len(format) > 0 && format[0] != "" {
			// Convert Python/Polars format to Go format if needed
			goFormat := convertPythonFormatToGo(format[0])
			formats = []string{goFormat}
		}
		
		for _, f := range formats {
			if t, err := time.Parse(f, str); err == nil {
				return t.UnixNano(), nil
			}
		}
		
		return 0, fmt.Errorf("unable to parse datetime: %s", str)
	}
	
	values := make([]int64, so.s.Len())
	validity := make([]bool, so.s.Len())
	
	for i := 0; i < so.s.Len(); i++ {
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}
		
		str := so.s.Get(i).(string)
		str = strings.TrimSpace(str)
		if str == "" {
			validity[i] = false
			continue
		}
		
		ts, err := parseFunc(str)
		if err != nil {
			validity[i] = false
		} else {
			values[i] = ts
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(so.s.Name()+"_datetime", values, validity, 
		datatypes.Datetime{Unit: datatypes.Nanoseconds}), nil
}

// ToDate parses strings to Date
func (so *StringOps) ToDate(format ...string) (series.Series, error) {
	// Use ToDateTime and extract date part
	dtSeries, err := so.ToDateTime(format...)
	if err != nil {
		return nil, err
	}
	
	// Convert timestamps to days since epoch
	values := make([]int32, dtSeries.Len())
	validity := make([]bool, dtSeries.Len())
	
	for i := 0; i < dtSeries.Len(); i++ {
		if dtSeries.IsNull(i) {
			validity[i] = false
			continue
		}
		
		ts := dtSeries.Get(i).(int64)
		t := time.Unix(0, ts).UTC()
		days := int32(t.Unix() / 86400)
		values[i] = days
		validity[i] = true
	}
	
	return series.NewSeriesWithValidity(so.s.Name()+"_date", values, validity, datatypes.Date{}), nil
}

// ToTime parses strings to Time
func (so *StringOps) ToTime(format ...string) (series.Series, error) {
	parseFunc := func(str string) (int64, error) {
		// Default time formats
		formats := []string{
			"15:04:05",
			"15:04:05.000",
			"15:04:05.000000",
			"15:04:05.000000000",
			"3:04:05 PM",
			"3:04 PM",
		}
		
		if len(format) > 0 && format[0] != "" {
			goFormat := convertPythonFormatToGo(format[0])
			formats = []string{goFormat}
		}
		
		for _, f := range formats {
			if t, err := time.Parse(f, str); err == nil {
				// Extract time as nanoseconds since midnight
				return int64(t.Hour())*3600e9 + int64(t.Minute())*60e9 + 
					int64(t.Second())*1e9 + int64(t.Nanosecond()), nil
			}
		}
		
		return 0, fmt.Errorf("unable to parse time: %s", str)
	}
	
	values := make([]int64, so.s.Len())
	validity := make([]bool, so.s.Len())
	
	for i := 0; i < so.s.Len(); i++ {
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}
		
		str := so.s.Get(i).(string)
		str = strings.TrimSpace(str)
		if str == "" {
			validity[i] = false
			continue
		}
		
		ns, err := parseFunc(str)
		if err != nil {
			validity[i] = false
		} else {
			values[i] = ns
			validity[i] = true
		}
	}
	
	return series.NewSeriesWithValidity(so.s.Name()+"_time", values, validity, 
		datatypes.Time{}), nil
}

// Helper function to convert Python/Polars format strings to Go format
func convertPythonFormatToGo(format string) string {
	replacements := map[string]string{
		"%Y": "2006",
		"%y": "06",
		"%m": "01",
		"%B": "January",
		"%b": "Jan",
		"%d": "02",
		"%H": "15",
		"%I": "03",
		"%M": "04",
		"%S": "05",
		"%p": "PM",
		"%f": "000000",
		"%z": "-0700",
		"%Z": "MST",
		"%-d": "2",
		"%-m": "1",
		"%-I": "3",
		"%-H": "15",
	}
	
	result := format
	for old, new := range replacements {
		result = strings.ReplaceAll(result, old, new)
	}
	
	return result
}

// IsNumericStr checks if strings can be parsed as numbers
func (so *StringOps) IsNumericStr() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		str = strings.TrimSpace(str)
		if str == "" {
			return false
		}
		
		// Try parsing as float (covers integers too)
		_, err := strconv.ParseFloat(str, 64)
		return err == nil
	}, "is_numeric")
}

// IsAlphaStr checks if strings contain only alphabetic characters
func (so *StringOps) IsAlphaStr() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		if str == "" {
			return false
		}
		for _, r := range str {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
				return false
			}
		}
		return true
	}, "is_alpha")
}

// IsAlphanumericStr checks if strings contain only alphanumeric characters
func (so *StringOps) IsAlphanumericStr() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		if str == "" {
			return false
		}
		for _, r := range str {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
				return false
			}
		}
		return true
	}, "is_alphanumeric")
}