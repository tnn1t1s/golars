package strings

import (
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// ToInteger parses strings to integers with optional base
func (so *StringOps) ToInteger(base ...int) (series.Series, error) {
	b := 10
	if len(base) > 0 {
		b = base[0]
	}

	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)

	// First pass: parse all values and track min/max for type selection
	parsed := make([]int64, n)
	parsedValidity := make([]bool, n)
	var minVal, maxVal int64
	hasNegative := false
	hasValid := false

	for i := 0; i < n; i++ {
		if !validity[i] || values[i] == "" {
			continue
		}
		v, err := strconv.ParseInt(values[i], b, 64)
		if err != nil {
			// Try unsigned
			uv, uerr := strconv.ParseUint(values[i], b, 64)
			if uerr != nil {
				continue
			}
			parsed[i] = int64(uv)
			parsedValidity[i] = true
			if !hasValid {
				minVal = int64(uv)
				maxVal = int64(uv)
				hasValid = true
			} else {
				if int64(uv) < minVal {
					minVal = int64(uv)
				}
				if int64(uv) > maxVal {
					maxVal = int64(uv)
				}
			}
			continue
		}
		parsed[i] = v
		parsedValidity[i] = true
		if v < 0 {
			hasNegative = true
		}
		if !hasValid {
			minVal = v
			maxVal = v
			hasValid = true
		} else {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}

	// Choose appropriate type
	if hasNegative {
		if minVal >= math.MinInt16 && maxVal <= math.MaxInt16 {
			result := make([]int16, n)
			for i := 0; i < n; i++ {
				result[i] = int16(parsed[i])
			}
			return series.NewSeriesWithValidity("to_integer", result, parsedValidity, datatypes.Int16{}), nil
		}
		if minVal >= math.MinInt32 && maxVal <= math.MaxInt32 {
			result := make([]int32, n)
			for i := 0; i < n; i++ {
				result[i] = int32(parsed[i])
			}
			return series.NewSeriesWithValidity("to_integer", result, parsedValidity, datatypes.Int32{}), nil
		}
		return series.NewSeriesWithValidity("to_integer", parsed, parsedValidity, datatypes.Int64{}), nil
	}

	// Unsigned
	if maxVal <= math.MaxUint8 {
		result := make([]uint8, n)
		for i := 0; i < n; i++ {
			result[i] = uint8(parsed[i])
		}
		return series.NewSeriesWithValidity("to_integer", result, parsedValidity, datatypes.UInt8{}), nil
	}
	if maxVal <= math.MaxUint16 {
		result := make([]uint16, n)
		for i := 0; i < n; i++ {
			result[i] = uint16(parsed[i])
		}
		return series.NewSeriesWithValidity("to_integer", result, parsedValidity, datatypes.UInt16{}), nil
	}
	if maxVal <= math.MaxInt32 {
		result := make([]int32, n)
		for i := 0; i < n; i++ {
			result[i] = int32(parsed[i])
		}
		return series.NewSeriesWithValidity("to_integer", result, parsedValidity, datatypes.Int32{}), nil
	}
	return series.NewSeriesWithValidity("to_integer", parsed, parsedValidity, datatypes.Int64{}), nil
}

// ToFloat parses strings to floating point numbers
func (so *StringOps) ToFloat() (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)
	result := make([]float64, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] || values[i] == "" {
			continue
		}
		s := strings.TrimSpace(values[i])
		lower := strings.ToLower(s)

		// Handle special cases
		switch lower {
		case "inf", "+inf", "infinity", "+infinity":
			result[i] = math.Inf(1)
			resultValidity[i] = true
			continue
		case "-inf", "-infinity":
			result[i] = math.Inf(-1)
			resultValidity[i] = true
			continue
		case "nan":
			result[i] = math.NaN()
			resultValidity[i] = true
			continue
		}

		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		result[i] = v
		resultValidity[i] = true
	}

	return series.NewSeriesWithValidity("to_float", result, resultValidity, datatypes.Float64{}), nil
}

// ToBoolean parses strings to boolean values
func (so *StringOps) ToBoolean() (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)
	result := make([]bool, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] {
			continue
		}
		lower := strings.ToLower(strings.TrimSpace(values[i]))
		switch lower {
		case "true", "t", "yes", "y", "1", "on":
			result[i] = true
			resultValidity[i] = true
		case "false", "f", "no", "n", "0", "off":
			result[i] = false
			resultValidity[i] = true
		}
	}

	return series.NewSeriesWithValidity("to_boolean", result, resultValidity, datatypes.Boolean{}), nil
}

// ToDateTime parses strings to DateTime with optional format
func (so *StringOps) ToDateTime(format ...string) (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)

	var goFormats []string
	if len(format) > 0 && format[0] != "" {
		goFormats = []string{convertPythonFormatToGo(format[0])}
	} else {
		goFormats = []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02",
			"01/02/2006",
			"02-Jan-2006",
			"2006-01-02T15:04:05",
		}
	}

	data := make([]interface{}, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] || values[i] == "" {
			data[i] = int64(0)
			continue
		}
		s := strings.TrimSpace(values[i])
		for _, fmt := range goFormats {
			t, err := time.Parse(fmt, s)
			if err == nil {
				data[i] = t.UnixNano()
				resultValidity[i] = true
				break
			}
		}
		if data[i] == nil {
			data[i] = int64(0)
		}
	}

	return series.NewInterfaceSeries("to_datetime", data, resultValidity, datatypes.Datetime{Unit: datatypes.Nanoseconds}), nil
}

// ToDate parses strings to Date
func (so *StringOps) ToDate(format ...string) (series.Series, error) {
	dtSeries, err := so.ToDateTime(format...)
	if err != nil {
		return nil, err
	}

	n := dtSeries.Len()
	data := make([]interface{}, n)
	resultValidity := make([]bool, n)

	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		if dtSeries.IsNull(i) {
			data[i] = int32(0)
			continue
		}
		ts := dtSeries.Get(i).(int64)
		t := time.Unix(0, ts).UTC()
		days := int32(t.Sub(epoch).Hours() / 24)
		data[i] = days
		resultValidity[i] = true
	}

	return series.NewInterfaceSeries("to_date", data, resultValidity, datatypes.Date{}), nil
}

// ToTime parses strings to Time
func (so *StringOps) ToTime(format ...string) (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)

	timeFormats := []string{
		"15:04:05",
		"15:04",
		"3:04:05 PM",
		"3:04 PM",
	}

	data := make([]interface{}, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] || values[i] == "" {
			data[i] = int64(0)
			continue
		}
		s := strings.TrimSpace(values[i])
		parsed := false
		for _, fmt := range timeFormats {
			t, err := time.Parse(fmt, s)
			if err == nil {
				ns := int64(t.Hour())*3600*1e9 + int64(t.Minute())*60*1e9 + int64(t.Second())*1e9
				data[i] = ns
				resultValidity[i] = true
				parsed = true
				break
			}
		}
		if !parsed {
			data[i] = int64(0)
		}
	}

	return series.NewInterfaceSeries("to_time", data, resultValidity, datatypes.Time{}), nil
}

// convertPythonFormatToGo converts Python/Polars format strings to Go format
func convertPythonFormatToGo(format string) string {
	replacements := map[string]string{
		"%Y": "2006",
		"%m": "01",
		"%d": "02",
		"%H": "15",
		"%M": "04",
		"%S": "05",
		"%f": "000000",
		"%p": "PM",
		"%I": "03",
		"%b": "Jan",
		"%B": "January",
		"%a": "Mon",
		"%A": "Monday",
	}
	result := format
	for py, go_ := range replacements {
		result = strings.ReplaceAll(result, py, go_)
	}
	return result
}

// IsNumericStr checks if strings can be parsed as numbers
func (so *StringOps) IsNumericStr() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		_, err := strconv.ParseFloat(s, 64)
		return err == nil
	}, "is_numeric_str")
}

// IsAlphaStr checks if strings contain only alphabetic characters
func (so *StringOps) IsAlphaStr() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if !unicode.IsLetter(r) {
				return false
			}
		}
		return true
	}, "is_alpha_str")
}

// IsAlphanumericStr checks if strings contain only alphanumeric characters
func (so *StringOps) IsAlphanumericStr() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				return false
			}
		}
		return true
	}, "is_alphanumeric_str")
}
