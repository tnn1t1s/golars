package strings

import (
	"strings"
	"unicode/utf8"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// StringOps provides string manipulation operations on a Series
type StringOps struct {
	s series.Series
}

// NewStringOps creates a new StringOps instance
func NewStringOps(s series.Series) *StringOps {
	return &StringOps{s: s}
}

// Length returns the length of each string in the series (byte length)
func (so *StringOps) Length() series.Series {
	return applyUnaryInt32Op(so.s, func(s string) int32 {
		return int32(len(s))
	}, "length")
}

// RuneLength returns the number of UTF-8 runes in each string
func (so *StringOps) RuneLength() series.Series {
	return applyUnaryInt32Op(so.s, func(s string) int32 {
		return int32(utf8.RuneCountInString(s))
	}, "rune_length")
}

// Concat concatenates two string series element-wise
func (so *StringOps) Concat(other series.Series) series.Series {
	values, validity := getStringValuesWithValidity(so.s)
	otherValues, otherValidity := getStringValuesWithValidity(other)
	n := len(values)
	if len(otherValues) < n {
		n = len(otherValues)
	}

	result := make([]string, n)
	resultValidity := make([]bool, n)
	for i := 0; i < n; i++ {
		if validity[i] && otherValidity[i] {
			result[i] = values[i] + otherValues[i]
			resultValidity[i] = true
		}
	}
	return series.NewSeriesWithValidity("concat", result, resultValidity, datatypes.String{})
}

// Repeat repeats each string n times
func (so *StringOps) Repeat(n int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		return strings.Repeat(s, n)
	}, "repeat")
}

// Slice extracts a substring from each string
func (so *StringOps) Slice(start, length int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		runes := []rune(s)
		runeLen := len(runes)

		localStart := start
		if localStart < 0 {
			localStart = runeLen + localStart
			if localStart < 0 {
				localStart = 0
			}
		}
		if localStart > runeLen {
			return ""
		}

		end := localStart + length
		if end > runeLen {
			end = runeLen
		}
		return string(runes[localStart:end])
	}, "slice")
}

// Left returns the first n characters of each string
func (so *StringOps) Left(n int) series.Series {
	return so.Slice(0, n)
}

// Right returns the last n characters of each string
func (so *StringOps) Right(n int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		runes := []rune(s)
		if n >= len(runes) {
			return s
		}
		return string(runes[len(runes)-n:])
	}, "right")
}

// Replace replaces occurrences of a pattern with a replacement string
// n < 0 means replace all occurrences
func (so *StringOps) Replace(pattern, replacement string, n int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		if n < 0 {
			return strings.ReplaceAll(s, pattern, replacement)
		}
		return strings.Replace(s, pattern, replacement, n)
	}, "replace")
}

// Reverse reverses each string
func (so *StringOps) Reverse() series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)
	}, "reverse")
}

// getStringValuesWithValidity extracts string values and validity from a series
func getStringValuesWithValidity(s series.Series) ([]string, []bool) {
	values, validity, ok := series.StringValuesWithValidity(s)
	if ok {
		return values, validity
	}
	// Fallback: use Get interface
	n := s.Len()
	vals := make([]string, n)
	val := make([]bool, n)
	for i := 0; i < n; i++ {
		if !s.IsNull(i) {
			v := s.Get(i)
			if sv, ok := v.(string); ok {
				vals[i] = sv
				val[i] = true
			}
		}
	}
	return vals, val
}

// applyUnaryStringOp applies a unary operation that returns a string to each element
func applyUnaryStringOp(s series.Series, op func(string) string, name string) series.Series {
	values, validity := getStringValuesWithValidity(s)
	n := len(values)
	result := make([]string, n)
	resultValidity := make([]bool, n)
	for i := 0; i < n; i++ {
		if validity[i] {
			result[i] = op(values[i])
			resultValidity[i] = true
		}
	}
	return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{})
}

// applyUnaryBoolOp applies a unary operation that returns a boolean to each string in the series
func applyUnaryBoolOp(s series.Series, op func(string) bool, name string) series.Series {
	values, validity := getStringValuesWithValidity(s)
	n := len(values)
	result := make([]bool, n)
	resultValidity := make([]bool, n)
	for i := 0; i < n; i++ {
		if validity[i] {
			result[i] = op(values[i])
			resultValidity[i] = true
		}
	}
	return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.Boolean{})
}

// applyUnaryInt32Op applies a unary operation that returns an int32 to each string in the series
func applyUnaryInt32Op(s series.Series, op func(string) int32, name string) series.Series {
	values, validity := getStringValuesWithValidity(s)
	n := len(values)
	result := make([]int32, n)
	resultValidity := make([]bool, n)
	for i := 0; i < n; i++ {
		if validity[i] {
			result[i] = op(values[i])
			resultValidity[i] = true
		}
	}
	return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.Int32{})
}

// applyUnaryInt64Op applies a unary operation that returns an int64 to each string in the series
func applyUnaryInt64Op(s series.Series, op func(string) int64, name string) series.Series {
	values, validity := getStringValuesWithValidity(s)
	n := len(values)
	result := make([]int64, n)
	resultValidity := make([]bool, n)
	for i := 0; i < n; i++ {
		if validity[i] {
			result[i] = op(values[i])
			resultValidity[i] = true
		}
	}
	return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.Int64{})
}

// Helper function to apply unary string operations (generic version)
func applyUnaryOp(s series.Series, op func(string) interface{}, name string) series.Series {
	values, validity := getStringValuesWithValidity(s)
	n := len(values)

	// First pass: find first non-null result to determine type
	var sample interface{}
	for i := 0; i < n; i++ {
		if validity[i] {
			sample = op(values[i])
			break
		}
	}
	if sample == nil {
		// All null, return string series
		result := make([]string, n)
		resultValidity := make([]bool, n)
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{})
	}

	switch sample.(type) {
	case bool:
		result := make([]bool, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				result[i] = op(values[i]).(bool)
				resultValidity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.Boolean{})
	case int32:
		result := make([]int32, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				result[i] = op(values[i]).(int32)
				resultValidity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.Int32{})
	case int64:
		result := make([]int64, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				result[i] = op(values[i]).(int64)
				resultValidity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.Int64{})
	default:
		// String output
		result := make([]string, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				result[i] = op(values[i]).(string)
				resultValidity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{})
	}
}

// Helper function to apply binary string operations
func applyBinaryOp(s1, s2 series.Series, op func(string, string) interface{}, name string) series.Series {
	values1, validity1 := getStringValuesWithValidity(s1)
	values2, validity2 := getStringValuesWithValidity(s2)
	n := len(values1)
	if len(values2) < n {
		n = len(values2)
	}

	result := make([]string, n)
	resultValidity := make([]bool, n)
	for i := 0; i < n; i++ {
		if validity1[i] && validity2[i] {
			result[i] = op(values1[i], values2[i]).(string)
			resultValidity[i] = true
		}
	}
	return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{})
}

// Helper function to apply unary operations that may return errors
func applyUnaryOpWithError(s series.Series, op func(string) (interface{}, error), name string) (series.Series, error) {
	values, validity := getStringValuesWithValidity(s)
	n := len(values)

	// First pass: determine output type
	var sample interface{}
	for i := 0; i < n; i++ {
		if validity[i] {
			v, err := op(values[i])
			if err == nil {
				sample = v
				break
			}
		}
	}

	if sample == nil {
		// Default to string
		result := make([]string, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				v, err := op(values[i])
				if err == nil {
					result[i] = v.(string)
					resultValidity[i] = true
				}
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{}), nil
	}

	switch sample.(type) {
	case string:
		result := make([]string, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				v, err := op(values[i])
				if err == nil {
					result[i] = v.(string)
					resultValidity[i] = true
				}
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{}), nil
	default:
		result := make([]string, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				v, err := op(values[i])
				if err == nil {
					result[i] = v.(string)
					resultValidity[i] = true
				}
			}
		}
		return series.NewSeriesWithValidity(name, result, resultValidity, datatypes.String{}), nil
	}
}

// applyPatternOp applies a pattern operation that returns a boolean
func applyPatternOp(s series.Series, op func(string) bool, name string) series.Series {
	return applyUnaryBoolOp(s, op, name)
}

// createBooleanSeries creates a boolean series (unused but kept for API compat)
func createBooleanSeries(name string, values []bool, _ interface{}) series.Series {
	return series.NewBooleanSeries(name, values)
}
