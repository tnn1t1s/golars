package strings

import (
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// StringOps provides string manipulation operations on a Series
type StringOps struct {
	s series.Series
}

// NewStringOps creates a new StringOps instance
func NewStringOps(s series.Series) *StringOps {
	return &StringOps{s: s}
}

// Length returns the length of each string in the series
func (so *StringOps) Length() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return int32(len(str))
	}, "length")
}

// RuneLength returns the number of UTF-8 runes in each string
func (so *StringOps) RuneLength() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return int32(utf8.RuneCountInString(str))
	}, "rune_length")
}

// Concat concatenates two string series element-wise
func (so *StringOps) Concat(other series.Series) series.Series {
	return applyBinaryOp(so.s, other, func(s1, s2 string) interface{} {
		return s1 + s2
	}, "concat")
}

// Repeat repeats each string n times
func (so *StringOps) Repeat(n int) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return strings.Repeat(str, n)
	}, "repeat")
}

// Slice extracts a substring from each string
func (so *StringOps) Slice(start, length int) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		// Convert to runes for proper Unicode handling
		runes := []rune(str)
		runeLen := len(runes)
		
		// Create local copy of start to avoid modifying the parameter
		localStart := start
		
		// Handle negative start
		if localStart < 0 {
			localStart = runeLen + localStart
		}
		if localStart < 0 {
			localStart = 0
		}
		if localStart >= runeLen {
			return ""
		}
		
		// Calculate end position
		end := localStart + length
		if end > runeLen || length < 0 {
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
	return applyUnaryOp(so.s, func(str string) interface{} {
		runes := []rune(str)
		if n > len(runes) {
			return str
		}
		return string(runes[len(runes)-n:])
	}, "right")
}

// Replace replaces occurrences of a pattern with a replacement string
// n < 0 means replace all occurrences
func (so *StringOps) Replace(pattern, replacement string, n int) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		if n < 0 {
			return strings.ReplaceAll(str, pattern, replacement)
		}
		return strings.Replace(str, pattern, replacement, n)
	}, "replace")
}

// Reverse reverses each string
func (so *StringOps) Reverse() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)
	}, "reverse")
}

// Helper function to apply unary string operations
func applyUnaryOp(s series.Series, op func(string) interface{}, name string) series.Series {
	pool := memory.NewGoAllocator()
	length := s.Len()
	
	// Determine output type based on first non-null result
	var outputType arrow.DataType
	var firstResult interface{}
	for i := 0; i < length; i++ {
		if !s.IsNull(i) {
			val := s.Get(i)
			if str, ok := val.(string); ok {
				firstResult = op(str)
				break
			}
		}
	}
	
	// Determine output type
	switch firstResult.(type) {
	case int32:
		outputType = arrow.PrimitiveTypes.Int32
	case string:
		outputType = arrow.BinaryTypes.String
	default:
		outputType = arrow.BinaryTypes.String
	}
	
	// Build appropriate array based on output type
	switch outputType {
	case arrow.PrimitiveTypes.Int32:
		builder := array.NewInt32Builder(pool)
		defer builder.Release()
		
		for i := 0; i < length; i++ {
			if s.IsNull(i) {
				builder.AppendNull()
			} else {
				val := s.Get(i)
				if str, ok := val.(string); ok {
					result := op(str)
					if intVal, ok := result.(int32); ok {
						builder.Append(intVal)
					} else {
						builder.AppendNull()
					}
				} else {
					builder.AppendNull()
				}
			}
		}
		
		arr := builder.NewArray()
		return series.NewInt32Series(name, arr.(*array.Int32).Int32Values())
		
	default: // String output
		builder := array.NewStringBuilder(pool)
		defer builder.Release()
		
		for i := 0; i < length; i++ {
			if s.IsNull(i) {
				builder.AppendNull()
			} else {
				val := s.Get(i)
				if str, ok := val.(string); ok {
					result := op(str)
					if strVal, ok := result.(string); ok {
						builder.Append(strVal)
					} else {
						builder.AppendNull()
					}
				} else {
					builder.AppendNull()
				}
			}
		}
		
		arr := builder.NewArray()
		values := make([]string, arr.Len())
		validity := make([]bool, arr.Len())
		strArr := arr.(*array.String)
		for i := 0; i < arr.Len(); i++ {
			if !strArr.IsNull(i) {
				values[i] = strArr.Value(i)
				validity[i] = true
			} else {
				validity[i] = false
			}
		}
		return series.NewSeriesWithValidity(name, values, validity, datatypes.String{})
	}
}

// Helper function to apply binary string operations
func applyBinaryOp(s1, s2 series.Series, op func(string, string) interface{}, name string) series.Series {
	if s1.Len() != s2.Len() {
		panic("series must have the same length for binary operations")
	}
	
	pool := memory.NewGoAllocator()
	length := s1.Len()
	builder := array.NewStringBuilder(pool)
	defer builder.Release()
	
	for i := 0; i < length; i++ {
		if s1.IsNull(i) || s2.IsNull(i) {
			builder.AppendNull()
		} else {
			val1 := s1.Get(i)
			val2 := s2.Get(i)
			if str1, ok1 := val1.(string); ok1 {
				if str2, ok2 := val2.(string); ok2 {
					result := op(str1, str2)
					if strVal, ok := result.(string); ok {
						builder.Append(strVal)
					} else {
						builder.AppendNull()
					}
				} else {
					builder.AppendNull()
				}
			} else {
				builder.AppendNull()
			}
		}
	}
	
	arr := builder.NewArray()
	values := make([]string, arr.Len())
	validity := make([]bool, arr.Len())
	strArr := arr.(*array.String)
	for i := 0; i < arr.Len(); i++ {
		if !strArr.IsNull(i) {
			values[i] = strArr.Value(i)
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	return series.NewSeriesWithValidity(name, values, validity, datatypes.String{})
}

// Helper function to apply unary operations that may return errors
func applyUnaryOpWithError(s series.Series, op func(string) (interface{}, error), name string) (series.Series, error) {
	length := s.Len()
	
	// First pass: determine output type
	var sampleOutput interface{}
	for i := 0; i < length && sampleOutput == nil; i++ {
		if !s.IsNull(i) {
			if str, ok := s.Get(i).(string); ok {
				result, err := op(str)
				if err == nil && result != nil {
					sampleOutput = result
					break
				}
			}
		}
	}
	
	// Default to string if no valid sample
	if sampleOutput == nil {
		sampleOutput = ""
	}
	
	// Build result based on output type
	switch sampleOutput.(type) {
	case []byte:
		// Return as binary data (not implemented in series yet, use string)
		values := make([]string, length)
		validity := make([]bool, length)
		
		for i := 0; i < length; i++ {
			if s.IsNull(i) {
				validity[i] = false
			} else if str, ok := s.Get(i).(string); ok {
				result, err := op(str)
				if err != nil {
					validity[i] = false
				} else if bytes, ok := result.([]byte); ok {
					values[i] = string(bytes)
					validity[i] = true
				} else {
					validity[i] = false
				}
			} else {
				validity[i] = false
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, datatypes.String{}), nil
		
	default: // String output
		values := make([]string, length)
		validity := make([]bool, length)
		
		for i := 0; i < length; i++ {
			if s.IsNull(i) {
				validity[i] = false
			} else if str, ok := s.Get(i).(string); ok {
				result, err := op(str)
				if err != nil {
					validity[i] = false
				} else if strVal, ok := result.(string); ok {
					values[i] = strVal
					validity[i] = true
				} else {
					validity[i] = false
				}
			} else {
				validity[i] = false
			}
		}
		
		return series.NewSeriesWithValidity(name, values, validity, datatypes.String{}), nil
	}
}

// applyUnaryBoolOp applies a unary operation that returns a boolean to each string in the series
func applyUnaryBoolOp(s series.Series, op func(string) bool, name string) series.Series {
	length := s.Len()
	values := make([]bool, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else if str, ok := s.Get(i).(string); ok {
			values[i] = op(str)
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Boolean{})
}

// applyUnaryInt64Op applies a unary operation that returns an int64 to each string in the series
func applyUnaryInt64Op(s series.Series, op func(string) int64, name string) series.Series {
	length := s.Len()
	values := make([]int64, length)
	validity := make([]bool, length)
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			validity[i] = false
		} else if str, ok := s.Get(i).(string); ok {
			values[i] = op(str)
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity(name, values, validity, datatypes.Int64{})
}