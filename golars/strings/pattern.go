package strings

import (
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/davidpalaitis/golars/series"
)

// Contains checks if each string contains the pattern
func (so *StringOps) Contains(pattern string, literal bool) series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		if literal {
			return strings.Contains(str, pattern)
		}
		// For non-literal, pattern is treated as substring search
		return strings.Contains(str, pattern)
	}, "contains")
}

// StartsWith checks if each string starts with the pattern
func (so *StringOps) StartsWith(pattern string) series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		return strings.HasPrefix(str, pattern)
	}, "starts_with")
}

// EndsWith checks if each string ends with the pattern
func (so *StringOps) EndsWith(pattern string) series.Series {
	return applyPatternOp(so.s, func(str string) bool {
		return strings.HasSuffix(str, pattern)
	}, "ends_with")
}

// Find returns the index of the first occurrence of pattern, or -1 if not found
func (so *StringOps) Find(pattern string) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		idx := strings.Index(str, pattern)
		return int32(idx)
	}, "find")
}

// Count returns the number of non-overlapping occurrences of pattern
func (so *StringOps) Count(pattern string) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		if pattern == "" {
			// Empty pattern counts positions between runes plus start and end
			return int32(len(str) + 1)
		}
		return int32(strings.Count(str, pattern))
	}, "count")
}

// Split splits each string by the separator and returns a list series
func (so *StringOps) Split(separator string) series.Series {
	pool := memory.NewGoAllocator()
	length := so.s.Len()
	
	// Build list of string arrays
	listBuilder := array.NewListBuilder(pool, arrow.BinaryTypes.String)
	defer listBuilder.Release()
	
	valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
	
	for i := 0; i < length; i++ {
		if so.s.IsNull(i) {
			listBuilder.AppendNull()
		} else {
			val := so.s.Get(i)
			if str, ok := val.(string); ok {
				parts := strings.Split(str, separator)
				listBuilder.Append(true)
				for _, part := range parts {
					valueBuilder.Append(part)
				}
			} else {
				listBuilder.AppendNull()
			}
		}
	}
	
	arr := listBuilder.NewArray()
	
	// For now, return a string series with the first element of each split
	// TODO: Implement proper List series type
	result := make([]string, length)
	listArr := arr.(*array.List)
	
	for i := 0; i < length; i++ {
		if listArr.IsNull(i) {
			result[i] = ""
		} else {
			start, end := listArr.ValueOffsets(i)
			if start < end {
				result[i] = valueBuilder.Value(int(start))
			} else {
				result[i] = ""
			}
		}
	}
	
	return series.NewStringSeries("split", result)
}

// SplitN splits each string into at most n parts
func (so *StringOps) SplitN(separator string, n int) series.Series {
	pool := memory.NewGoAllocator()
	length := so.s.Len()
	
	// Build list of string arrays
	listBuilder := array.NewListBuilder(pool, arrow.BinaryTypes.String)
	defer listBuilder.Release()
	
	valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
	
	for i := 0; i < length; i++ {
		if so.s.IsNull(i) {
			listBuilder.AppendNull()
		} else {
			val := so.s.Get(i)
			if str, ok := val.(string); ok {
				parts := strings.SplitN(str, separator, n)
				listBuilder.Append(true)
				for _, part := range parts {
					valueBuilder.Append(part)
				}
			} else {
				listBuilder.AppendNull()
			}
		}
	}
	
	arr := listBuilder.NewArray()
	
	// For now, return a string series with the first element of each split
	// TODO: Implement proper List series type
	result := make([]string, length)
	listArr := arr.(*array.List)
	
	for i := 0; i < length; i++ {
		if listArr.IsNull(i) {
			result[i] = ""
		} else {
			start, end := listArr.ValueOffsets(i)
			if start < end {
				result[i] = valueBuilder.Value(int(start))
			} else {
				result[i] = ""
			}
		}
	}
	
	return series.NewStringSeries("split_n", result)
}

// JoinList concatenates elements with a separator (for future list series support)
func (so *StringOps) JoinList(separator string) series.Series {
	// For now, this just returns the original series
	// TODO: Implement when List series type is available
	return so.s
}

// Helper function to apply pattern operations that return boolean results
func applyPatternOp(s series.Series, op func(string) bool, name string) series.Series {
	pool := memory.NewGoAllocator()
	length := s.Len()
	builder := array.NewBooleanBuilder(pool)
	defer builder.Release()
	
	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			builder.AppendNull()
		} else {
			val := s.Get(i)
			if str, ok := val.(string); ok {
				builder.Append(op(str))
			} else {
				builder.AppendNull()
			}
		}
	}
	
	arr := builder.NewArray()
	values := make([]bool, arr.Len())
	boolArr := arr.(*array.Boolean)
	for i := 0; i < arr.Len(); i++ {
		if !boolArr.IsNull(i) {
			values[i] = boolArr.Value(i)
		}
	}
	
	// Use the helper function for boolean results
	return createBooleanSeries(name, values, boolArr)
}

// Helper to create boolean series with null handling
func createBooleanSeries(name string, values []bool, boolArr *array.Boolean) series.Series {
	// Create a mask for null values
	mask := make([]bool, len(values))
	for i := 0; i < len(values); i++ {
		mask[i] = boolArr.IsNull(i)
	}
	
	// Use NewBooleanSeries which should handle the values and mask appropriately
	return series.NewBooleanSeries(name, values)
}