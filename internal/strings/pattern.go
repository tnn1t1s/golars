package strings

import (
	"strings"

	"github.com/tnn1t1s/golars/series"
)

// Contains checks if each string contains the pattern
func (so *StringOps) Contains(pattern string, literal bool) series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		return strings.Contains(s, pattern)
	}, "contains")
}

// StartsWith checks if each string starts with the pattern
func (so *StringOps) StartsWith(pattern string) series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		return strings.HasPrefix(s, pattern)
	}, "starts_with")
}

// EndsWith checks if each string ends with the pattern
func (so *StringOps) EndsWith(pattern string) series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		return strings.HasSuffix(s, pattern)
	}, "ends_with")
}

// Find returns the index of the first occurrence of pattern, or -1 if not found
func (so *StringOps) Find(pattern string) series.Series {
	return applyUnaryInt32Op(so.s, func(s string) int32 {
		return int32(strings.Index(s, pattern))
	}, "find")
}

// Count returns the number of non-overlapping occurrences of pattern
func (so *StringOps) Count(pattern string) series.Series {
	return applyUnaryInt32Op(so.s, func(s string) int32 {
		if pattern == "" {
			return int32(len([]rune(s)) + 1)
		}
		return int32(strings.Count(s, pattern))
	}, "count")
}

// Split splits each string by the separator and returns a list series
func (so *StringOps) Split(separator string) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		parts := strings.Split(s, separator)
		if len(parts) > 0 {
			return parts[0]
		}
		return ""
	}, "split")
}

// SplitN splits each string into at most n parts
func (so *StringOps) SplitN(separator string, n int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		parts := strings.SplitN(s, separator, n)
		if len(parts) > 0 {
			return parts[0]
		}
		return ""
	}, "split_n")
}

// JoinList concatenates elements with a separator (for future list series support)
func (so *StringOps) JoinList(separator string) series.Series {
	// For now, just return the original series
	return so.s
}
