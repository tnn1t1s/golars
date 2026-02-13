package strings

import (
	"regexp"
	"sync"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// regexCache caches compiled regex patterns for performance
type regexCache struct {
	mu       sync.RWMutex
	patterns map[string]*regexp.Regexp
	maxSize  int
}

var globalRegexCache = &regexCache{
	patterns: make(map[string]*regexp.Regexp),
	maxSize:  1000,
}

// compileRegex compiles and caches a regex pattern
func compileRegex(pattern string) (*regexp.Regexp, error) {
	globalRegexCache.mu.RLock()
	if re, ok := globalRegexCache.patterns[pattern]; ok {
		globalRegexCache.mu.RUnlock()
		return re, nil
	}
	globalRegexCache.mu.RUnlock()

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	globalRegexCache.mu.Lock()
	if len(globalRegexCache.patterns) < globalRegexCache.maxSize {
		globalRegexCache.patterns[pattern] = re
	}
	globalRegexCache.mu.Unlock()

	return re, nil
}

// Match checks if the pattern matches the string
func (so *StringOps) Match(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return all-false series on error
		n := so.s.Len()
		result := make([]bool, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			validity[i] = true
		}
		return series.NewSeriesWithValidity("match", result, validity, datatypes.Boolean{})
	}

	return applyUnaryBoolOp(so.s, func(s string) bool {
		return re.MatchString(s)
	}, "match")
}

// Extract extracts the first match of the pattern (or specific group)
func (so *StringOps) Extract(pattern string, group int) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		n := so.s.Len()
		result := make([]string, n)
		validity := make([]bool, n)
		return series.NewSeriesWithValidity("extract", result, validity, datatypes.String{})
	}

	return applyUnaryStringOp(so.s, func(s string) string {
		matches := re.FindStringSubmatch(s)
		if matches == nil {
			return ""
		}
		if group > 0 && group < len(matches) {
			return matches[group]
		}
		return matches[0]
	}, "extract")
}

// ExtractAll extracts all matches of the pattern
func (so *StringOps) ExtractAll(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		n := so.s.Len()
		result := make([]string, n)
		validity := make([]bool, n)
		return series.NewSeriesWithValidity("extract_all", result, validity, datatypes.String{})
	}

	return applyUnaryStringOp(so.s, func(s string) string {
		matches := re.FindAllString(s, -1)
		if len(matches) > 0 {
			return matches[0]
		}
		return ""
	}, "extract_all")
}

// ReplaceRegex replaces matches of the regex pattern with replacement
func (so *StringOps) ReplaceRegex(pattern, replacement string, n int) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		return so.s
	}

	return applyUnaryStringOp(so.s, func(s string) string {
		if n < 0 {
			return re.ReplaceAllString(s, replacement)
		}
		// Limited replacements: manually replace n times
		result := s
		for i := 0; i < n; i++ {
			loc := re.FindStringIndex(result)
			if loc == nil {
				break
			}
			result = result[:loc[0]] + replacement + result[loc[1]:]
		}
		return result
	}, "replace_regex")
}

// SplitRegex splits the string by regex pattern
func (so *StringOps) SplitRegex(pattern string, n int) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		return so.s
	}

	return applyUnaryStringOp(so.s, func(s string) string {
		var parts []string
		if n < 0 {
			parts = re.Split(s, -1)
		} else {
			parts = re.Split(s, n)
		}
		if len(parts) > 0 {
			return parts[0]
		}
		return ""
	}, "split_regex")
}

// FindAll finds all occurrences of the pattern and returns their positions
func (so *StringOps) FindAll(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		return applyUnaryInt32Op(so.s, func(s string) int32 {
			return -1
		}, "find_all")
	}

	return applyUnaryInt32Op(so.s, func(s string) int32 {
		loc := re.FindStringIndex(s)
		if loc == nil {
			return -1
		}
		return int32(loc[0])
	}, "find_all")
}

// CountMatches counts the number of regex matches
func (so *StringOps) CountMatches(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		return applyUnaryInt32Op(so.s, func(s string) int32 {
			return 0
		}, "count_matches")
	}

	return applyUnaryInt32Op(so.s, func(s string) int32 {
		matches := re.FindAllString(s, -1)
		return int32(len(matches))
	}, "count_matches")
}
