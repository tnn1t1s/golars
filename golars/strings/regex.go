package strings

import (
	"regexp"
	"sync"

	"github.com/davidpalaitis/golars/series"
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
	// Check cache first
	globalRegexCache.mu.RLock()
	if re, ok := globalRegexCache.patterns[pattern]; ok {
		globalRegexCache.mu.RUnlock()
		return re, nil
	}
	globalRegexCache.mu.RUnlock()
	
	// Compile and cache
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
		// Return null series on error
		return series.NewStringSeries("match_error", make([]string, so.s.Len()))
	}
	
	return applyPatternOp(so.s, func(str string) bool {
		return re.MatchString(str)
	}, "match")
}

// Extract extracts the first match of the pattern (or specific group)
func (so *StringOps) Extract(pattern string, group int) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return null series on error
		return series.NewStringSeries("extract_error", make([]string, so.s.Len()))
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		matches := re.FindStringSubmatch(str)
		if matches == nil {
			return ""
		}
		
		// Return full match if group is 0 or out of range
		if group < 0 || group >= len(matches) {
			if len(matches) > 0 {
				return matches[0]
			}
			return ""
		}
		
		return matches[group]
	}, "extract")
}

// ExtractAll extracts all matches of the pattern
func (so *StringOps) ExtractAll(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return null series on error
		return series.NewStringSeries("extract_all_error", make([]string, so.s.Len()))
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		matches := re.FindAllString(str, -1)
		if len(matches) == 0 {
			return ""
		}
		// For now, return the first match
		// TODO: Return as List series when available
		return matches[0]
	}, "extract_all")
}

// ReplaceRegex replaces matches of the regex pattern with replacement
func (so *StringOps) ReplaceRegex(pattern, replacement string, n int) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return original series on error
		return so.s
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		if n < 0 {
			return re.ReplaceAllString(str, replacement)
		}
		
		// For limited replacements, we need to do it manually
		count := 0
		result := re.ReplaceAllStringFunc(str, func(match string) string {
			if count < n {
				count++
				return replacement
			}
			return match
		})
		return result
	}, "replace_regex")
}

// SplitRegex splits the string by regex pattern
func (so *StringOps) SplitRegex(pattern string, n int) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return original series on error
		return so.s
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		parts := re.Split(str, n)
		if len(parts) == 0 {
			return ""
		}
		// For now, return the first part
		// TODO: Return as List series when available
		return parts[0]
	}, "split_regex")
}

// FindAll finds all occurrences of the pattern and returns their positions
func (so *StringOps) FindAll(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return -1 series on error
		return applyUnaryOp(so.s, func(str string) interface{} {
			return int32(-1)
		}, "find_all_error")
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		indices := re.FindAllStringIndex(str, -1)
		if len(indices) == 0 {
			return int32(-1)
		}
		// Return the position of the first match
		return int32(indices[0][0])
	}, "find_all")
}

// CountMatches counts the number of regex matches
func (so *StringOps) CountMatches(pattern string) series.Series {
	re, err := compileRegex(pattern)
	if err != nil {
		// Return 0 series on error
		return applyUnaryOp(so.s, func(str string) interface{} {
			return int32(0)
		}, "count_matches_error")
	}
	
	return applyUnaryOp(so.s, func(str string) interface{} {
		matches := re.FindAllString(str, -1)
		return int32(len(matches))
	}, "count_matches")
}