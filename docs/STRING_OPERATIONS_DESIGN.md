# String Operations Design Document

## Overview

This document outlines the design and implementation of string operations for Golars, providing comprehensive text manipulation capabilities similar to Polars. String operations are critical for data analysis, cleaning, and transformation tasks.

## Goals

1. **Comprehensive Coverage**: Implement all essential string operations found in Polars
2. **Performance**: Leverage Go's efficient string handling and Arrow's string arrays
3. **Null Safety**: Proper null propagation throughout all operations
4. **Expression Integration**: All operations available through the expression API
5. **Type Safety**: Compile-time type checking where possible

## Architecture

### Package Structure

```
strings/
├── ops.go           // Core string operations
├── pattern.go       // Pattern matching (contains, startswith, etc.)
├── regex.go         // Regular expression operations
├── case.go          // Case transformation operations
├── parse.go         // String parsing to other types
├── format.go        // String formatting operations
├── split_join.go    // Split and join operations
├── trim.go          // Trimming and padding operations
├── benchmarks_test.go
└── doc.go
```

### Integration Points

1. **Series Level**: Direct methods on String series
2. **Expression Level**: String operations as expressions
3. **DataFrame Level**: Applied through select/with_column

## API Design

### Core String Operations

```go
// Series methods
type StringSeries interface {
    // Basic operations
    Length() Series                    // String length
    Concat(other Series) Series        // Concatenate strings
    Repeat(n int) Series              // Repeat string n times
    
    // Substring operations
    Slice(start, length int) Series    // Extract substring
    Left(n int) Series                // First n characters
    Right(n int) Series               // Last n characters
    
    // Transformation
    Replace(pattern, replacement string, n int) Series
    Reverse() Series
    
    // Null handling inherent in all operations
}

// Expression builders
func StrLength(expr Expr) Expr
func StrConcat(exprs ...Expr) Expr
func StrRepeat(expr Expr, n int) Expr
func StrSlice(expr Expr, start, length int) Expr
func StrReplace(expr Expr, pattern, replacement string, n int) Expr
```

### Pattern Matching

```go
// Pattern matching operations
func StrContains(expr Expr, pattern string, literal bool) Expr
func StrStartsWith(expr Expr, pattern string) Expr
func StrEndsWith(expr Expr, pattern string) Expr
func StrFind(expr Expr, pattern string) Expr  // Returns index or -1

// Series methods
type StringSeries interface {
    Contains(pattern string, literal bool) BooleanSeries
    StartsWith(pattern string) BooleanSeries
    EndsWith(pattern string) BooleanSeries
    Find(pattern string) Int32Series  // Index of first occurrence
}
```

### Regular Expression Operations

```go
// Regex operations
func StrExtract(expr Expr, pattern string, group int) Expr
func StrExtractAll(expr Expr, pattern string) Expr  // Returns List<String>
func StrMatch(expr Expr, pattern string) Expr       // Boolean result
func StrReplace(expr Expr, pattern string, replacement string) Expr
func StrSplit(expr Expr, pattern string) Expr       // Returns List<String>

// Regex configuration
type RegexOptions struct {
    CaseInsensitive bool
    Multiline       bool
    DotAll          bool
}

func StrExtractWithOptions(expr Expr, pattern string, group int, opts RegexOptions) Expr
```

### Case Operations

```go
// Case transformations
func StrToUpper(expr Expr) Expr
func StrToLower(expr Expr) Expr
func StrToTitle(expr Expr) Expr     // Title Case
func StrCapitalize(expr Expr) Expr  // First letter uppercase

// Series methods
type StringSeries interface {
    ToUpper() Series
    ToLower() Series
    ToTitle() Series
    Capitalize() Series
}
```

### Trimming and Padding

```go
// Trim operations
func StrTrim(expr Expr, chars ...string) Expr      // Trim both sides
func StrLTrim(expr Expr, chars ...string) Expr     // Left trim
func StrRTrim(expr Expr, chars ...string) Expr     // Right trim
func StrStrip(expr Expr) Expr                      // Strip whitespace

// Padding operations
func StrPad(expr Expr, width int, side string, fillchar string) Expr
func StrZFill(expr Expr, width int) Expr  // Pad with zeros

// Series methods
type StringSeries interface {
    Trim(chars ...string) Series
    LTrim(chars ...string) Series
    RTrim(chars ...string) Series
    Strip() Series
    Pad(width int, side string, fillchar string) Series
    ZFill(width int) Series
}
```

### String Parsing

```go
// Parse to other types
func StrToInt(expr Expr, base ...int) Expr
func StrToFloat(expr Expr) Expr
func StrToDate(expr Expr, format string) Expr
func StrToDateTime(expr Expr, format string) Expr
func StrToBool(expr Expr) Expr

// Series methods
type StringSeries interface {
    ToInt(base ...int) (Int64Series, error)
    ToFloat() (Float64Series, error)
    ToDate(format string) (DateSeries, error)
    ToDateTime(format string) (DateTimeSeries, error)
    ToBool() (BooleanSeries, error)
}
```

### Split and Join Operations

```go
// Split operations
func StrSplit(expr Expr, separator string) Expr         // Returns List<String>
func StrSplitN(expr Expr, separator string, n int) Expr // Split into n parts
func StrSplitWhitespace(expr Expr) Expr                 // Split on whitespace

// Join operations  
func StrJoin(expr Expr, separator string) Expr  // For List<String> -> String
func StrConcat(exprs ...Expr) Expr             // Concatenate multiple string columns

// Series methods
type StringSeries interface {
    Split(separator string) ListSeries
    SplitN(separator string, n int) ListSeries
    SplitWhitespace() ListSeries
}
```

### String Formatting

```go
// Format operations
func StrFormat(template string, exprs ...Expr) Expr  // Printf-style formatting
func StrInterpolate(template string, exprs map[string]Expr) Expr  // Named placeholders

// Series methods
type StringSeries interface {
    Format(template string, args ...Series) Series
}
```

## Implementation Details

### Efficient String Operations

```go
// Use Arrow's string array for efficient operations
func (s *stringChunkedArray) Length() *int32ChunkedArray {
    builder := array.NewInt32Builder(s.pool)
    defer builder.Release()
    
    for _, chunk := range s.chunks {
        strArray := chunk.(*array.String)
        for i := 0; i < strArray.Len(); i++ {
            if strArray.IsNull(i) {
                builder.AppendNull()
            } else {
                str := strArray.Value(i)
                builder.Append(int32(len(str)))
            }
        }
    }
    
    return newInt32ChunkedArrayFromBuilder(builder)
}
```

### Regex Compilation and Caching

```go
// Cache compiled regex patterns
type regexCache struct {
    mu       sync.RWMutex
    patterns map[string]*regexp.Regexp
    maxSize  int
}

var globalRegexCache = &regexCache{
    patterns: make(map[string]*regexp.Regexp),
    maxSize:  1000,
}

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
```

### Null Propagation

```go
// Consistent null handling across all operations
func (s *stringChunkedArray) Replace(pattern, replacement string, n int) *stringChunkedArray {
    builder := array.NewStringBuilder(s.pool)
    defer builder.Release()
    
    for _, chunk := range s.chunks {
        strArray := chunk.(*array.String)
        for i := 0; i < strArray.Len(); i++ {
            if strArray.IsNull(i) {
                builder.AppendNull()
            } else {
                str := strArray.Value(i)
                result := strings.Replace(str, pattern, replacement, n)
                builder.Append(result)
            }
        }
    }
    
    return newStringChunkedArrayFromBuilder(builder)
}
```

### UTF-8 Support

```go
// Proper UTF-8 handling for all operations
func (s *stringChunkedArray) RuneLength() *int32ChunkedArray {
    builder := array.NewInt32Builder(s.pool)
    defer builder.Release()
    
    for _, chunk := range s.chunks {
        strArray := chunk.(*array.String)
        for i := 0; i < strArray.Len(); i++ {
            if strArray.IsNull(i) {
                builder.AppendNull()
            } else {
                str := strArray.Value(i)
                builder.Append(int32(utf8.RuneCountInString(str)))
            }
        }
    }
    
    return newInt32ChunkedArrayFromBuilder(builder)
}
```

## Performance Considerations

### Optimization Strategies

1. **Batch Processing**: Process entire chunks at once
2. **Memory Reuse**: Use builders and preallocate when possible
3. **Lazy Evaluation**: Defer computation until needed
4. **Parallel Processing**: Process chunks concurrently for large datasets

### Benchmarks

```go
func BenchmarkStringOperations(b *testing.B) {
    sizes := []int{1000, 10000, 100000}
    
    for _, size := range sizes {
        data := generateStringData(size)
        series := NewStringSeries("test", data)
        
        b.Run(fmt.Sprintf("Length_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = series.Length()
            }
        })
        
        b.Run(fmt.Sprintf("Contains_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = series.Contains("test", true)
            }
        })
        
        b.Run(fmt.Sprintf("Replace_%d", size), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                _ = series.Replace("old", "new", -1)
            }
        })
    }
}
```

## Examples

### Basic String Manipulation

```go
// DataFrame with string operations
df := golars.NewDataFrame(
    golars.NewStringSeries("names", []string{"Alice", "Bob", "Charlie"}),
    golars.NewStringSeries("emails", []string{"alice@example.com", "bob@test.com", "charlie@demo.org"}),
)

// Extract domain from email
result := df.WithColumn("domain",
    golars.Col("emails").Str().Split("@").List().Get(1),
).WithColumn("name_upper",
    golars.Col("names").Str().ToUpper(),
).WithColumn("name_length",
    golars.Col("names").Str().Length(),
)
```

### Pattern Matching and Extraction

```go
// Extract information using regex
df.WithColumn("has_gmail",
    golars.Col("email").Str().Contains("@gmail.com", true),
).WithColumn("username",
    golars.Col("email").Str().Extract("^([^@]+)@", 1),
).WithColumn("tld",
    golars.Col("email").Str().Extract(r"\.([^.]+)$", 1),
)
```

### String Parsing

```go
// Parse strings to other types
df := golars.NewDataFrame(
    golars.NewStringSeries("numbers", []string{"123", "456", "789"}),
    golars.NewStringSeries("dates", []string{"2024-01-01", "2024-02-01", "2024-03-01"}),
)

result := df.WithColumn("parsed_int",
    golars.Col("numbers").Str().ToInt(),
).WithColumn("parsed_date",
    golars.Col("dates").Str().ToDate("2006-01-02"),
)
```

## Testing Strategy

### Unit Tests

1. **Basic Operations**: Test all string operations with various inputs
2. **Null Handling**: Verify null propagation
3. **UTF-8 Support**: Test with multi-byte characters
4. **Edge Cases**: Empty strings, special characters
5. **Performance**: Ensure operations scale linearly

### Integration Tests

1. **Expression Integration**: Test string operations in expressions
2. **DataFrame Integration**: Test with select/with_column
3. **Type Conversions**: Test parsing to other types
4. **Chaining**: Test multiple operations chained together

## Implementation Plan

1. **Phase 1**: Core string operations (Length, Concat, Slice, Replace)
2. **Phase 2**: Pattern matching (Contains, StartsWith, EndsWith)
3. **Phase 3**: Regex operations (Extract, Match, Split)
4. **Phase 4**: Case operations and trimming
5. **Phase 5**: Parsing and formatting
6. **Phase 6**: Integration and optimization

## Conclusion

String operations are essential for data manipulation in Golars. This design provides a comprehensive set of operations that match Polars' capabilities while leveraging Go's strengths and maintaining consistency with the rest of the Golars API.