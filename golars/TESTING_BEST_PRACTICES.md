# Testing Best Practices for Golars

This guide outlines modern Go testing practices that should be adopted in the Golars project to replace outdated testing patterns.

## Table of Contents
1. [Current Issues](#current-issues)
2. [Core Testing Principles](#core-testing-principles)
3. [Table-Driven Tests](#table-driven-tests)
4. [Example Tests](#example-tests)
5. [Test Helpers](#test-helpers)
6. [Parallel Testing](#parallel-testing)
7. [Benchmarking](#benchmarking)
8. [Fuzzing](#fuzzing)
9. [Integration Testing](#integration-testing)
10. [Test Organization](#test-organization)
11. [Running Tests](#running-tests)
12. [Code Examples](#code-examples)

## Current Issues

The current Golars test suite exhibits several outdated patterns:

- ❌ Manual mock objects instead of interface-based design
- ❌ Individual test cases instead of table-driven tests
- ❌ No example tests for documentation
- ❌ Limited use of subtests (`t.Run()`)
- ❌ Repetitive setup code without helpers
- ❌ No fuzzing or property-based testing
- ❌ No parallel test execution
- ❌ Assertions via external library instead of standard library

## Core Testing Principles

### 1. Test Behavior, Not Implementation
Focus on what the code does, not how it does it.

### 2. Make Tests Readable
Tests serve as documentation. A developer should understand what's being tested without reading the implementation.

### 3. Keep Tests Independent
Each test should be runnable in isolation without depending on other tests.

### 4. Use the Standard Library
Prefer Go's built-in testing package over external assertion libraries when possible.

## Table-Driven Tests

Table-driven tests are the idiomatic way to test multiple scenarios in Go.

### ❌ Bad: Individual Test Functions
```go
func TestSeriesGetFirst(t *testing.T) {
    s := NewFloat64Series("floats", []float64{1.1, 2.2, 3.3})
    assert.Equal(t, 1.1, s.Get(0))
}

func TestSeriesGetSecond(t *testing.T) {
    s := NewFloat64Series("floats", []float64{1.1, 2.2, 3.3})
    assert.Equal(t, 2.2, s.Get(1))
}

func TestSeriesGetOutOfBounds(t *testing.T) {
    s := NewFloat64Series("floats", []float64{1.1, 2.2, 3.3})
    assert.Nil(t, s.Get(5))
}
```

### ✅ Good: Table-Driven Test
```go
func TestSeries_Get(t *testing.T) {
    tests := []struct {
        name     string
        series   Series
        index    int
        want     interface{}
        wantNil  bool
    }{
        {
            name:   "valid first index",
            series: NewFloat64Series("floats", []float64{1.1, 2.2, 3.3}),
            index:  0,
            want:   1.1,
        },
        {
            name:   "valid middle index",
            series: NewFloat64Series("floats", []float64{1.1, 2.2, 3.3}),
            index:  1,
            want:   2.2,
        },
        {
            name:    "out of bounds positive",
            series:  NewFloat64Series("floats", []float64{1.1}),
            index:   5,
            wantNil: true,
        },
        {
            name:    "negative index",
            series:  NewInt32Series("ints", []int32{1, 2, 3}),
            index:   -1,
            wantNil: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := tt.series.Get(tt.index)
            
            if tt.wantNil {
                if got != nil {
                    t.Errorf("Get(%d) = %v, want nil", tt.index, got)
                }
                return
            }
            
            if got != tt.want {
                t.Errorf("Get(%d) = %v, want %v", tt.index, got, tt.want)
            }
        })
    }
}
```

## Example Tests

Example tests serve as executable documentation and appear in generated Go documentation.

```go
func ExampleNewDataFrame() {
    df, err := golars.NewDataFrame(
        golars.NewStringSeries("name", []string{"Alice", "Bob"}),
        golars.NewInt32Series("age", []int32{25, 30}),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Rows: %d, Columns: %d\n", df.Height(), df.Width())
    // Output: Rows: 2, Columns: 2
}

func ExampleDataFrame_Select() {
    df, _ := golars.NewDataFrame(
        golars.NewStringSeries("name", []string{"Alice", "Bob"}),
        golars.NewInt32Series("age", []int32{25, 30}),
        golars.NewFloat64Series("score", []float64{92.5, 88.0}),
    )
    
    selected, _ := df.Select("name", "score")
    fmt.Printf("Selected %d columns\n", selected.Width())
    // Output: Selected 2 columns
}

func ExampleCol() {
    // Create an expression for age > 25
    expr := golars.Col("age").Gt(golars.Lit(25))
    fmt.Println(expr.String())
    // Output: col(age) > lit(25)
}
```

## Test Helpers

Extract common setup code into helper functions using `t.Helper()`.

```go
// testutil/helpers.go
package testutil

import (
    "testing"
    "github.com/davidpalaitis/golars"
)

// CreateTestDataFrame creates a standard DataFrame for testing
func CreateTestDataFrame(t *testing.T, size int) *golars.DataFrame {
    t.Helper() // Errors will show caller's line number
    
    names := make([]string, size)
    ages := make([]int32, size)
    scores := make([]float64, size)
    
    for i := 0; i < size; i++ {
        names[i] = fmt.Sprintf("Person_%d", i)
        ages[i] = int32(20 + i%50)
        scores[i] = float64(50 + i%50)
    }
    
    df, err := golars.NewDataFrame(
        golars.NewStringSeries("name", names),
        golars.NewInt32Series("age", ages),
        golars.NewFloat64Series("score", scores),
    )
    if err != nil {
        t.Fatalf("failed to create test DataFrame: %v", err)
    }
    
    return df
}

// AssertDataFrameEqual compares two DataFrames
func AssertDataFrameEqual(t *testing.T, want, got *golars.DataFrame) {
    t.Helper()
    
    if want.Height() != got.Height() {
        t.Errorf("height mismatch: want %d, got %d", want.Height(), got.Height())
        return
    }
    
    if want.Width() != got.Width() {
        t.Errorf("width mismatch: want %d, got %d", want.Width(), got.Width())
        return
    }
    
    // Compare column by column
    for i := 0; i < want.Width(); i++ {
        wantCol := want.Column(i)
        gotCol := got.Column(i)
        
        AssertSeriesEqual(t, wantCol, gotCol)
    }
}
```

## Parallel Testing

Use `t.Parallel()` to run independent tests concurrently.

```go
func TestDataFrame_Operations(t *testing.T) {
    // Create shared test data
    df := testutil.CreateTestDataFrame(t, 100)
    
    // Each subtest can run in parallel
    t.Run("Select", func(t *testing.T) {
        t.Parallel()
        
        selected, err := df.Select("name", "age")
        if err != nil {
            t.Fatalf("Select failed: %v", err)
        }
        
        if selected.Width() != 2 {
            t.Errorf("expected 2 columns, got %d", selected.Width())
        }
    })
    
    t.Run("Head", func(t *testing.T) {
        t.Parallel()
        
        head := df.Head(10)
        if head.Height() != 10 {
            t.Errorf("expected 10 rows, got %d", head.Height())
        }
    })
    
    t.Run("Tail", func(t *testing.T) {
        t.Parallel()
        
        tail := df.Tail(10)
        if tail.Height() != 10 {
            t.Errorf("expected 10 rows, got %d", tail.Height())
        }
    })
}
```

## Benchmarking

Write benchmarks to measure performance and detect regressions.

```go
func BenchmarkDataFrame_Creation(b *testing.B) {
    sizes := []int{10, 100, 1000, 10000}
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            // Prepare data outside benchmark loop
            names := make([]string, size)
            ages := make([]int32, size)
            for i := 0; i < size; i++ {
                names[i] = fmt.Sprintf("Person_%d", i)
                ages[i] = int32(20 + i%50)
            }
            
            // Reset timer after setup
            b.ResetTimer()
            
            // Run the benchmark
            for i := 0; i < b.N; i++ {
                _, err := golars.NewDataFrame(
                    golars.NewStringSeries("name", names),
                    golars.NewInt32Series("age", ages),
                )
                if err != nil {
                    b.Fatal(err)
                }
            }
        })
    }
}

func BenchmarkDataFrame_Select(b *testing.B) {
    df := createLargeDataFrame(10000) // Helper function
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, err := df.Select("col1", "col5", "col10")
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

## Fuzzing

Use Go's built-in fuzzing (Go 1.18+) to find edge cases automatically.

```go
func FuzzExpressionParser(f *testing.F) {
    // Add seed corpus - valid expressions
    f.Add("col(age)")
    f.Add("col(name).gt(25)")
    f.Add("col(a).add(col(b))")
    f.Add("when(col(x)).then(1).otherwise(0)")
    
    f.Fuzz(func(t *testing.T, input string) {
        // Try to parse the expression
        expr, err := ParseExpression(input)
        if err != nil {
            // Invalid input is expected during fuzzing
            return
        }
        
        // If it parsed successfully, verify properties
        
        // Property 1: Should produce non-empty string
        str := expr.String()
        if str == "" {
            t.Error("parsed expression produced empty string")
        }
        
        // Property 2: Should have a data type
        dt := expr.DataType()
        if dt == nil {
            t.Error("parsed expression has nil data type")
        }
    })
}

func FuzzSeries_Get(f *testing.F) {
    // Test that Get() doesn't panic for any index
    f.Add(0)
    f.Add(1)
    f.Add(-1)
    f.Add(1000000)
    
    f.Fuzz(func(t *testing.T, index int) {
        s := golars.NewInt32Series("test", []int32{1, 2, 3})
        
        // Should not panic
        val := s.Get(index)
        
        // Verify bounds checking
        if index < 0 || index >= s.Len() {
            if val != nil {
                t.Errorf("Get(%d) = %v, expected nil for out of bounds", index, val)
            }
        }
    })
}
```

## Integration Testing

Use build tags to separate integration tests from unit tests.

```go
//go:build integration
// +build integration

package golars_test

import (
    "testing"
    "github.com/davidpalaitis/golars"
)

func TestCSVToParquetPipeline(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test in short mode")
    }
    
    // Use temp directory for test files
    tmpDir := t.TempDir()
    
    // Test complete workflow
    t.Run("full pipeline", func(t *testing.T) {
        // 1. Create test CSV
        csvPath := filepath.Join(tmpDir, "input.csv")
        createTestCSV(t, csvPath, 1000)
        
        // 2. Read CSV
        df, err := golars.ReadCSV(csvPath)
        if err != nil {
            t.Fatalf("failed to read CSV: %v", err)
        }
        
        // 3. Transform
        transformed, err := df.
            Filter(golars.Col("age").Gt(golars.Lit(25))).
            Select("name", "age", "score")
        if err != nil {
            t.Fatalf("transformation failed: %v", err)
        }
        
        // 4. Write Parquet
        parquetPath := filepath.Join(tmpDir, "output.parquet")
        err = transformed.WriteParquet(parquetPath)
        if err != nil {
            t.Fatalf("failed to write Parquet: %v", err)
        }
        
        // 5. Verify
        result, err := golars.ReadParquet(parquetPath)
        if err != nil {
            t.Fatalf("failed to read Parquet: %v", err)
        }
        
        testutil.AssertDataFrameEqual(t, transformed, result)
    })
}
```

## Test Organization

### Directory Structure
```
golars/
├── dataframe.go
├── dataframe_test.go           # Unit tests
├── dataframe_example_test.go   # Example tests
├── dataframe_bench_test.go     # Benchmarks
├── export_test.go              # Export internal functions for testing
├── testutil/
│   ├── helpers.go              # Common test helpers
│   ├── fixtures.go             # Test data generators
│   └── assertions.go           # Custom assertions
├── integration/
│   ├── csv_test.go             # CSV integration tests
│   ├── parquet_test.go         # Parquet integration tests
│   └── workflow_test.go        # End-to-end workflows
└── testdata/
    ├── sample.csv              # Test data files
    ├── sample.parquet
    └── expected/               # Expected output files
```

### File Naming Conventions
- `*_test.go` - Standard test files
- `*_example_test.go` - Example tests
- `*_bench_test.go` - Benchmarks
- `export_test.go` - Export unexported functions for testing

## Running Tests

### Basic Commands
```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run specific package
go test -v ./frame

# Run specific test
go test -v -run TestDataFrame_Select ./frame

# Run with race detector
go test -race ./...

# Run short tests only (skip integration)
go test -short ./...
```

### Coverage
```bash
# Run with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Coverage for specific package
go test -cover -coverprofile=coverage.out ./frame
go tool cover -html=coverage.out -o frame_coverage.html
```

### Benchmarks
```bash
# Run all benchmarks
go test -bench=. ./...

# Run specific benchmark
go test -bench=BenchmarkDataFrame_Creation ./frame

# Run benchmarks with memory profiling
go test -bench=. -benchmem ./...

# Compare benchmark results
go test -bench=. -count=10 ./... > old.txt
# ... make changes ...
go test -bench=. -count=10 ./... > new.txt
benchstat old.txt new.txt
```

### Fuzzing
```bash
# Run fuzzing for specific function
go test -fuzz=FuzzExpressionParser -fuzztime=30s ./expr

# Run with specific number of workers
go test -fuzz=FuzzExpressionParser -parallel=8 ./expr

# Run fuzzing with corpus from previous runs
go test -fuzz=FuzzExpressionParser ./expr
```

### Integration Tests
```bash
# Run integration tests
go test -tags=integration ./...

# Skip integration tests
go test -short ./...
```

## Code Examples

### Complete Test File Example

```go
package frame_test

import (
    "fmt"
    "testing"
    
    "github.com/davidpalaitis/golars"
    "github.com/davidpalaitis/golars/testutil"
)

// Test the main functionality with table-driven tests
func TestDataFrame_Filter(t *testing.T) {
    df := testutil.CreateTestDataFrame(t, 10)
    
    tests := []struct {
        name      string
        expr      golars.Expr
        wantRows  int
        wantError bool
    }{
        {
            name:     "filter age > 25",
            expr:     golars.Col("age").Gt(golars.Lit(25)),
            wantRows: 5,
        },
        {
            name:     "filter age == 30",
            expr:     golars.Col("age").Eq(golars.Lit(30)),
            wantRows: 1,
        },
        {
            name:      "filter invalid column",
            expr:      golars.Col("invalid").Gt(golars.Lit(0)),
            wantError: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            
            filtered, err := df.Filter(tt.expr)
            
            if tt.wantError {
                if err == nil {
                    t.Error("expected error but got none")
                }
                return
            }
            
            if err != nil {
                t.Errorf("unexpected error: %v", err)
                return
            }
            
            if got := filtered.Height(); got != tt.wantRows {
                t.Errorf("got %d rows, want %d", got, tt.wantRows)
            }
        })
    }
}

// Example showing how to filter a DataFrame
func ExampleDataFrame_Filter() {
    df, _ := golars.NewDataFrame(
        golars.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
        golars.NewInt32Series("age", []int32{25, 30, 35}),
    )
    
    // Filter age > 26
    filtered, _ := df.Filter(golars.Col("age").Gt(golars.Lit(26)))
    
    fmt.Printf("Filtered to %d rows\n", filtered.Height())
    // Output: Filtered to 2 rows
}

// Benchmark filtering with different DataFrame sizes
func BenchmarkDataFrame_Filter(b *testing.B) {
    sizes := []int{100, 1000, 10000}
    
    for _, size := range sizes {
        df := testutil.CreateTestDataFrame(b, size)
        expr := golars.Col("age").Gt(golars.Lit(25))
        
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            b.ResetTimer()
            
            for i := 0; i < b.N; i++ {
                _, err := df.Filter(expr)
                if err != nil {
                    b.Fatal(err)
                }
            }
        })
    }
}

// Test unexported functionality using export_test.go
func TestDataFrame_internalState(t *testing.T) {
    df := testutil.CreateTestDataFrame(t, 5)
    
    // Access internal state through exported test functions
    state := GetInternalState(df)
    
    if state == nil {
        t.Fatal("internal state is nil")
    }
    
    // Verify internal consistency
    if !state.IsConsistent() {
        t.Error("internal state is inconsistent")
    }
}
```

## Best Practices Summary

1. **Use table-driven tests** for testing multiple scenarios
2. **Write example tests** for documentation
3. **Create test helpers** with `t.Helper()` to reduce duplication
4. **Run tests in parallel** when possible with `t.Parallel()`
5. **Use subtests** with `t.Run()` for better organization
6. **Write benchmarks** for performance-critical code
7. **Add fuzzing** for parsers and input validation
8. **Separate integration tests** with build tags
9. **Use `t.TempDir()`** for temporary test files
10. **Prefer standard library** over external assertion libraries
11. **Test behavior**, not implementation details
12. **Keep tests independent** and runnable in isolation

By following these practices, Golars tests will be more maintainable, readable, and efficient while serving as excellent documentation for the library.