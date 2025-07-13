# Build Issues Report for Golars

## Date: 2025-07-13

## Summary
The Golars library currently has compilation errors due to duplicate function declarations across multiple files. These issues prevent the library from building and running examples.

## Identified Issues

### 1. Duplicate I/O Function Declarations (Critical)

**Location**: Root package (`github.com/davidpalaitis/golars`)

**Files involved**:
- `io.go` (lines 13-31)
- `golars.go` (lines 184-200)

**Duplicate functions**:
- `ReadCSV` - declared in both files
- `WriteCSV` - declared in both files  
- `ReadParquet` - declared in both files
- `WriteParquet` - declared in both files

**Error message**:
```
io.go:13:6: ReadCSV redeclared in this block
    golars.go:184:6: other declaration of ReadCSV
```

**Root cause**: Both files are in the same package (`golars`) and declare functions with identical names, which is not allowed in Go.

**Recommended fix**: 
Remove the functions from `io.go` since `golars.go` already provides these as part of the main API. The `golars.go` versions properly delegate to the io subpackage.

### 2. Duplicate toFloat64Value Function (Secondary)

**Location**: Frame package (`github.com/davidpalaitis/golars/frame`)

**Files involved**:
- `rolling_join.go` (line 421)
- `interpolate.go` (line 230)

**Error message**:
```
frame/rolling_join.go:421:6: toFloat64Value redeclared in this block
    frame/interpolate.go:230:6: other declaration of toFloat64Value
```

**Recommended fix**: 
Rename one of the functions or move to a shared utility file if they serve the same purpose.

### 3. Additional I/O Related Errors

The `io.go` file also has undefined type errors:
- `undefined: csv.Option` (line 13)
- `undefined: csv.ReadCSV` (line 14) 
- `undefined: csv.WriteCSV` (line 19)
- `undefined: parquet.WriteOption` (line 30)

These errors suggest the imports or type definitions may be incorrect.

## Impact

1. **Cannot build the library** - No examples or tests can run
2. **Cannot import in other projects** - The compilation errors prevent usage
3. **Blocks testing** - Cannot verify the 75% feature parity claim

## Recommended Action Plan

1. **Immediate fix**: Remove or rename duplicate function declarations
2. **Architecture review**: Clarify the intended structure between:
   - Root package exports (`golars.go`)
   - Direct I/O exports (`io.go`) 
   - Subpackage implementations (`io/csv`, `io/parquet`)
3. **Add CI/CD**: Implement continuous integration to catch such issues early
4. **Test all examples**: Ensure all example programs compile and run

## Testing Attempted

Created a simple hello world example:
```go
package main

import (
    "fmt"
    "log"
    "github.com/davidpalaitis/golars"
)

func main() {
    df, err := golars.NewDataFrame(
        golars.NewSeries("greeting", []string{"Hello", "Hola", "Bonjour", "Ciao"}),
        golars.NewSeries("language", []string{"English", "Spanish", "French", "Italian"}),
        golars.NewSeries("formality", []int32{5, 4, 7, 3}),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Hello World DataFrame:")
    fmt.Println(df)
}
```

This example cannot run due to the compilation errors described above.

## Conclusion

While the documentation indicates 75% feature parity with Polars and the test suite appears comprehensive, the library cannot currently be used due to these build errors. These are straightforward fixes that should be addressed before further development.