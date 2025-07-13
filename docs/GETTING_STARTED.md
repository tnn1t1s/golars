# Getting Started with Golars

This guide will walk you through creating and running a simple "Hello World" example using the Golars library.

## Prerequisites

- Go 1.21 or higher installed
- Basic knowledge of Go programming

## Step 1: Create a New Directory for Your Project

Open a terminal and create a new directory for your example:

```bash
mkdir my-golars-example
cd my-golars-example
```

## Step 2: Initialize a Go Module

Initialize a new Go module in your directory:

```bash
go mod init my-golars-example
```

## Step 3: Create the Hello World Program

Create a new file called `main.go` with the following content:

```go
package main

import (
    "fmt"
    "log"
    "github.com/davidpalaitis/golars"
)

func main() {
    // Create a DataFrame with three columns
    df, err := golars.NewDataFrame(
        golars.NewStringSeries("greeting", []string{"Hello", "Hola", "Bonjour", "Ciao"}),
        golars.NewStringSeries("language", []string{"English", "Spanish", "French", "Italian"}),
        golars.NewInt32Series("formality", []int32{5, 4, 7, 3}),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Hello World DataFrame:")
    fmt.Println(df)
    
    // Filter the DataFrame to show only formal greetings (formality > 4)
    filtered, err := df.Filter(golars.Col("formality").Gt(4))
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("\nFormal Greetings (formality > 4):")
    fmt.Println(filtered)
    
    // Select specific columns
    selected := df.Select("greeting", "language")
    
    fmt.Println("\nJust Greetings and Languages:")
    fmt.Println(selected)
}
```

## Step 4: Get the Golars Dependency

Since Golars is a local project, you need to tell Go where to find it. Add a replace directive to your `go.mod`:

```bash
go mod edit -replace github.com/davidpalaitis/golars=../golars
```

Then get the dependency:

```bash
go get github.com/davidpalaitis/golars
```

## Step 5: Run the Program

Run your hello world example:

```bash
go run main.go
```

## Expected Output

You should see output similar to this:

```
Hello World DataFrame:
DataFrame: 4 × 3
┌─────────────┬─────────────┬─────────────┐
│ greeting    │ language    │ formality   │
│ str         │ str         │ i32         │
├─────────────┼─────────────┼─────────────┤
│ Hello       │ English     │ 5           │
│ Hola        │ Spanish     │ 4           │
│ Bonjour     │ French      │ 7           │
│ Ciao        │ Italian     │ 3           │
└─────────────┴─────────────┴─────────────┘

Formal Greetings (formality > 4):
DataFrame: 2 × 3
┌─────────────┬─────────────┬─────────────┐
│ greeting    │ language    │ formality   │
│ str         │ str         │ i32         │
├─────────────┼─────────────┼─────────────┤
│ Hello       │ English     │ 5           │
│ Bonjour     │ French      │ 7           │
└─────────────┴─────────────┴─────────────┘

Just Greetings and Languages:
DataFrame: 4 × 2
┌─────────────┬─────────────┐
│ greeting    │ language    │
│ str         │ str         │
├─────────────┼─────────────┤
│ Hello       │ English     │
│ Hola        │ Spanish     │
│ Bonjour     │ French      │
│ Ciao        │ Italian     │
└─────────────┴─────────────┘
```

## Common Series Types

Golars provides convenience functions for creating different types of series:

- `NewStringSeries(name string, values []string)` - For string data
- `NewInt32Series(name string, values []int32)` - For 32-bit integers
- `NewInt64Series(name string, values []int64)` - For 64-bit integers
- `NewFloat32Series(name string, values []float32)` - For 32-bit floats
- `NewFloat64Series(name string, values []float64)` - For 64-bit floats
- `NewBoolSeries(name string, values []bool)` - For boolean data

## Next Steps

Now that you have a working example, you can explore more features:

1. **Reading/Writing Files**:
   ```go
   // Read from CSV
   df, err := golars.ReadCSV("data.csv")
   
   // Write to Parquet
   err = golars.WriteParquet(df, "output.parquet")
   ```

2. **Aggregations**:
   ```go
   // Group by language and calculate mean formality
   grouped := df.GroupBy("language").Agg(
       golars.Col("formality").Mean().Alias("avg_formality"),
   )
   ```

3. **Joins**:
   ```go
   // Join two DataFrames
   joined, err := df1.Join(df2, []string{"key_column"}, golars.InnerJoin)
   ```

4. **Window Functions**:
   ```go
   // Add row numbers partitioned by language
   windowed := df.WithColumn("row_num", 
       golars.RowNumber().Over(golars.NewSpec().PartitionBy("language")),
   )
   ```

## Troubleshooting

If you encounter any issues:

1. Make sure you're using Go 1.21 or higher: `go version`
2. Ensure the path in the replace directive points to the correct location of the Golars repository
3. Check that the Golars library builds successfully: `cd ../golars && go build`

## Documentation

For more detailed documentation and examples, check:
- The `docs/` directory in the Golars repository
- The roadmap at `docs/ROADMAP_JULY_2025.md` for feature availability
- Example programs in `cmd/hello/` for a working example