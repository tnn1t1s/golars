// +build ignore

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/davidpalaitis/golars"
)

func main() {
	fmt.Println("Golars Parquet I/O Example")
	fmt.Println("=========================")

	// Create a sample DataFrame
	df, err := golars.NewDataFrame(
		golars.NewInt64Series("id", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		golars.NewStringSeries("name", []string{
			"Alice", "Bob", "Charlie", "David", "Eve",
			"Frank", "Grace", "Henry", "Iris", "Jack",
		}),
		golars.NewFloat64Series("score", []float64{
			95.5, 87.3, 92.1, 78.9, 88.4,
			91.2, 85.6, 93.7, 79.8, 90.1,
		}),
		golars.NewStringSeries("department", []string{
			"Engineering", "Sales", "Engineering", "HR", "Sales",
			"Engineering", "HR", "Sales", "Engineering", "HR",
		}),
		golars.NewBooleanSeries("active", []bool{
			true, true, false, true, false,
			true, true, false, true, true,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nOriginal DataFrame:")
	fmt.Println(df)

	// Example 1: Basic Write and Read
	fmt.Println("\n1. Basic Parquet Write/Read")
	fmt.Println("--------------------------")

	// Write to Parquet with default options (Snappy compression)
	err = golars.WriteParquet(df, "employees.parquet")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Written to employees.parquet")

	// Read back the full file
	dfRead, err := golars.ReadParquet("employees.parquet")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nRead back from Parquet:")
	fmt.Println(dfRead)

	// Example 2: Different Compression Types
	fmt.Println("\n2. Compression Comparison")
	fmt.Println("------------------------")

	compressionTypes := []struct {
		name        string
		compression golars.CompressionType
		filename    string
	}{
		{"None", golars.CompressionNone, "employees_none.parquet"},
		{"Snappy", golars.CompressionSnappy, "employees_snappy.parquet"},
		{"Gzip", golars.CompressionGzip, "employees_gzip.parquet"},
	}

	for _, ct := range compressionTypes {
		start := time.Now()
		err = golars.WriteParquet(df, ct.filename,
			golars.WithCompression(ct.compression))
		if err != nil {
			log.Printf("Failed to write with %s compression: %v", ct.name, err)
			continue
		}
		elapsed := time.Since(start)

		// Get file size
		info, _ := os.Stat(ct.filename)
		fmt.Printf("%-10s: %6d bytes, written in %v\n", 
			ct.name, info.Size(), elapsed)
	}

	// Example 3: Column Selection
	fmt.Println("\n3. Selective Column Reading")
	fmt.Println("--------------------------")

	// Read only specific columns
	dfSubset, err := golars.ReadParquet("employees.parquet",
		golars.WithParquetColumns([]string{"id", "name", "score"}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Reading only id, name, and score columns:")
	fmt.Println(dfSubset)

	// Example 4: Row Limiting
	fmt.Println("\n4. Reading Limited Rows")
	fmt.Println("----------------------")

	// Read only first 5 rows
	dfLimited, err := golars.ReadParquet("employees.parquet",
		golars.WithNumRows(5))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Reading only first 5 rows:")
	fmt.Println(dfLimited)

	// Example 5: Working with Nulls
	fmt.Println("\n5. Handling Null Values")
	fmt.Println("----------------------")

	// Create DataFrame with nulls
	dfWithNulls, err := golars.NewDataFrame(
		golars.NewSeriesWithValidity("id", []int32{1, 2, 3, 4, 5},
			[]bool{false, false, true, false, true}, golars.Int32),
		golars.NewSeriesWithValidity("value", []float64{10.5, 20.3, 0, 15.7, 0},
			[]bool{false, false, true, false, true}, golars.Float64),
		golars.NewSeriesWithValidity("notes", []string{"Good", "Fair", "", "Excellent", ""},
			[]bool{false, false, true, false, true}, golars.String),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("DataFrame with nulls:")
	fmt.Println(dfWithNulls)

	// Write and read back
	err = golars.WriteParquet(dfWithNulls, "data_with_nulls.parquet")
	if err != nil {
		log.Fatal(err)
	}

	dfNullsRead, err := golars.ReadParquet("data_with_nulls.parquet")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nRead back (nulls preserved):")
	fmt.Println(dfNullsRead)

	// Example 6: Large Dataset Performance
	fmt.Println("\n6. Performance with Larger Dataset")
	fmt.Println("---------------------------------")

	// Create a larger dataset
	size := 100000
	ids := make([]int64, size)
	values := make([]float64, size)
	categories := make([]string, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		values[i] = float64(i) * 1.5 + 100
		categories[i] = []string{"A", "B", "C", "D", "E"}[i%5]
	}

	largeDf, err := golars.NewDataFrame(
		golars.NewInt64Series("id", ids),
		golars.NewFloat64Series("value", values),
		golars.NewStringSeries("category", categories),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created DataFrame with %d rows\n", largeDf.Height())

	// Write to Parquet
	start := time.Now()
	err = golars.WriteParquet(largeDf, "large_dataset.parquet",
		golars.WithRowGroupSize(50*1024*1024)) // 50MB row groups
	if err != nil {
		log.Fatal(err)
	}
	writeTime := time.Since(start)

	// Write to CSV for comparison
	start = time.Now()
	err = golars.WriteCSV(largeDf, "large_dataset.csv")
	if err != nil {
		log.Fatal(err)
	}
	csvWriteTime := time.Since(start)

	parquetInfo, _ := os.Stat("large_dataset.parquet")
	csvInfo, _ := os.Stat("large_dataset.csv")

	fmt.Printf("\nWrite Performance:\n")
	fmt.Printf("  Parquet: %v (size: %d bytes)\n", writeTime, parquetInfo.Size())
	fmt.Printf("  CSV:     %v (size: %d bytes)\n", csvWriteTime, csvInfo.Size())
	fmt.Printf("  Compression ratio: %.2f%%\n", 
		float64(parquetInfo.Size())/float64(csvInfo.Size())*100)

	// Read performance
	start = time.Now()
	_, err = golars.ReadParquet("large_dataset.parquet")
	if err != nil {
		log.Fatal(err)
	}
	parquetReadTime := time.Since(start)

	start = time.Now()
	_, err = golars.ReadCSV("large_dataset.csv")
	if err != nil {
		log.Fatal(err)
	}
	csvReadTime := time.Since(start)

	fmt.Printf("\nRead Performance:\n")
	fmt.Printf("  Parquet: %v\n", parquetReadTime)
	fmt.Printf("  CSV:     %v\n", csvReadTime)
	fmt.Printf("  Speedup: %.2fx\n", float64(csvReadTime)/float64(parquetReadTime))

	// Example 7: Advanced Options
	fmt.Println("\n7. Advanced Write Options")
	fmt.Println("------------------------")

	// Write with custom options
	err = golars.WriteParquet(df, "employees_custom.parquet",
		golars.WithCompression(golars.CompressionGzip),
		golars.WithCompressionLevel(9),          // Maximum compression
		golars.WithRowGroupSize(10*1024*1024),   // 10MB row groups
		golars.WithPageSize(512*1024),           // 512KB pages
		golars.WithDictionary(true),             // Enable dictionary encoding
	)
	if err != nil {
		log.Fatal(err)
	}

	info, _ := os.Stat("employees_custom.parquet")
	fmt.Printf("Written with custom options: %d bytes\n", info.Size())

	// Clean up example files
	fmt.Println("\nCleaning up example files...")
	os.Remove("employees.parquet")
	os.Remove("employees_none.parquet")
	os.Remove("employees_snappy.parquet")
	os.Remove("employees_gzip.parquet")
	os.Remove("employees_custom.parquet")
	os.Remove("data_with_nulls.parquet")
	os.Remove("large_dataset.parquet")
	os.Remove("large_dataset.csv")
	
	fmt.Println("\nParquet example completed!")
}