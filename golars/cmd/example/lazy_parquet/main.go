package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/davidpalaitis/golars"
)

func main() {
	// First, create a sample Parquet file with some data
	if err := createSampleParquetFile("sample_data.parquet"); err != nil {
		log.Fatal(err)
	}
	defer os.Remove("sample_data.parquet")

	fmt.Println("=== Golars Lazy Parquet Example ===\n")

	// Example 1: Basic lazy scan
	fmt.Println("1. Basic Lazy Parquet Scan:")
	lf := golars.ScanParquet("sample_data.parquet")
	df, err := lf.Collect()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(df)
	fmt.Println()

	// Example 2: Column selection (projection pushdown)
	fmt.Println("2. Lazy Scan with Column Selection:")
	lf2 := golars.ScanParquet("sample_data.parquet").
		SelectColumns("customer_id", "product", "amount")
	
	// Show the optimized plan
	plan, _ := lf2.ExplainOptimized()
	fmt.Println("Optimized Plan:")
	fmt.Println(plan)
	
	df2, err := lf2.Collect()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nResult:")
	fmt.Println(df2)
	fmt.Println()

	// Example 3: Filter pushdown
	fmt.Println("3. Lazy Scan with Filter (Predicate Pushdown):")
	lf3 := golars.ScanParquet("sample_data.parquet").
		Filter(golars.ColBuilder("amount").Gt(100).Build()).
		SelectColumns("customer_id", "product", "amount")
	
	// Show the optimized plan
	plan3, _ := lf3.ExplainOptimized()
	fmt.Println("Optimized Plan:")
	fmt.Println(plan3)
	
	df3, err := lf3.Collect()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nResult:")
	fmt.Println(df3)
	fmt.Println()

	// Example 4: Complex query with aggregation
	fmt.Println("4. Complex Query with GroupBy and Aggregation:")
	lf4 := golars.ScanParquet("sample_data.parquet").
		Filter(golars.ColBuilder("status").Eq(golars.Lit("completed")).Build()).
		GroupBy("category").
		Agg(map[string]golars.Expr{
			"total_amount": golars.ColBuilder("amount").Sum().Build(),
			"avg_amount":   golars.ColBuilder("amount").Mean().Build(),
			"order_count":  golars.ColBuilder("category").Count().Build(),
		}).
		Sort("total_amount", true)
	
	// Show the optimized plan
	plan4, _ := lf4.ExplainOptimized()
	fmt.Println("Optimized Plan:")
	fmt.Println(plan4)
	
	df4, err := lf4.Collect()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nResult:")
	fmt.Println(df4)
	fmt.Println()

	// Example 5: Performance comparison
	fmt.Println("5. Performance Comparison (Lazy vs Eager):")
	
	// Eager evaluation (read everything first)
	start := time.Now()
	dfEager, err := golars.ReadParquet("sample_data.parquet")
	if err != nil {
		log.Fatal(err)
	}
	dfEager, err = dfEager.Filter(golars.ColBuilder("amount").Gt(50).Build())
	if err != nil {
		log.Fatal(err)
	}
	dfEager, err = dfEager.Select("customer_id", "amount")
	if err != nil {
		log.Fatal(err)
	}
	eagerTime := time.Since(start)
	
	// Lazy evaluation (optimized)
	start = time.Now()
	lfLazy := golars.ScanParquet("sample_data.parquet").
		Filter(golars.ColBuilder("amount").Gt(50).Build()).
		SelectColumns("customer_id", "amount")
	dfLazy, err := lfLazy.Collect()
	if err != nil {
		log.Fatal(err)
	}
	lazyTime := time.Since(start)
	
	fmt.Printf("Eager evaluation time: %v\n", eagerTime)
	fmt.Printf("Lazy evaluation time:  %v\n", lazyTime)
	fmt.Printf("Rows returned: %d\n", dfLazy.Height())
	
	// Note: For small files, the difference might be minimal, but for large files
	// with selective filters and projections, lazy evaluation can be much faster
}

func createSampleParquetFile(filename string) error {
	// Create sample e-commerce transaction data
	size := 10000
	customerIDs := make([]int32, size)
	products := make([]string, size)
	amounts := make([]float64, size)
	categories := make([]string, size)
	statuses := make([]string, size)
	
	productNames := []string{"Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones", "Camera"}
	categoryNames := []string{"Electronics", "Computing", "Audio", "Accessories"}
	statusNames := []string{"completed", "pending", "cancelled"}
	
	for i := 0; i < size; i++ {
		customerIDs[i] = int32(1000 + (i % 500))
		products[i] = productNames[i%len(productNames)]
		amounts[i] = float64(10 + (i%500))
		categories[i] = categoryNames[i%len(categoryNames)]
		statuses[i] = statusNames[i%len(statusNames)]
	}
	
	df, err := golars.NewDataFrame(
		golars.NewInt32Series("customer_id", customerIDs),
		golars.NewStringSeries("product", products),
		golars.NewFloat64Series("amount", amounts),
		golars.NewStringSeries("category", categories),
		golars.NewStringSeries("status", statuses),
	)
	if err != nil {
		return err
	}
	
	// Write with compression for better demonstration of Parquet benefits
	return golars.WriteParquet(df, filename, golars.WithCompression(golars.CompressionSnappy))
}