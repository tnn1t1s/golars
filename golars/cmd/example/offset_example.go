package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create sample data for time series analysis
	df, err := golars.NewDataFrame(
		golars.NewStringSeries("date", []string{
			"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
			"2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10",
		}),
		golars.NewInt32Series("sales", []int32{
			100, 120, 110, 130, 140,
			90, 95, 150, 160, 155,
		}),
		golars.NewStringSeries("product", []string{
			"A", "A", "B", "B", "A",
			"B", "A", "A", "B", "B",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: LAG and LEAD functions
	fmt.Println("=== LAG and LEAD Functions ===")
	
	result := df
	
	// Previous day's sales (LAG)
	result, err = result.WithColumn("prev_sales",
		golars.Lag("sales", 1, -1).Over(
			golars.Window().OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	// Next day's sales (LEAD)
	result, err = result.WithColumn("next_sales",
		golars.Lead("sales", 1, -1).Over(
			golars.Window().OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	// 2-day lag
	result, err = result.WithColumn("sales_lag2",
		golars.Lag("sales", 2, 0).Over(
			golars.Window().OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(result.Select("date", "sales", "prev_sales", "next_sales", "sales_lag2"))
	fmt.Println()
	
	// Example 2: FIRST_VALUE and LAST_VALUE
	fmt.Println("=== FIRST_VALUE and LAST_VALUE ===")
	
	valueResult := df
	
	// First and last sales in the entire dataset
	valueResult, err = valueResult.WithColumn("first_sales",
		golars.FirstValue("sales").Over(
			golars.Window().OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	valueResult, err = valueResult.WithColumn("last_sales",
		golars.LastValue("sales").Over(
			golars.Window().OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(valueResult.Select("date", "sales", "first_sales", "last_sales"))
	fmt.Println()
	
	// Example 3: Partitioned offset functions
	fmt.Println("=== Partitioned Offset Functions ===")
	
	partResult := df
	
	// LAG within each product
	partResult, err = partResult.WithColumn("prev_product_sales",
		golars.Lag("sales", 1, 0).Over(
			golars.Window().
				PartitionBy("product").
				OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	// First value within each product
	partResult, err = partResult.WithColumn("first_product_sales",
		golars.FirstValue("sales").Over(
			golars.Window().
				PartitionBy("product").
				OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(partResult.Select("date", "product", "sales", "prev_product_sales", "first_product_sales"))
	fmt.Println()
	
	// Example 4: Calculate day-over-day change
	fmt.Println("=== Day-over-Day Change Analysis ===")
	
	changeResult := df
	
	// Previous sales
	changeResult, err = changeResult.WithColumn("prev_sales",
		golars.Lag("sales", 1, 0).Over(
			golars.Window().OrderBy("date")))
	if err != nil {
		log.Fatal(err)
	}
	
	// Note: In a full implementation, we'd calculate the change percentage
	// For now, just show the lag values
	fmt.Println(changeResult.Select("date", "sales", "prev_sales"))
}