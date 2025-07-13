package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create sample time series data
	df, err := golars.NewDataFrame(
		golars.NewInt32Series("day", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		golars.NewInt32Series("sales", []int32{100, 150, 120, 180, 200, 160, 140, 190, 210, 185}),
		golars.NewFloat64Series("temperature", []float64{20.5, 22.1, 21.3, 23.5, 24.2, 22.8, 21.9, 23.7, 25.1, 24.3}),
		golars.NewStringSeries("store", []string{"A", "A", "A", "B", "B", "B", "A", "A", "B", "B"}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Running totals and averages
	fmt.Println("=== Running Totals and Averages ===")
	
	result := df
	
	// Running total of sales
	result, err = result.WithColumn("running_total",
		golars.Sum("sales").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	// Running average of sales
	result, err = result.WithColumn("running_avg",
		golars.Avg("sales").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	// Running min and max
	result, err = result.WithColumn("running_min",
		golars.Min("sales").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	result, err = result.WithColumn("running_max",
		golars.Max("sales").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(result.Select("day", "sales", "running_total", "running_avg", "running_min", "running_max"))
	fmt.Println()
	
	// Example 2: Moving window aggregations (3-day window)
	fmt.Println("=== 3-Day Moving Window ===")
	
	movingResult := df
	
	// 3-day moving average (current + 2 previous days)
	movingResult, err = movingResult.WithColumn("ma3",
		golars.Avg("sales").Over(
			golars.Window().
				OrderBy("day").
				RowsBetween(-2, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	// 3-day moving sum
	movingResult, err = movingResult.WithColumn("sum3",
		golars.Sum("sales").Over(
			golars.Window().
				OrderBy("day").
				RowsBetween(-2, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	// 3-day count
	movingResult, err = movingResult.WithColumn("count3",
		golars.Count("sales").Over(
			golars.Window().
				OrderBy("day").
				RowsBetween(-2, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(movingResult.Select("day", "sales", "ma3", "sum3", "count3"))
	fmt.Println()
	
	// Example 3: Partitioned aggregations
	fmt.Println("=== Partitioned Running Totals ===")
	
	partResult := df
	
	// Running total by store
	partResult, err = partResult.WithColumn("store_total",
		golars.Sum("sales").Over(
			golars.Window().
				PartitionBy("store").
				OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	// Running average by store
	partResult, err = partResult.WithColumn("store_avg",
		golars.Avg("sales").Over(
			golars.Window().
				PartitionBy("store").
				OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	// Count of rows by store
	partResult, err = partResult.WithColumn("store_count",
		golars.Count("sales").Over(
			golars.Window().
				PartitionBy("store").
				OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(partResult.Select("day", "store", "sales", "store_total", "store_avg", "store_count"))
	fmt.Println()
	
	// Example 4: Temperature statistics
	fmt.Println("=== Temperature Statistics ===")
	
	tempResult := df
	
	// Running min/max/avg temperature
	tempResult, err = tempResult.WithColumn("temp_min",
		golars.Min("temperature").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	tempResult, err = tempResult.WithColumn("temp_max",
		golars.Max("temperature").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	tempResult, err = tempResult.WithColumn("temp_avg",
		golars.Avg("temperature").Over(
			golars.Window().OrderBy("day")))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(tempResult.Select("day", "temperature", "temp_min", "temp_max", "temp_avg"))
}