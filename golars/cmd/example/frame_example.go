package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create sample data
	df, err := golars.NewDataFrame(
		golars.NewInt32Series("id", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		golars.NewInt32Series("value", []int32{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}),
		golars.NewStringSeries("group", []string{"A", "A", "A", "A", "B", "B", "B", "B", "B", "B"}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Different frame boundaries
	fmt.Println("=== Different Frame Boundaries ===")
	
	result := df
	
	// ROWS BETWEEN 2 PRECEDING AND CURRENT ROW (3-row window)
	result, err = result.WithColumn("sum_3rows",
		golars.Sum("value").Over(
			golars.Window().
				OrderBy("id").
				RowsBetween(-2, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (centered 3-row window)
	result, err = result.WithColumn("sum_centered",
		golars.Sum("value").Over(
			golars.Window().
				OrderBy("id").
				RowsBetween(-1, 1)))
	if err != nil {
		log.Fatal(err)
	}
	
	// ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING (forward-looking window)
	result, err = result.WithColumn("sum_forward",
		golars.Sum("value").Over(
			golars.Window().
				OrderBy("id").
				RowsBetween(0, 2)))
	if err != nil {
		log.Fatal(err)
	}
	
	// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running total)
	result, err = result.WithColumn("running_total",
		golars.Sum("value").Over(
			golars.Window().
				OrderBy("id")))
	if err != nil {
		log.Fatal(err)
	}
	
	// ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING (reverse running total)
	result, err = result.WithColumn("reverse_total",
		golars.Sum("value").Over(
			golars.Window().
				OrderBy("id").
				RowsBetween(0, 1000000))) // Large number to simulate unbounded
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(result.Select("id", "value", "sum_3rows", "sum_centered", "sum_forward", "running_total", "reverse_total"))
	fmt.Println()
	
	// Example 2: Frame boundaries with partitions
	fmt.Println("=== Frame Boundaries with Partitions ===")
	
	partResult := df
	
	// 2-row moving average within each group
	partResult, err = partResult.WithColumn("group_ma2",
		golars.Avg("value").Over(
			golars.Window().
				PartitionBy("group").
				OrderBy("id").
				RowsBetween(-1, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Min/Max in 3-row window within groups
	partResult, err = partResult.WithColumn("group_min3",
		golars.Min("value").Over(
			golars.Window().
				PartitionBy("group").
				OrderBy("id").
				RowsBetween(-2, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	partResult, err = partResult.WithColumn("group_max3",
		golars.Max("value").Over(
			golars.Window().
				PartitionBy("group").
				OrderBy("id").
				RowsBetween(-2, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(partResult.Select("id", "group", "value", "group_ma2", "group_min3", "group_max3"))
	fmt.Println()
	
	// Example 3: Edge cases - windows extending beyond data
	fmt.Println("=== Edge Cases - Large Windows ===")
	
	edgeResult := df
	
	// 5-row window (larger than some partitions)
	edgeResult, err = edgeResult.WithColumn("sum_5rows",
		golars.Sum("value").Over(
			golars.Window().
				OrderBy("id").
				RowsBetween(-4, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Count in 5-row window
	edgeResult, err = edgeResult.WithColumn("count_5rows",
		golars.Count("value").Over(
			golars.Window().
				OrderBy("id").
				RowsBetween(-4, 0)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(edgeResult.Select("id", "value", "sum_5rows", "count_5rows"))
}