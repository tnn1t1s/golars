package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create sample data
	df, err := golars.NewDataFrame(
		golars.NewStringSeries("department", []string{"Sales", "Sales", "Sales", "IT", "IT", "IT", "HR", "HR"}),
		golars.NewStringSeries("employee", []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"}),
		golars.NewInt32Series("salary", []int32{70000, 65000, 80000, 90000, 85000, 95000, 60000, 62000}),
		golars.NewInt32Series("years", []int32{5, 3, 7, 8, 6, 10, 4, 2}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Row number without partitioning
	fmt.Println("1. Row Number (no partitioning):")
	result1, err := df.WithColumn("row_num",
		golars.RowNumber().Over(golars.Window()))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result1)
	fmt.Println()

	// Example 2: Row number with ordering
	fmt.Println("2. Row Number ordered by salary (descending):")
	result2, err := df.WithColumn("salary_rank",
		golars.RowNumber().Over(
			golars.Window().OrderBy("salary", false)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result2)
	fmt.Println()

	// Example 3: Row number within departments
	fmt.Println("3. Row Number within each department (ordered by salary desc):")
	result3, err := df.WithColumn("dept_rank",
		golars.RowNumber().Over(
			golars.Window().
				PartitionBy("department").
				OrderBy("salary", false)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result3)
	fmt.Println()

	// Example 4: Multiple window functions
	fmt.Println("4. Multiple window functions:")
	result4 := df
	
	// Add row number
	result4, err = result4.WithColumn("row_num",
		golars.RowNumber().Over(golars.Window()))
	if err != nil {
		log.Fatal(err)
	}
	
	// Add rank by salary
	result4, err = result4.WithColumn("salary_rank",
		golars.Rank().Over(
			golars.Window().OrderBy("salary", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Add dense rank by years
	result4, err = result4.WithColumn("years_dense_rank",
		golars.DenseRank().Over(
			golars.Window().OrderBy("years", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(result4)
}