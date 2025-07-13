package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Example 1: Simple Inner Join
	fmt.Println("=== Example 1: Inner Join ===")
	
	// Create employee DataFrame
	employees, err := golars.NewDataFrame(
		golars.NewInt32Series("id", []int32{1, 2, 3, 4}),
		golars.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
		golars.NewStringSeries("dept", []string{"Sales", "IT", "Sales", "HR"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Create salary DataFrame
	salaries, err := golars.NewDataFrame(
		golars.NewInt32Series("id", []int32{2, 3, 4, 5}),
		golars.NewFloat64Series("salary", []float64{75000, 65000, 80000, 70000}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Perform inner join
	result, err := employees.Join(salaries, "id", golars.InnerJoin)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Employees with salaries (Inner Join):")
	printDataFrame(result)
	
	// Example 2: Left Join
	fmt.Println("\n=== Example 2: Left Join ===")
	
	result, err = employees.Join(salaries, "id", golars.LeftJoin)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("All employees with optional salaries (Left Join):")
	printDataFrame(result)
	
	// Example 3: Multi-column Join
	fmt.Println("\n=== Example 3: Multi-column Join ===")
	
	// Create sales data
	sales2020, err := golars.NewDataFrame(
		golars.NewInt32Series("year", []int32{2020, 2020, 2020, 2020}),
		golars.NewInt32Series("quarter", []int32{1, 2, 3, 4}),
		golars.NewFloat64Series("sales", []float64{100000, 120000, 115000, 140000}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Create targets data
	targets, err := golars.NewDataFrame(
		golars.NewInt32Series("year", []int32{2020, 2020, 2020, 2021}),
		golars.NewInt32Series("quarter", []int32{1, 2, 4, 1}),
		golars.NewFloat64Series("target", []float64{95000, 110000, 130000, 105000}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Join on multiple columns
	result, err = sales2020.JoinOn(targets, 
		[]string{"year", "quarter"}, 
		[]string{"year", "quarter"}, 
		golars.InnerJoin)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Sales vs Targets (Multi-column Join):")
	printDataFrame(result)
	
	// Example 4: Different Column Names
	fmt.Println("\n=== Example 4: Join with Different Column Names ===")
	
	// Create orders DataFrame
	orders, err := golars.NewDataFrame(
		golars.NewInt32Series("order_id", []int32{101, 102, 103, 104}),
		golars.NewInt32Series("customer_id", []int32{1, 2, 1, 3}),
		golars.NewFloat64Series("amount", []float64{150.50, 200.00, 75.25, 300.00}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Create customers DataFrame
	customers, err := golars.NewDataFrame(
		golars.NewInt32Series("id", []int32{1, 2, 3, 4}),
		golars.NewStringSeries("customer_name", []string{"Alice", "Bob", "Charlie", "David"}),
		golars.NewStringSeries("city", []string{"NYC", "LA", "Chicago", "Houston"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Join on different column names
	result, err = orders.JoinOn(customers,
		[]string{"customer_id"},
		[]string{"id"},
		golars.InnerJoin)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Orders with Customer Details:")
	printDataFrame(result)
	
	// Example 5: Cross Join
	fmt.Println("\n=== Example 5: Cross Join ===")
	
	// Create small DataFrames for cross join
	colors, err := golars.NewDataFrame(
		golars.NewStringSeries("color", []string{"Red", "Blue", "Green"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	sizes, err := golars.NewDataFrame(
		golars.NewStringSeries("size", []string{"S", "M", "L"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// Perform cross join
	result, err = colors.JoinWithConfig(sizes, golars.JoinConfig{
		How: golars.CrossJoin,
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("All Color-Size Combinations (Cross Join):")
	printDataFrame(result)
	
	// Example 6: Anti Join
	fmt.Println("\n=== Example 6: Anti Join ===")
	
	// Find employees without salaries
	result, err = employees.JoinWithConfig(salaries, golars.JoinConfig{
		How:     golars.AntiJoin,
		LeftOn:  []string{"id"},
		RightOn: []string{"id"},
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Employees without salary records (Anti Join):")
	printDataFrame(result)
	
	// Example 7: Semi Join
	fmt.Println("\n=== Example 7: Semi Join ===")
	
	// Find employees that have salary records
	result, err = employees.JoinWithConfig(salaries, golars.JoinConfig{
		How:     golars.SemiJoin,
		LeftOn:  []string{"id"},
		RightOn: []string{"id"},
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Employees with salary records (Semi Join):")
	printDataFrame(result)
}

// Helper function to print DataFrame
func printDataFrame(df *golars.DataFrame) {
	// Print column names
	cols := df.Columns()
	for i, col := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(col)
	}
	fmt.Println()
	
	// Print separator
	for i := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print("------")
	}
	fmt.Println()
	
	// Print rows
	for i := 0; i < df.Height(); i++ {
		for j, col := range cols {
			if j > 0 {
				fmt.Print("\t")
			}
			series, _ := df.Column(col)
			fmt.Print(series.Get(i))
		}
		fmt.Println()
	}
	fmt.Printf("\n[%d rows x %d columns]\n", df.Height(), len(cols))
}