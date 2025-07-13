// +build ignore

package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
	"github.com/davidpalaitis/golars/expr"
)

func main() {
	fmt.Println("Golars Query Optimization Example")
	fmt.Println("=================================")

	// Create a sample DataFrame
	df, err := golars.NewDataFrame(
		golars.NewStringSeries("product", 
			[]string{"Apple", "Banana", "Apple", "Orange", "Banana", "Apple", "Orange", "Banana"}),
		golars.NewInt64Series("quantity",
			[]int64{10, 15, 20, 5, 25, 30, 10, 20}),
		golars.NewFloat64Series("price",
			[]float64{1.5, 0.5, 1.6, 0.8, 0.6, 1.4, 0.9, 0.5}),
		golars.NewStringSeries("store",
			[]string{"A", "A", "B", "A", "B", "C", "B", "C"}),
		golars.NewInt64Series("year",
			[]int64{2022, 2023, 2023, 2022, 2023, 2023, 2022, 2023}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nOriginal DataFrame:")
	fmt.Println(df)

	// Example 1: Predicate Pushdown
	fmt.Println("\n1. Predicate Pushdown Example")
	fmt.Println("-----------------------------")
	
	// Create a query with multiple filters
	lf1 := golars.LazyFromDataFrame(df).
		SelectColumns("product", "quantity", "price", "year").
		Filter(expr.ColBuilder("year").Eq(expr.Lit(int64(2023))).Build()).
		Sort("quantity", true).
		Filter(expr.ColBuilder("quantity").Gt(int64(10)).Build()).
		Limit(5)
	
	fmt.Println("\nUnoptimized Query Plan:")
	fmt.Println(lf1.Explain())
	
	fmt.Println("\nOptimized Query Plan:")
	optimized, err := lf1.ExplainOptimized()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(optimized)
	
	// Execute the query
	result1, err := lf1.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult:")
	fmt.Println(result1)

	// Example 2: Projection Pushdown
	fmt.Println("\n2. Projection Pushdown Example")
	fmt.Println("------------------------------")
	
	// Create a query that only needs some columns
	lf2 := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("store").Eq(expr.Lit("B")).Build()).
		GroupBy("product").
		Sum("quantity").
		SelectColumns("product", "quantity_sum").
		Sort("quantity_sum", true)
	
	fmt.Println("\nUnoptimized Query Plan:")
	fmt.Println(lf2.Explain())
	
	fmt.Println("\nOptimized Query Plan:")
	optimized2, err := lf2.ExplainOptimized()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(optimized2)
	
	// Execute the query
	result2, err := lf2.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult:")
	fmt.Println(result2)

	// Example 3: Combined Optimizations
	fmt.Println("\n3. Combined Optimizations Example")
	fmt.Println("---------------------------------")
	
	// Complex query that benefits from both optimizations
	lf3 := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("year").Eq(expr.Lit(int64(2023))).Build()).
		Filter(expr.ColBuilder("quantity").Gt(int64(10)).Build()).
		SelectColumns("product", "quantity", "price").
		Filter(expr.ColBuilder("price").Lt(1.0).Build()).
		Sort("quantity", true).
		Limit(3)
	
	fmt.Println("\nUnoptimized Query Plan:")
	fmt.Println(lf3.Explain())
	
	fmt.Println("\nOptimized Query Plan:")
	optimized3, err := lf3.ExplainOptimized()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(optimized3)
	
	fmt.Println("\nNotice how:")
	fmt.Println("- All filters are combined and pushed to the scan")
	fmt.Println("- Only needed columns are selected at the scan level")
	fmt.Println("- This reduces data movement through the query pipeline")
	
	// Execute the query
	result3, err := lf3.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult:")
	fmt.Println(result3)
}