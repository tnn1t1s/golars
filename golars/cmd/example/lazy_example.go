// +build ignore

package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
	"github.com/davidpalaitis/golars/expr"
)

func main() {
	fmt.Println("Golars Lazy Evaluation Example")
	fmt.Println("==============================")

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
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nOriginal DataFrame:")
	fmt.Println(df)

	// Example 1: Basic lazy operations
	fmt.Println("\n1. Basic Lazy Operations")
	fmt.Println("------------------------")
	
	lazyFrame := golars.LazyFromDataFrame(df)
	plan := lazyFrame.
		Filter(expr.ColBuilder("quantity").Gt(int64(10)).Build()).
		SelectColumns("product", "quantity", "price").
		Sort("quantity", true). // descending
		Explain()
	
	fmt.Println("Query Plan:")
	fmt.Println(plan)
	
	// Execute the lazy query
	result, err := lazyFrame.
		Filter(expr.ColBuilder("quantity").Gt(int64(10)).Build()).
		SelectColumns("product", "quantity", "price").
		Sort("quantity", true).
		Collect()
	
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult:")
	fmt.Println(result)

	// Example 2: Group by with aggregations
	fmt.Println("\n2. Lazy GroupBy Operations")
	fmt.Println("--------------------------")
	
	summary, err := golars.LazyFromDataFrame(df).
		GroupBy("product").
		Agg(map[string]expr.Expr{
			"total_quantity": expr.ColBuilder("quantity").Sum().Build(),
			"avg_price":      expr.ColBuilder("price").Mean().Build(),
			"count":          expr.ColBuilder("price").Count().Build(),
		}).
		Sort("total_quantity", true).
		Collect()
	
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Product Summary:")
	fmt.Println(summary)

	// Example 3: Complex query with multiple operations
	fmt.Println("\n3. Complex Lazy Query")
	fmt.Println("---------------------")
	
	// Find top products by store with revenue calculation
	complexQuery := golars.LazyFromDataFrame(df).
		// Calculate revenue
		Select(
			expr.Col("store"),
			expr.Col("product"),
			expr.Col("quantity"),
			expr.Col("price"),
			// Revenue would be: expr.Col("quantity").Mul(expr.Col("price")).Alias("revenue")
			// For now, we'll just use the columns as-is
		).
		// Filter for significant quantities
		Filter(expr.ColBuilder("quantity").Gt(int64(5)).Build()).
		// Group by store and product
		GroupBy("store", "product").
		Sum("quantity").
		// Sort by quantity within each store
		Sort("quantity_sum", true).
		// Take top results
		Limit(5)
	
	fmt.Println("Query Plan:")
	fmt.Println(complexQuery.Explain())
	
	complexResult, err := complexQuery.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nTop Products by Store:")
	fmt.Println(complexResult)

	// Example 4: Demonstrating lazy evaluation benefits
	fmt.Println("\n4. Lazy Evaluation Benefits")
	fmt.Println("---------------------------")
	
	// This query would be inefficient if executed eagerly
	// With lazy evaluation, operations can be optimized
	optimizedQuery := golars.LazyFromDataFrame(df).
		// These filters could be combined and pushed down
		Filter(expr.ColBuilder("store").Eq(expr.Lit("A")).Build()).
		Filter(expr.ColBuilder("quantity").Gt(int64(0)).Build()).
		// Only select needed columns early
		SelectColumns("product", "quantity").
		// Sort and limit - with lazy eval, we might only sort what we need
		Sort("quantity", true).
		Limit(3)
	
	fmt.Println("Optimizable Query Plan:")
	fmt.Println(optimizedQuery.Explain())
	
	optimizedResult, err := optimizedQuery.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nOptimized Result:")
	fmt.Println(optimizedResult)

	// Example 5: Using Head for quick preview
	fmt.Println("\n5. Quick Preview with Head")
	fmt.Println("--------------------------")
	
	preview, err := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("price").Lt(1.0).Build()).
		Head(3).
		Collect()
	
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Preview of low-price items:")
	fmt.Println(preview)
}