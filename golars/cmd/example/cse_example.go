// +build ignore

package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/lazy"
)

func main() {
	fmt.Println("Golars Common Subexpression Elimination Example")
	fmt.Println("==============================================")

	// Create a sample DataFrame
	df, err := golars.NewDataFrame(
		golars.NewFloat64Series("price", []float64{10.0, 20.0, 30.0, 40.0, 50.0}),
		golars.NewFloat64Series("quantity", []float64{5, 10, 15, 20, 25}),
		golars.NewFloat64Series("tax_rate", []float64{0.08, 0.08, 0.10, 0.10, 0.12}),
		golars.NewStringSeries("category", []string{"A", "B", "A", "B", "C"}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nOriginal DataFrame:")
	fmt.Println(df)

	// Example 1: Duplicate expressions in projection
	fmt.Println("\n1. Duplicate Expressions in Projection")
	fmt.Println("--------------------------------------")
	
	// Create a lazy query with duplicate expressions
	// Note: price * quantity appears multiple times
	lf1 := golars.LazyFromDataFrame(df).
		SelectColumns("price", "quantity", "tax_rate").
		WithOptimizers() // Clear default optimizers to see unoptimized plan
	
	// Add computed columns with duplicates
	totalExpr := expr.ColBuilder("price").Mul(expr.Col("quantity")).Build()
	lf1 = lf1.Select(
		expr.Col("price"),
		expr.Col("quantity"),
		totalExpr,                                                    // total = price * quantity
		expr.ColBuilder("tax_rate").Mul(totalExpr).Build(),         // tax = tax_rate * (price * quantity)
		totalExpr,                                                    // duplicate: price * quantity again
		expr.ColBuilder("price").Mul(expr.Col("quantity")).Build(), // another way to write the same expression
	)
	
	fmt.Println("\nUnoptimized Plan (with duplicate expressions):")
	fmt.Println(lf1.Explain())
	
	// Now with CSE optimizer
	lf1Opt := lf1.WithOptimizers(lazy.NewCommonSubexpressionElimination())
	
	fmt.Println("\nOptimized Plan (with CSE):")
	optimized, _ := lf1Opt.ExplainOptimized()
	fmt.Println(optimized)

	// Example 2: Duplicate aggregations in GroupBy
	fmt.Println("\n2. Duplicate Aggregations in GroupBy")
	fmt.Println("------------------------------------")
	
	lf2 := golars.LazyFromDataFrame(df).
		GroupBy("category").
		WithOptimizers() // Clear optimizers
	
	// Create aggregations with duplicates
	lf2 = lf2.Agg(map[string]expr.Expr{
		"total_price":     expr.ColBuilder("price").Sum().Build(),
		"total_quantity":  expr.ColBuilder("quantity").Sum().Build(),
		"duplicate_price": expr.ColBuilder("price").Sum().Build(),     // Duplicate sum
		"another_dup":     expr.ColBuilder("price").Sum().Build(),     // Another duplicate
		"avg_price":       expr.ColBuilder("price").Mean().Build(),
		"total_qty_dup":   expr.ColBuilder("quantity").Sum().Build(), // Duplicate quantity sum
	})
	
	fmt.Println("\nUnoptimized GroupBy Plan:")
	fmt.Println(lf2.Explain())
	
	// With CSE
	lf2Opt := lf2.WithOptimizers(lazy.NewCommonSubexpressionElimination())
	
	fmt.Println("\nOptimized GroupBy Plan (duplicates removed):")
	optimized2, _ := lf2Opt.ExplainOptimized()
	fmt.Println(optimized2)
	
	// Execute to see results
	result, err := lf2Opt.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult (note: duplicate aggregations eliminated):")
	fmt.Println(result)

	// Example 3: Complex query with all optimizers
	fmt.Println("\n3. Complex Query with All Optimizers")
	fmt.Println("-------------------------------------")
	
	lf3 := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("price").Gt(15.0).Build()).
		Filter(expr.ColBuilder("quantity").Gt(5.0).Build()).
		GroupBy("category").
		Agg(map[string]expr.Expr{
			"total":     expr.ColBuilder("price").Mul(expr.Col("quantity")).Sum().Build(),
			"duplicate": expr.ColBuilder("price").Mul(expr.Col("quantity")).Sum().Build(), // Same calculation
			"count":     expr.ColBuilder("").Count().Build(),
		}).
		Sort("total", true)
	
	fmt.Println("\nUnoptimized Complex Query:")
	fmt.Println(lf3.WithOptimizers().Explain())
	
	fmt.Println("\nOptimized Complex Query (all optimizers):")
	// Default optimizers include predicate pushdown, projection pushdown, and CSE
	optimized3, _ := lf3.ExplainOptimized()
	fmt.Println(optimized3)
	
	fmt.Println("\nNote how:")
	fmt.Println("- Filters are pushed down and combined (predicate pushdown)")
	fmt.Println("- Only needed columns are read (projection pushdown)")
	fmt.Println("- Duplicate aggregations are eliminated (CSE)")
	
	result3, err := lf3.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nFinal Result:")
	fmt.Println(result3)
}