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
	fmt.Println("Golars Filter Combining Optimization Example")
	fmt.Println("===========================================")

	// Create a sample DataFrame
	df, err := golars.NewDataFrame(
		golars.NewInt32Series("age", []int32{25, 30, 35, 40, 45, 20, 22, 28, 33, 38}),
		golars.NewStringSeries("status", []string{"active", "active", "inactive", "active", "inactive", "active", "active", "inactive", "active", "active"}),
		golars.NewFloat64Series("score", []float64{85.5, 92.3, 78.1, 88.9, 75.5, 91.2, 87.4, 79.8, 90.1, 86.7}),
		golars.NewStringSeries("department", []string{"sales", "eng", "sales", "hr", "eng", "sales", "eng", "hr", "sales", "eng"}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nOriginal DataFrame:")
	fmt.Println(df)

	// Example 1: Multiple adjacent filters
	fmt.Println("\n1. Multiple Adjacent Filters")
	fmt.Println("----------------------------")
	
	// Create a lazy query with multiple filters applied sequentially
	lf1 := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("age").Gt(int32(25)).Build()).
		Filter(expr.ColBuilder("status").Eq(expr.Lit("active")).Build()).
		Filter(expr.ColBuilder("score").Gt(85.0).Build()).
		Filter(expr.ColBuilder("department").Ne(expr.Lit("hr")).Build())
	
	// Show unoptimized plan without filter combining
	lfNoOpt := lf1.WithOptimizers(
		lazy.NewPredicatePushdown(),
		lazy.NewProjectionPushdown(),
		// Deliberately exclude filter combining
	)
	
	fmt.Println("\nUnoptimized Plan (without filter combining):")
	fmt.Println(lfNoOpt.Explain())
	
	fmt.Println("\nOptimized Plan (with filter combining):")
	optimized, _ := lf1.ExplainOptimized()
	fmt.Println(optimized)
	
	result, err := lf1.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult after filtering:")
	fmt.Println(result)

	// Example 2: Filters with intermediate operations
	fmt.Println("\n2. Filters with Intermediate Operations")
	fmt.Println("---------------------------------------")
	
	lf2 := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("age").Gt(int32(20)).Build()).
		Filter(expr.ColBuilder("status").Eq(expr.Lit("active")).Build()).
		SelectColumns("age", "department", "score").
		Filter(expr.ColBuilder("score").Gt(80.0).Build()).
		Filter(expr.ColBuilder("department").Eq(expr.Lit("eng")).Build())
	
	fmt.Println("\nUnoptimized Plan:")
	fmt.Println(lf2.WithOptimizers().Explain())
	
	fmt.Println("\nOptimized Plan (filters combined where possible):")
	optimized2, _ := lf2.ExplainOptimized()
	fmt.Println(optimized2)

	// Example 3: Complex query with filters around aggregation
	fmt.Println("\n3. Filters Around Aggregation")
	fmt.Println("-----------------------------")
	
	lf3 := golars.LazyFromDataFrame(df).
		Filter(expr.ColBuilder("status").Eq(expr.Lit("active")).Build()).
		Filter(expr.ColBuilder("age").Gt(int32(22)).Build()).
		GroupBy("department").
		Agg(map[string]expr.Expr{
			"avg_score": expr.ColBuilder("score").Mean().Build(),
			"count":     expr.ColBuilder("age").Count().Build(),
		}).
		Filter(expr.ColBuilder("avg_score").Gt(85.0).Build()).
		Filter(expr.ColBuilder("count").Gt(int64(1)).Build())
	
	fmt.Println("\nUnoptimized Plan:")
	fmt.Println(lf3.WithOptimizers().Explain())
	
	fmt.Println("\nOptimized Plan (pre-agg and post-agg filters combined):")
	optimized3, _ := lf3.ExplainOptimized()
	fmt.Println(optimized3)
	
	result3, err := lf3.Collect()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("\nResult:")
	fmt.Println(result3)

	// Example 4: Performance comparison
	fmt.Println("\n4. Performance Impact")
	fmt.Println("--------------------")
	
	// Create a query with many filters
	lfMany := golars.LazyFromDataFrame(df)
	for i := 0; i < 5; i++ {
		lfMany = lfMany.Filter(expr.ColBuilder("age").Gt(int32(20 + i)).Build())
	}
	
	fmt.Println("\nPlan with 5 adjacent filters (unoptimized):")
	unoptPlan := lfMany.WithOptimizers(
		lazy.NewPredicatePushdown(),
		lazy.NewProjectionPushdown(),
		// No filter combining
	).Explain()
	fmt.Println(unoptPlan)
	
	fmt.Println("\nPlan with filter combining (optimized):")
	optPlan, _ := lfMany.ExplainOptimized()
	fmt.Println(optPlan)
	
	fmt.Println("\nNote how filter combining:")
	fmt.Println("- Reduces the number of filter nodes in the plan")
	fmt.Println("- Combines adjacent filters into single AND expressions")
	fmt.Println("- Improves performance by reducing iterations over data")
	fmt.Println("- Works seamlessly with other optimizers")
}