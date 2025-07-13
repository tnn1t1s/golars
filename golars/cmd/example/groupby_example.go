package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
	"github.com/davidpalaitis/golars/expr"
)

func main() {
	// Create a sample sales dataset
	df, err := golars.NewDataFrameFromMap(map[string]interface{}{
		"date":     []string{"2023-01-01", "2023-01-01", "2023-01-02", "2023-01-02", "2023-01-03", "2023-01-03"},
		"product":  []string{"A", "B", "A", "B", "A", "B"},
		"category": []string{"Electronics", "Electronics", "Electronics", "Electronics", "Electronics", "Electronics"},
		"quantity": []int32{10, 15, 12, 8, 20, 18},
		"price":    []float64{99.99, 149.99, 99.99, 149.99, 99.99, 149.99},
		"revenue":  []float64{999.90, 2249.85, 1199.88, 1199.92, 1999.80, 2699.82},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Simple groupby with count
	fmt.Println("Example 1: Count by product")
	gb1, err := df.GroupBy("product")
	if err != nil {
		log.Fatal(err)
	}

	result1, err := gb1.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result1)
	fmt.Println()

	// Example 2: Sum quantities by product
	fmt.Println("Example 2: Total quantity by product")
	result2, err := gb1.Sum("quantity")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result2)
	fmt.Println()

	// Example 3: Multiple aggregations
	fmt.Println("Example 3: Sum and mean revenue by product")
	result3, err := gb1.Sum("revenue")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Sum:")
	fmt.Println(result3)

	result4, err := gb1.Mean("revenue")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Mean:")
	fmt.Println(result4)
	fmt.Println()

	// Example 4: Group by multiple columns
	fmt.Println("Example 4: Group by date and product")
	gb2, err := df.GroupBy("date", "product")
	if err != nil {
		log.Fatal(err)
	}

	result5, err := gb2.Sum("quantity", "revenue")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result5)
	fmt.Println()

	// Example 5: Min/Max aggregations
	fmt.Println("Example 5: Min and Max quantities by product")
	minResult, err := gb1.Min("quantity")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Min quantities:")
	fmt.Println(minResult)

	maxResult, err := gb1.Max("quantity")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Max quantities:")
	fmt.Println(maxResult)
	fmt.Println()

	// Example 6: Custom aggregations using Agg
	fmt.Println("Example 6: Custom aggregations")
	gb3, err := df.GroupBy("product")
	if err != nil {
		log.Fatal(err)
	}

	customResult, err := gb3.Agg(map[string]expr.Expr{
		"total_quantity": expr.ColBuilder("quantity").Sum().Build(),
		"avg_price":      expr.ColBuilder("price").Mean().Build(),
		"max_revenue":    expr.ColBuilder("revenue").Max().Build(),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(customResult)
	fmt.Println()

	// Example 7: Filter then GroupBy
	fmt.Println("Example 7: Filter high-value sales then group")
	filtered, err := df.Filter(
		expr.ColBuilder("revenue").Gt(1500).Build(),
	)
	if err != nil {
		log.Fatal(err)
	}

	gb4, err := filtered.GroupBy("product")
	if err != nil {
		log.Fatal(err)
	}

	result7, err := gb4.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("High-value sales count by product:")
	fmt.Println(result7)

	// Example 8: Working with the results
	fmt.Println("\nExample 8: Accessing aggregation results")
	sumResult, err := gb1.Sum("revenue")
	if err != nil {
		log.Fatal(err)
	}

	// Get specific columns from result
	productCol, _ := sumResult.Column("product")
	revenueCol, _ := sumResult.Column("revenue_sum")

	fmt.Println("Revenue summary:")
	for i := 0; i < sumResult.Height(); i++ {
		product := productCol.Get(i).(string)
		revenue := revenueCol.Get(i).(float64)
		fmt.Printf("Product %s: $%.2f\n", product, revenue)
	}
}