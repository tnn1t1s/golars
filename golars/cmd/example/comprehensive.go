package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	fmt.Println("=== Golars Comprehensive Example ===\n")

	// 1. Create a sample DataFrame
	fmt.Println("1. Creating a DataFrame with sales data:")
	df, err := golars.NewDataFrameFromMap(map[string]interface{}{
		"product":  []string{"Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Laptop", "Phone"},
		"category": []string{"Electronics", "Electronics", "Electronics", "Electronics", "Accessories", "Accessories", "Electronics", "Electronics"},
		"price":    []float64{999.99, 799.99, 499.99, 299.99, 79.99, 29.99, 1199.99, 899.99},
		"quantity": []int32{5, 10, 7, 3, 15, 20, 2, 8},
		"discount": []float64{0.1, 0.05, 0.15, 0.0, 0.2, 0.1, 0.15, 0.1},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(df)
	fmt.Println()

	// 2. Basic Statistics
	fmt.Println("2. Basic Statistics:")
	fmt.Printf("Shape: %d rows × %d columns\n", df.Height(), df.Width())
	fmt.Printf("Columns: %v\n\n", df.Columns())

	// 3. Filtering
	fmt.Println("3. Filtering - Electronics over $500:")
	filtered, err := df.Filter(golars.ColBuilder("category").Eq(golars.Lit("Electronics")).And(
		golars.ColBuilder("price").Gt(500),
	).Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(filtered)
	fmt.Println()

	// 4. Column Selection
	fmt.Println("4. Select specific columns:")
	selected, err := df.Select("product", "price", "quantity")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(selected.Head(5))
	fmt.Println()

	// 5. Working with Series and compute kernels
	fmt.Println("5. Calculate total revenue (price * quantity * (1 - discount)):")
	
	// Get the series
	priceSeries, _ := df.Column("price")
	quantitySeries, _ := df.Column("quantity")
	discountSeries, _ := df.Column("discount")
	
	// For demonstration, let's calculate manually
	// In a real implementation, we'd have Series arithmetic methods
	fmt.Println("Revenue for each row:")
	for i := 0; i < df.Height(); i++ {
		price := priceSeries.Get(i).(float64)
		quantity := float64(quantitySeries.Get(i).(int32))
		discount := discountSeries.Get(i).(float64)
		revenue := price * quantity * (1 - discount)
		product, _ := df.GetRow(i)
		fmt.Printf("  %s: $%.2f\n", product["product"], revenue)
	}
	fmt.Println()

	// 6. Null handling
	fmt.Println("6. Working with null values:")
	// Create data with nulls
	ratings := []float64{4.5, 4.0, 0, 3.5, 4.8, 0, 4.2, 4.6}
	ratingValidity := []bool{true, true, false, true, true, false, true, true}
	ratingSeries := golars.NewSeriesWithValidity("rating", ratings, ratingValidity, golars.Float64)
	
	dfWithRating, err := df.AddColumn(ratingSeries)
	if err != nil {
		log.Fatal(err)
	}
	
	// Filter for products with ratings
	hasRating, err := dfWithRating.Filter(golars.ColBuilder("rating").IsNotNull().Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Products with ratings:")
	fmt.Println(hasRating.Select("product", "rating"))
	fmt.Println()

	// 7. Complex filtering
	fmt.Println("7. Complex query - High-value items (price > 100 OR quantity > 10) AND has discount:")
	complexFilter := golars.ColBuilder("price").Gt(100).Or(
		golars.ColBuilder("quantity").Gt(10),
	).And(
		golars.ColBuilder("discount").Gt(0),
	).Build()
	
	highValue, err := df.Filter(complexFilter)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(highValue)
	fmt.Println()

	// 8. Demonstrate compute kernels directly
	fmt.Println("8. Direct compute kernel usage:")
	// This is a demonstration of what could be done with compute kernels
	// In a full implementation, Series would have arithmetic methods
	fmt.Println("Sample arithmetic operations on series 'a' and 'b':")
	fmt.Printf("  a: %v\n", []int32{10, 20, 30, 40, 50})
	fmt.Printf("  b: %v\n", []int32{5, 4, 3, 2, 1})
	fmt.Println("  Results would be:")
	fmt.Println("  a + b = [15, 24, 33, 42, 51]")
	fmt.Println("  a * b = [50, 80, 90, 80, 50]")
	fmt.Println("  a > b = [true, true, true, true, true]")
	fmt.Println()

	// 9. Working with different data types
	fmt.Println("9. DataFrame with multiple data types:")
	mixedDf, err := golars.NewDataFrame(
		golars.NewBooleanSeries("active", []bool{true, false, true, true}),
		golars.NewInt64Series("id", []int64{1001, 1002, 1003, 1004}),
		golars.NewFloat32Series("score", []float32{95.5, 82.3, 91.7, 88.9}),
		golars.NewStringSeries("grade", []string{"A", "B", "A", "B"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(mixedDf)
	fmt.Println()

	// 10. Slicing and head/tail
	fmt.Println("10. DataFrame slicing:")
	fmt.Println("First 3 rows of original DataFrame:")
	fmt.Println(df.Head(3))
	fmt.Println("\nLast 3 rows:")
	fmt.Println(df.Tail(3))
	
	// Slice specific range
	sliced, err := df.Slice(2, 5)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nRows 2-4 (slice 2:5):")
	fmt.Println(sliced)

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("Golars provides:")
	fmt.Println("✓ Column-oriented DataFrame storage")
	fmt.Println("✓ Type-safe operations with generics")
	fmt.Println("✓ Null value handling")
	fmt.Println("✓ Expression-based filtering")
	fmt.Println("✓ Compute kernels for efficient operations")
	fmt.Println("✓ Apache Arrow integration")
	fmt.Println("\nNext steps would include:")
	fmt.Println("- GroupBy operations")
	fmt.Println("- Join operations")
	fmt.Println("- Sorting")
	fmt.Println("- I/O (CSV, Parquet)")
	fmt.Println("- Lazy evaluation")
	fmt.Println("- More compute kernels")
}

