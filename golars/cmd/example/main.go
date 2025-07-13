package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create a DataFrame using the map constructor
	data := map[string]interface{}{
		"name":   []string{"Alice", "Bob", "Charlie", "David", "Eve"},
		"age":    []int32{25, 30, 35, 28, 32},
		"city":   []string{"NYC", "LA", "Chicago", "NYC", "Boston"},
		"salary": []float64{75000, 85000, 95000, 72000, 88000},
	}

	df, err := golars.NewDataFrameFromMap(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Show shape
	height, width := df.Shape()
	fmt.Printf("Shape: %d rows × %d columns\n\n", height, width)

	// Show first 3 rows
	fmt.Println("First 3 rows:")
	fmt.Println(df.Head(3))
	fmt.Println()

	// Select specific columns
	selected, err := df.Select("name", "salary")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Selected columns (name, salary):")
	fmt.Println(selected)
	fmt.Println()

	// Create a DataFrame from Series
	ids := golars.NewInt64Series("id", []int64{101, 102, 103, 104, 105})
	active := golars.NewBooleanSeries("active", []bool{true, true, false, true, false})
	scores := golars.NewFloat32Series("score", []float32{92.5, 87.3, 95.1, 88.9, 91.2})

	df2, err := golars.NewDataFrame(ids, active, scores)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("DataFrame from Series:")
	fmt.Println(df2)
	fmt.Println()

	// Get a specific row
	row, err := df.GetRow(2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Row 2: %+v\n", row)
	fmt.Println()

	// Work with individual Series
	ageSeries, err := df.Column("age")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Age column: %s\n", ageSeries)

	// Create series with null values
	values := []string{"A", "B", "C", "D", "E"}
	validity := []bool{true, false, true, true, false}
	nullableSeries := golars.NewSeriesWithValidity("nullable", values, validity, golars.String)
	
	fmt.Println("\nSeries with null values:")
	for i := 0; i < nullableSeries.Len(); i++ {
		if nullableSeries.IsNull(i) {
			fmt.Printf("  Index %d: null\n", i)
		} else {
			fmt.Printf("  Index %d: %v\n", i, nullableSeries.Get(i))
		}
	}
}