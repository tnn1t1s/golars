package series_test

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Example demonstrates creating different types of series
func Example() {
	// Integer series
	intSeries := series.NewInt32Series("ages", []int32{25, 30, 35, 40})
	fmt.Printf("Integer series: %s, length: %d\n", intSeries.Name(), intSeries.Len())

	// Float series
	floatSeries := series.NewFloat64Series("scores", []float64{95.5, 87.3, 92.1})
	fmt.Printf("Float series: %s, mean: %.1f\n", floatSeries.Name(), floatSeries.Mean())

	// String series
	stringSeries := series.NewStringSeries("names", []string{"Alice", "Bob", "Charlie"})
	fmt.Printf("String series: %s, first: %v\n", stringSeries.Name(), stringSeries.Get(0))

	// Output:
	// Integer series: ages, length: 4
	// Float series: scores, mean: 91.6
	// String series: names, first: Alice
}

// ExampleNewSeriesWithValidity demonstrates creating series with null values
func ExampleNewSeriesWithValidity() {
	values := []int32{10, 20, 30, 40, 50}
	validity := []bool{true, false, true, false, true} // false indicates null

	s := series.NewSeriesWithValidity("measurements", values, validity, datatypes.Int32{})

	fmt.Printf("Series length: %d\n", s.Len())
	fmt.Printf("Null count: %d\n", s.NullCount())
	fmt.Printf("Sum (excluding nulls): %.0f\n", s.Sum())
	fmt.Printf("Value at index 1: %v (valid: %v)\n", s.Get(1), s.IsValid(1))

	// Output:
	// Series length: 5
	// Null count: 2
	// Sum (excluding nulls): 90
	// Value at index 1: <nil> (valid: false)
}

// ExampleSeries_Slice demonstrates slicing a series
func ExampleSeries_Slice() {
	s := series.NewInt64Series("numbers", []int64{10, 20, 30, 40, 50, 60, 70})

	// Slice from index 2 to 5 (exclusive)
	sliced, err := s.Slice(2, 5)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Original length: %d\n", s.Len())
	fmt.Printf("Sliced length: %d\n", sliced.Len())
	fmt.Printf("First value in slice: %v\n", sliced.Get(0))

	// Output:
	// Original length: 7
	// Sliced length: 3
	// First value in slice: 30
}

// ExampleSeries_Head demonstrates getting the first n elements
func ExampleSeries_Head() {
	s := series.NewStringSeries("fruits", []string{"apple", "banana", "cherry", "date", "elderberry"})

	head := s.Head(3)
	fmt.Printf("First 3 fruits:\n")
	for i := 0; i < head.Len(); i++ {
		fmt.Printf("  %d: %v\n", i, head.Get(i))
	}

	// Output:
	// First 3 fruits:
	//   0: apple
	//   1: banana
	//   2: cherry
}

// ExampleSeries_Tail demonstrates getting the last n elements
func ExampleSeries_Tail() {
	s := series.NewFloat32Series("temperatures", []float32{20.5, 21.0, 22.3, 23.1, 24.5})

	tail := s.Tail(2)
	fmt.Printf("Last 2 temperatures: %.1f, %.1f\n", tail.Get(0), tail.Get(1))

	// Output:
	// Last 2 temperatures: 23.1, 24.5
}

// ExampleTypedSeries_Sum demonstrates aggregation on typed series
func ExampleTypedSeries_Sum() {
	sales := series.NewFloat64Series("sales", []float64{1500.50, 2300.75, 1850.25, 3200.00})

	total := sales.Sum()
	average := sales.Mean()
	highest := sales.Max().(float64)
	lowest := sales.Min().(float64)

	fmt.Printf("Total sales: $%.2f\n", total)
	fmt.Printf("Average sale: $%.2f\n", average)
	fmt.Printf("Highest sale: $%.2f\n", highest)
	fmt.Printf("Lowest sale: $%.2f\n", lowest)

	// Output:
	// Total sales: $8851.50
	// Average sale: $2212.88
	// Highest sale: $3200.00
	// Lowest sale: $1500.50
}

// ExampleTypedSeries_Median demonstrates calculating median
func ExampleTypedSeries_Median() {
	// Odd number of values
	odd := series.NewInt32Series("odd", []int32{1, 3, 5, 7, 9})
	fmt.Printf("Median (odd): %.1f\n", odd.Median())

	// Even number of values
	even := series.NewInt32Series("even", []int32{1, 2, 3, 4})
	fmt.Printf("Median (even): %.1f\n", even.Median())

	// Output:
	// Median (odd): 5.0
	// Median (even): 2.5
}

// ExampleSeries_GetAsString demonstrates string representation of values
func ExampleSeries_GetAsString() {
	// Create series with different types
	intSeries := series.NewInt32Series("ints", []int32{100, 200, 300})
	floatSeries := series.NewFloat64Series("floats", []float64{1.23, 4.56, 7.89})
	boolSeries := series.NewBooleanSeries("bools", []bool{true, false, true})

	// Get string representations
	fmt.Printf("Int as string: %s\n", intSeries.GetAsString(0))
	fmt.Printf("Float as string: %s\n", floatSeries.GetAsString(1))
	fmt.Printf("Bool as string: %s\n", boolSeries.GetAsString(2))

	// With nulls
	nullSeries := series.NewSeriesWithValidity(
		"nulls",
		[]int32{10, 20, 30},
		[]bool{true, false, true},
		datatypes.Int32{},
	)
	fmt.Printf("Null value as string: %s\n", nullSeries.GetAsString(1))

	// Output:
	// Int as string: 100
	// Float as string: 4.56
	// Bool as string: true
	// Null value as string: null
}

// ExampleSeries_Rename demonstrates renaming a series
func ExampleSeries_Rename() {
	original := series.NewInt64Series("old_name", []int64{1, 2, 3})
	renamed := original.Rename("new_name")

	fmt.Printf("Original name: %s\n", original.Name())
	fmt.Printf("Renamed name: %s\n", renamed.Name())
	fmt.Printf("Same data: %v\n", original.Get(0) == renamed.Get(0))

	// Output:
	// Original name: old_name
	// Renamed name: new_name
	// Same data: true
}
