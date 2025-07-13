package frame_test

import (
	"fmt"
	"log"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// Example demonstrates creating a DataFrame
func Example() {
	// Create series
	ids := series.NewInt32Series("id", []int32{1, 2, 3, 4})
	names := series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David"})
	ages := series.NewInt32Series("age", []int32{25, 30, 35, 28})
	salaries := series.NewFloat64Series("salary", []float64{50000, 60000, 75000, 55000})

	// Create DataFrame
	df, err := frame.NewDataFrame(ids, names, ages, salaries)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Shape: (%d rows, %d columns)\n", df.Height(), df.Width())
	fmt.Printf("Columns: %v\n", df.Columns())

	// Output:
	// Shape: (4 rows, 4 columns)
	// Columns: [id name age salary]
}

// ExampleNewDataFrameFromMap demonstrates creating DataFrame from a map
func ExampleNewDataFrameFromMap() {
	data := map[string]interface{}{
		"product": []string{"A", "B", "C", "D"},
		"price":   []float64{10.99, 25.50, 15.75, 30.00},
		"stock":   []int32{100, 50, 75, 120},
		"active":  []bool{true, true, false, true},
	}

	df, err := frame.NewDataFrameFromMap(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("DataFrame created with %d products\n", df.Height())
	// Output:
	// DataFrame created with 4 products
}

// ExampleDataFrame_Select demonstrates selecting specific columns
func ExampleDataFrame_Select() {
	// Create a DataFrame
	df, _ := frame.NewDataFrame(
		series.NewInt32Series("a", []int32{1, 2, 3}),
		series.NewInt32Series("b", []int32{4, 5, 6}),
		series.NewInt32Series("c", []int32{7, 8, 9}),
		series.NewInt32Series("d", []int32{10, 11, 12}),
	)

	// Select columns a and c
	selected, err := df.Select("a", "c")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Original columns: %v\n", df.Columns())
	fmt.Printf("Selected columns: %v\n", selected.Columns())

	// Output:
	// Original columns: [a b c d]
	// Selected columns: [a c]
}

// ExampleDataFrame_Drop demonstrates dropping columns
func ExampleDataFrame_Drop() {
	df, _ := frame.NewDataFrame(
		series.NewStringSeries("name", []string{"Alice", "Bob"}),
		series.NewInt32Series("age", []int32{25, 30}),
		series.NewStringSeries("city", []string{"NYC", "LA"}),
		series.NewFloat64Series("score", []float64{95.5, 87.3}),
	)

	// Drop age and city columns
	dropped, err := df.Drop("age", "city")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Remaining columns: %v\n", dropped.Columns())
	// Output:
	// Remaining columns: [name score]
}

// ExampleDataFrame_Head demonstrates getting first rows
func ExampleDataFrame_Head() {
	// Create a larger DataFrame
	values := make([]int32, 10)
	for i := range values {
		values[i] = int32(i * 10)
	}
	df, _ := frame.NewDataFrame(
		series.NewInt32Series("value", values),
		series.NewStringSeries("label", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}),
	)

	// Get first 3 rows
	head := df.Head(3)
	fmt.Printf("First %d rows of %d total\n", head.Height(), df.Height())

	// Access values
	col, _ := head.Column("value")
	for i := 0; i < head.Height(); i++ {
		fmt.Printf("Row %d: %v\n", i, col.Get(i))
	}

	// Output:
	// First 3 rows of 10 total
	// Row 0: 0
	// Row 1: 10
	// Row 2: 20
}

// ExampleDataFrame_Tail demonstrates getting last rows
func ExampleDataFrame_Tail() {
	df, _ := frame.NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
		series.NewStringSeries("status", []string{"new", "pending", "done", "done", "new"}),
	)

	tail := df.Tail(2)
	statusCol, _ := tail.Column("status")

	fmt.Printf("Last 2 statuses: %v, %v\n", statusCol.Get(0), statusCol.Get(1))
	// Output:
	// Last 2 statuses: done, new
}

// ExampleDataFrame_Column demonstrates accessing columns
func ExampleDataFrame_Column() {
	df, _ := frame.NewDataFrame(
		series.NewFloat64Series("temperature", []float64{20.5, 21.0, 19.8, 22.3}),
		series.NewStringSeries("condition", []string{"sunny", "cloudy", "rainy", "sunny"}),
	)

	// Access by name
	temp, err := df.Column("temperature")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Average temperature: %.1f\n", temp.Mean())

	// Access by index
	cond, err := df.ColumnAt(1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("First condition: %v\n", cond.Get(0))

	// Output:
	// Average temperature: 20.9
	// First condition: sunny
}

// ExampleDataFrame_RenameColumn demonstrates renaming a column
func ExampleDataFrame_RenameColumn() {
	df, _ := frame.NewDataFrame(
		series.NewInt32Series("old_name", []int32{1, 2, 3}),
		series.NewInt32Series("other", []int32{4, 5, 6}),
	)

	// Rename a column
	renamed, err := df.RenameColumn("old_name", "new_name")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Original: %v\n", df.Columns())
	fmt.Printf("Renamed: %v\n", renamed.Columns())

	// Output:
	// Original: [old_name other]
	// Renamed: [new_name other]
}

// ExampleDataFrame_AddColumn demonstrates adding a new column
func ExampleDataFrame_AddColumn() {
	df, _ := frame.NewDataFrame(
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
		series.NewInt32Series("age", []int32{25, 30, 35}),
	)

	// Add a new column
	scores := series.NewFloat64Series("score", []float64{95.5, 87.3, 92.1})
	newDf, err := df.AddColumn(scores)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Original columns: %v\n", df.Columns())
	fmt.Printf("After adding score: %v\n", newDf.Columns())

	// Output:
	// Original columns: [name age]
	// After adding score: [name age score]
}

// ExampleDataFrame_Sort demonstrates sorting a DataFrame
func ExampleDataFrame_Sort() {
	df, _ := frame.NewDataFrame(
		series.NewStringSeries("name", []string{"Charlie", "Alice", "Bob"}),
		series.NewInt32Series("age", []int32{35, 25, 30}),
		series.NewFloat64Series("score", []float64{92.1, 95.5, 87.3}),
	)

	// Sort by age (ascending by default)
	sorted, err := df.Sort("age")
	if err != nil {
		log.Fatal(err)
	}

	nameCol, _ := sorted.Column("name")
	fmt.Printf("Names sorted by age: ")
	for i := 0; i < sorted.Height(); i++ {
		fmt.Printf("%v ", nameCol.Get(i))
	}
	fmt.Println()

	// Output:
	// Names sorted by age: Alice Bob Charlie
}

// ExampleDataFrame_Clone demonstrates cloning a DataFrame
func ExampleDataFrame_Clone() {
	original, _ := frame.NewDataFrame(
		series.NewInt32Series("x", []int32{1, 2, 3}),
		series.NewInt32Series("y", []int32{4, 5, 6}),
	)

	// Clone creates an independent copy
	cloned := original.Clone()

	// Modifying the clone doesn't affect the original
	fmt.Printf("Original and clone have same shape: %v\n", 
		original.Height() == cloned.Height() && original.Width() == cloned.Width())

	// Output:
	// Original and clone have same shape: true
}