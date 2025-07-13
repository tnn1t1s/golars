package golars_test

import (
	"fmt"
	"log"

	"github.com/tnn1t1s/golars"
)

// ExampleDataFrameFrom demonstrates creating a DataFrame from a map
func ExampleDataFrameFrom() {
	df, err := golars.DataFrameFrom(map[string]interface{}{
		"name":   []string{"Alice", "Bob", "Charlie"},
		"age":    []int{25, 30, 35},
		"salary": []float64{50000, 60000, 75000},
		"active": []bool{true, true, false},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Shape: (%d, %d)\n", df.Height(), df.Width())
	fmt.Printf("Has columns: name=%v, age=%v, salary=%v, active=%v\n", 
		df.HasColumn("name"), df.HasColumn("age"), 
		df.HasColumn("salary"), df.HasColumn("active"))
	// Output:
	// Shape: (3, 4)
	// Has columns: name=true, age=true, salary=true, active=true
}

// ExampleDataFrameFrom_records demonstrates creating a DataFrame from records
func ExampleDataFrameFrom_records() {
	records := []map[string]interface{}{
		{"id": 1, "name": "Alice", "score": 95},
		{"id": 2, "name": "Bob", "score": 87},
		{"id": 3, "name": "Charlie"}, // Missing score
	}

	df, err := golars.DataFrameFrom(records)
	if err != nil {
		log.Fatal(err)
	}

	// Check for missing values
	scoreCol, _ := df.Column("score")
	fmt.Printf("Null count in score: %d\n", scoreCol.NullCount())
	// Output:
	// Null count in score: 1
}

// ExampleSeriesFrom demonstrates creating a Series with type inference
func ExampleSeriesFrom() {
	// Named series
	s1, err := golars.SeriesFrom("temperatures", []float64{22.5, 23.1, 21.8, 24.2})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Series: %s, Type: %v, Length: %d\n", s1.Name(), s1.DataType(), s1.Len())

	// Anonymous series
	s2, err := golars.SeriesFrom([]int{1, 2, 3, 4, 5})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Anonymous series type: %v\n", s2.DataType())

	// Output:
	// Series: temperatures, Type: f64, Length: 4
	// Anonymous series type: i64
}

// ExampleCol demonstrates building column expressions
func ExampleCol() {
	// Simple comparison
	expr1 := golars.Col("age").Gt(25)
	fmt.Println(expr1.String())

	// Chained conditions
	expr2 := golars.Col("age").Gt(25).And(golars.Col("salary").Gt(50000))
	fmt.Println(expr2.String())

	// Between expression
	expr3 := golars.Col("score").Between(80, 90)
	fmt.Println(expr3.String())

	// Output:
	// (col(age) > lit(25))
	// ((col(age) > lit(25)) & (col(salary) > lit(50000)))
	// col(score).between(lit(80), lit(90))
}

// ExampleDataFrame_Filter demonstrates filtering with expressions
func ExampleDataFrame_Filter() {
	df, _ := golars.DataFrameFrom(map[string]interface{}{
		"name":   []string{"Alice", "Bob", "Charlie", "David"},
		"age":    []int{25, 30, 35, 28},
		"salary": []float64{50000, 60000, 75000, 55000},
	})

	// Filter by age > 28
	filtered, err := df.Filter(golars.Col("age").Gt(28))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Filtered rows: %d\n", filtered.Height())
	// Output:
	// Filtered rows: 2
}

// ExampleSeries_Sum demonstrates aggregation methods
func ExampleSeries_Sum() {
	s := golars.NewFloat64Series("values", []float64{10.5, 20.3, 15.7, 25.1, 30.2})

	fmt.Printf("Sum: %.1f\n", s.Sum())
	fmt.Printf("Mean: %.1f\n", s.Mean())
	fmt.Printf("Min: %.1f\n", s.Min())
	fmt.Printf("Max: %.1f\n", s.Max())
	fmt.Printf("Count: %d\n", s.Count())

	// Output:
	// Sum: 101.8
	// Mean: 20.4
	// Min: 10.5
	// Max: 30.2
	// Count: 5
}

// ExampleDataFrame_GroupBy demonstrates grouping and aggregation
func ExampleDataFrame_GroupBy() {
	df, _ := golars.DataFrameFrom(map[string]interface{}{
		"department": []string{"Sales", "Sales", "IT", "IT", "HR"},
		"employee":   []string{"Alice", "Bob", "Charlie", "David", "Eve"},
		"salary":     []float64{50000, 55000, 65000, 70000, 45000},
	})

	grouped, err := df.GroupBy("department")
	if err != nil {
		log.Fatal(err)
	}

	// Sum salaries by department
	result, err := grouped.Sum("salary")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Grouped rows: %d\n", result.Height())
	// Output:
	// Grouped rows: 3
}

// ExampleWhen demonstrates conditional expressions
func ExampleWhen() {
	// Create a conditional expression
	expr := golars.When(golars.Col("score").Gt(90)).
		Then(golars.Lit("A")).
		Otherwise(golars.Lit("F"))

	fmt.Println(expr.String())
	// Output:
	// when((col(score) > lit(90))).then(lit(A)).otherwise(lit(F))
}

// ExampleDataFrame_Select demonstrates column selection
func ExampleDataFrame_Select() {
	df, _ := golars.DataFrameFrom(map[string]interface{}{
		"a": []int{1, 2, 3},
		"b": []int{4, 5, 6},
		"c": []int{7, 8, 9},
		"d": []int{10, 11, 12},
	})

	// Select specific columns
	selected, err := df.Select("a", "c")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Selected columns: %v\n", selected.Columns())
	// Output:
	// Selected columns: [a c]
}

// ExampleReadCSV demonstrates reading CSV files
func ExampleReadCSV() {
	// This is a demonstration - actual file reading would happen here
	// df, err := golars.ReadCSV("data.csv",
	//     golars.WithHeader(true),
	//     golars.WithInferSchemaRows(1000),
	// )

	fmt.Println("CSV reading is supported with various options")
	// Output:
	// CSV reading is supported with various options
}