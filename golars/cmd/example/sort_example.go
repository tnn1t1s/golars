package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

func main() {
	// Create a sample dataset
	df, err := golars.NewDataFrameFromMap(map[string]interface{}{
		"name":       []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"},
		"department": []string{"Sales", "IT", "Sales", "HR", "IT", "HR"},
		"age":        []int32{28, 35, 28, 42, 35, 30},
		"salary":     []float64{50000, 75000, 52000, 65000, 80000, 55000},
		"years":      []int32{3, 8, 3, 12, 10, 5},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Sort by single column (ascending)
	fmt.Println("Example 1: Sort by name (ascending)")
	sorted1, err := df.Sort("name")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sorted1)
	fmt.Println()

	// Example 2: Sort by single column (descending)
	fmt.Println("Example 2: Sort by salary (descending)")
	sorted2, err := df.SortDesc("salary")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sorted2)
	fmt.Println()

	// Example 3: Sort by multiple columns
	fmt.Println("Example 3: Sort by department (asc), then salary (desc)")
	sorted3, err := df.SortBy(golars.SortOptions{
		Columns: []string{"department", "salary"},
		Orders:  []series.SortOrder{series.Ascending, series.Descending},
		Stable:  true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sorted3)
	fmt.Println()

	// Example 4: Sort with nulls
	fmt.Println("Example 4: Sort with null values")
	// Create data with nulls
	scores := []float64{85.5, 90.0, 88.0, 92.5, 87.0, 89.0}
	validity := []bool{true, false, true, true, false, true} // Bob and Eve have null scores
	
	dfWithNulls, err := golars.NewDataFrame(
		golars.NewStringSeries("employee", []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"}),
		golars.NewSeriesWithValidity("performance_score", scores, validity, datatypes.Float64{}),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("DataFrame with nulls:")
	fmt.Println(dfWithNulls)
	
	sortedNulls, err := dfWithNulls.Sort("performance_score")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nSorted by performance_score (nulls last):")
	fmt.Println(sortedNulls)
	fmt.Println()

	// Example 5: Series sorting
	fmt.Println("Example 5: Series sorting")
	ageSeries, _ := df.Column("age")
	sortedAges := ageSeries.Sort(true)
	fmt.Println("Sorted ages:", sortedAges)
	
	// Get sort indices
	ageTyped := ageSeries.(*series.TypedSeries[int32])
	indices := ageTyped.ArgSort(series.SortConfig{
		Order: series.Ascending,
	})
	fmt.Println("Sort indices:", indices)
	fmt.Println()

	// Example 6: Complex multi-column sort
	fmt.Println("Example 6: Sort by age (asc), years (desc), salary (desc)")
	sorted6, err := df.SortBy(golars.SortOptions{
		Columns: []string{"age", "years", "salary"},
		Orders:  []series.SortOrder{
			series.Ascending,
			series.Descending,
			series.Descending,
		},
		Stable: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sorted6)
	fmt.Println()

	// Example 7: Using Take to reorder rows
	fmt.Println("Example 7: Custom row ordering with Take")
	// Reverse the DataFrame
	n := df.Height()
	reverseIndices := make([]int, n)
	for i := 0; i < n; i++ {
		reverseIndices[i] = n - 1 - i
	}
	
	reversed, err := df.Take(reverseIndices)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Reversed DataFrame:")
	fmt.Println(reversed)
}