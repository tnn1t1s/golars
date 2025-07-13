package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create a DataFrame with employee data
	df, err := golars.NewDataFrameFromMap(map[string]interface{}{
		"name":       []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"},
		"department": []string{"Engineering", "Sales", "Engineering", "HR", "Sales", "Engineering"},
		"age":        []int32{28, 35, 42, 31, 26, 38},
		"salary":     []float64{85000, 75000, 95000, 65000, 70000, 88000},
		"years":      []int32{3, 8, 15, 6, 2, 10},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Simple filter - employees over 30
	fmt.Println("1. Employees over 30:")
	filtered, err := df.Filter(golars.ColBuilder("age").Gt(30).Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(filtered)
	fmt.Println()

	// Example 2: Filter by department
	fmt.Println("2. Engineering department:")
	filtered, err = df.Filter(golars.ColBuilder("department").Eq(golars.Lit("Engineering")).Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(filtered)
	fmt.Println()

	// Example 3: Compound filter - High earners (salary > 80k AND years > 5)
	fmt.Println("3. High earners with experience:")
	filtered, err = df.Filter(
		golars.ColBuilder("salary").Gt(80000).And(
			golars.ColBuilder("years").Gt(5),
		).Build(),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(filtered)
	fmt.Println()

	// Example 4: OR condition - Junior or Senior employees
	fmt.Println("4. Junior (< 3 years) OR Senior (> 10 years) employees:")
	filtered, err = df.Filter(
		golars.ColBuilder("years").Lt(3).Or(
			golars.ColBuilder("years").Gt(10),
		).Build(),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(filtered)
	fmt.Println()

	// Example 5: Complex filter with multiple conditions
	fmt.Println("5. Complex filter (Engineering OR Sales) AND salary > 70k AND age < 40:")
	filtered, err = df.Filter(
		golars.ColBuilder("department").Eq(golars.Lit("Engineering")).Or(
			golars.ColBuilder("department").Eq(golars.Lit("Sales")),
		).And(
			golars.ColBuilder("salary").Gt(70000),
		).And(
			golars.ColBuilder("age").Lt(40),
		).Build(),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(filtered)
	fmt.Println()

	// Example 6: Using When-Then-Otherwise for conditional logic
	fmt.Println("6. Categorize employees by salary:")
	// First filter to show only a subset, then we'd apply the when-then logic
	// (Note: When-Then is typically used for creating new columns, not filtering)
	highEarners, err := df.Filter(golars.ColBuilder("salary").Ge(85000).Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("High earners (>= 85k):")
	fmt.Println(highEarners)

	// Example with null handling
	fmt.Println("\n7. Null handling example:")
	// Create data with nulls
	bonuses := []float64{5000, 0, 8000, 0, 3000, 10000}
	bonusValidity := []bool{true, false, true, false, true, true}
	bonusSeries := golars.NewSeriesWithValidity("bonus", bonuses, bonusValidity, golars.Float64)
	
	dfWithBonus, err := df.AddColumn(bonusSeries)
	if err != nil {
		log.Fatal(err)
	}

	// Filter for employees with bonuses (non-null)
	fmt.Println("Employees with bonuses:")
	withBonus, err := dfWithBonus.Filter(golars.ColBuilder("bonus").IsNotNull().Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(withBonus)
	fmt.Println()

	// Filter for employees without bonuses (null)
	fmt.Println("Employees without bonuses:")
	withoutBonus, err := dfWithBonus.Filter(golars.ColBuilder("bonus").IsNull().Build())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(withoutBonus)
}