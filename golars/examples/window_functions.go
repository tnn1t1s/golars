package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
	"github.com/davidpalaitis/golars/dataframe"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

func main() {
	// Example 1: Employee Salary Rankings
	fmt.Println("=== Example 1: Employee Salary Rankings ===")
	employeeExample()

	// Example 2: Sales Time Series Analysis
	fmt.Println("\n=== Example 2: Sales Time Series Analysis ===")
	salesExample()

	// Example 3: Stock Price Analysis
	fmt.Println("\n=== Example 3: Stock Price Analysis ===")
	stockExample()
}

func employeeExample() {
	// Create employee data
	df, err := dataframe.New(
		series.NewStringSeries("employee", []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"}),
		series.NewStringSeries("department", []string{"Sales", "Sales", "Sales", "IT", "IT", "IT"}),
		series.NewInt32Series("salary", []int32{50000, 60000, 55000, 70000, 65000, 80000}),
		series.NewInt32Series("years_exp", []int32{2, 5, 3, 4, 2, 6}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Add various window calculations
	result := df.WithColumn("dept_rank",
		expr.WindowFunc(golars.Rank().Over(
			golars.NewSpec().PartitionBy("department").OrderBy("salary", false),
		)),
	).WithColumn("overall_rank",
		expr.WindowFunc(golars.Rank().Over(
			golars.NewSpec().OrderBy("salary", false),
		)),
	).WithColumn("salary_percentile",
		expr.WindowFunc(golars.PercentRank().Over(
			golars.NewSpec().PartitionBy("department").OrderBy("salary"),
		)),
	).WithColumn("dept_avg_salary",
		expr.WindowFunc(golars.Avg("salary").Over(
			golars.NewSpec().PartitionBy("department"),
		)),
	)

	fmt.Println("Employee Salary Analysis:")
	fmt.Println(result)
}

func salesExample() {
	// Create sales data
	df, err := dataframe.New(
		series.NewStringSeries("date", []string{
			"2024-01", "2024-02", "2024-03", "2024-04", "2024-05",
			"2024-01", "2024-02", "2024-03", "2024-04", "2024-05",
		}),
		series.NewStringSeries("product", []string{
			"A", "A", "A", "A", "A",
			"B", "B", "B", "B", "B",
		}),
		series.NewInt32Series("sales", []int32{
			100, 120, 110, 140, 130,
			200, 180, 220, 210, 240,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Time series analysis
	result := df.WithColumn("running_total",
		expr.WindowFunc(golars.Sum("sales").Over(
			golars.NewSpec().PartitionBy("product").OrderBy("date"),
		)),
	).WithColumn("3mo_avg",
		expr.WindowFunc(golars.Avg("sales").Over(
			golars.NewSpec().PartitionBy("product").OrderBy("date").RowsBetween(-2, 0),
		)),
	).WithColumn("prev_month",
		expr.WindowFunc(golars.Lag("sales", 1, int32(0)).Over(
			golars.NewSpec().PartitionBy("product").OrderBy("date"),
		)),
	).WithColumn("month_over_month",
		expr.Col("sales").Sub(expr.Col("prev_month")),
	).WithColumn("sales_rank",
		expr.WindowFunc(golars.Rank().Over(
			golars.NewSpec().PartitionBy("product").OrderBy("sales", false),
		)),
	)

	fmt.Println("Sales Time Series Analysis:")
	fmt.Println(result)
}

func stockExample() {
	// Create stock price data
	df, err := dataframe.New(
		series.NewStringSeries("date", []string{
			"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
			"2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12",
		}),
		series.NewFloat64Series("price", []float64{
			100.0, 102.5, 101.0, 103.5, 105.0,
			104.5, 106.0, 105.5, 107.0, 108.5,
		}),
		series.NewInt64Series("volume", []int64{
			1000000, 1200000, 900000, 1100000, 1300000,
			1050000, 1400000, 1250000, 1150000, 1600000,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Stock analysis with moving averages
	result := df.WithColumn("sma_5",
		expr.WindowFunc(golars.Avg("price").Over(
			golars.NewSpec().OrderBy("date").RowsBetween(-4, 0),
		)),
	).WithColumn("price_min_5d",
		expr.WindowFunc(golars.Min("price").Over(
			golars.NewSpec().OrderBy("date").RowsBetween(-4, 0),
		)),
	).WithColumn("price_max_5d",
		expr.WindowFunc(golars.Max("price").Over(
			golars.NewSpec().OrderBy("date").RowsBetween(-4, 0),
		)),
	).WithColumn("volume_rank",
		expr.WindowFunc(golars.Rank().Over(
			golars.NewSpec().OrderBy("volume", false),
		)),
	).WithColumn("prev_close",
		expr.WindowFunc(golars.Lag("price", 1, 0.0).Over(
			golars.NewSpec().OrderBy("date"),
		)),
	).WithColumn("daily_return",
		expr.Col("price").Sub(expr.Col("prev_close")).
			Div(expr.Col("prev_close")).Mul(expr.Lit(100.0)),
	)

	fmt.Println("Stock Price Analysis:")
	fmt.Println(result)

	// Calculate volatility metrics
	volatility := result.WithColumn("return_avg",
		expr.WindowFunc(golars.Avg("daily_return").Over(
			golars.NewSpec().OrderBy("date").RowsBetween(-4, 0),
		)),
	)

	fmt.Println("\nVolatility Metrics:")
	fmt.Println(volatility.Select("date", "price", "daily_return", "return_avg"))
}