package expr_test

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
)

// Example demonstrates creating basic expressions
func Example() {
	// Column reference
	col := expr.Col("age")
	fmt.Println("Column:", col.String())

	// Literal value
	lit := expr.Lit(25)
	fmt.Println("Literal:", lit.String())

	// Comparison
	comparison := expr.Col("age").Gt(expr.Lit(25))
	fmt.Println("Comparison:", comparison.String())

	// Output:
	// Column: col(age)
	// Literal: lit(25)
	// Comparison: (col(age) > lit(25))
}

// ExampleColumnExpr_Gt demonstrates comparison operations
func ExampleColumnExpr_Gt() {
	// Greater than
	gt := expr.Col("score").Gt(90)
	fmt.Println("Greater than:", gt.String())

	// Greater than or equal
	gte := expr.Col("score").Gte(90)
	fmt.Println("Greater or equal:", gte.String())

	// Less than
	lt := expr.Col("age").Lt(30)
	fmt.Println("Less than:", lt.String())

	// Equal
	eq := expr.Col("status").Eq("active")
	fmt.Println("Equal:", eq.String())

	// Not equal
	ne := expr.Col("status").Ne("inactive")
	fmt.Println("Not equal:", ne.String())

	// Output:
	// Greater than: (col(score) > lit(90))
	// Greater or equal: (col(score) >= lit(90))
	// Less than: (col(age) < lit(30))
	// Equal: (col(status) == col(active))
	// Not equal: (col(status) != col(inactive))
}

// ExampleColumnExpr_And demonstrates logical operations
func ExampleColumnExpr_And() {
	// AND condition
	and := expr.Col("age").Gt(25).And(expr.Col("salary").Gt(50000))
	fmt.Println("AND:", and.String())

	// OR condition
	or := expr.Col("department").Eq("Sales").Or(expr.Col("department").Eq("Marketing"))
	fmt.Println("OR:", or.String())

	// NOT condition
	not := expr.Col("active").Not()
	fmt.Println("NOT:", not.String())

	// Complex condition
	complex := expr.Col("age").Gt(30).And(
		expr.Col("experience").Gt(5).Or(expr.Col("degree").Eq("PhD")),
	)
	fmt.Println("Complex:", complex.String())

	// Output:
	// AND: ((col(age) > lit(25)) & (col(salary) > lit(50000)))
	// OR: ((col(department) == col(Sales)) | (col(department) == col(Marketing)))
	// NOT: !col(active)
	// Complex: ((col(age) > lit(30)) & ((col(experience) > lit(5)) | (col(degree) == col(PhD))))
}

// ExampleColumnExpr_Add demonstrates arithmetic operations
func ExampleColumnExpr_Add() {
	// Addition
	add := expr.Col("base_salary").Add(expr.Col("bonus"))
	fmt.Println("Add:", add.String())

	// Subtraction
	sub := expr.Col("revenue").Sub(expr.Col("costs"))
	fmt.Println("Subtract:", sub.String())

	// Multiplication
	mul := expr.Col("price").Mul(expr.Col("quantity"))
	fmt.Println("Multiply:", mul.String())

	// Division
	div := expr.Col("total").Div(expr.Col("count"))
	fmt.Println("Divide:", div.String())

	// Modulo
	mod := expr.Col("value").Mod(10)
	fmt.Println("Modulo:", mod.String())

	// Output:
	// Add: (col(base_salary) + col(bonus))
	// Subtract: (col(revenue) - col(costs))
	// Multiply: (col(price) * col(quantity))
	// Divide: (col(total) / col(count))
	// Modulo: (col(value) % lit(10))
}

// ExampleColumnExpr_IsNull demonstrates null checking
func ExampleColumnExpr_IsNull() {
	// Check for null
	isNull := expr.Col("email").IsNull()
	fmt.Println("Is null:", isNull.String())

	// Check for not null
	isNotNull := expr.Col("email").IsNotNull()
	fmt.Println("Is not null:", isNotNull.String())

	// Combine null check with other conditions
	combined := expr.Col("score").IsNotNull()
	fmt.Println("Combined check:", combined.String())

	// Output:
	// Is null: col(email).is_null()
	// Is not null: col(email).is_not_null()
	// Combined check: col(score).is_not_null()
}

// ExampleColumnExpr_Sum demonstrates aggregation expressions
func ExampleColumnExpr_Sum() {
	// Sum
	sum := expr.Col("amount").Sum()
	fmt.Println("Sum:", sum.String())

	// Mean
	mean := expr.Col("score").Mean()
	fmt.Println("Mean:", mean.String())

	// Min and Max
	min := expr.Col("age").Min()
	max := expr.Col("age").Max()
	fmt.Println("Min:", min.String())
	fmt.Println("Max:", max.String())

	// Count
	count := expr.Col("id").Count()
	fmt.Println("Count:", count.String())

	// Standard deviation
	std := expr.Col("value").Std()
	fmt.Println("Std:", std.String())

	// Output:
	// Sum: col(amount).sum()
	// Mean: col(score).mean()
	// Min: col(age).min()
	// Max: col(age).max()
	// Count: col(id).count()
	// Std: col(value).std()
}

// ExampleColumnExpr_Between demonstrates range checking
func ExampleColumnExpr_Between() {
	// Check if value is between two bounds
	between := expr.Col("age").Between(25, 35)
	fmt.Println("Between:", between.String())

	// Price range check
	priceRange := expr.Col("price").Between(10.0, 100.0)
	fmt.Println("Price range:", priceRange.String())

	// Date range (as strings)
	dateRange := expr.Col("date").Between("2023-01-01", "2023-12-31")
	fmt.Println("Date range:", dateRange.String())

	// Output:
	// Between: col(age).between(lit(25), lit(35))
	// Price range: col(price).between(lit(10), lit(100))
	// Date range: col(date).between(col(2023-01-01), col(2023-12-31))
}

// ExampleColumnExpr_IsIn demonstrates membership testing
func ExampleColumnExpr_IsIn() {
	// Check if value is in a list
	departments := []string{"Sales", "Marketing", "IT"}
	inDepts := expr.Col("department").IsIn(departments)
	fmt.Println("In departments:", inDepts.String())

	// Check if number is in a set
	validCodes := []int{200, 201, 204}
	inCodes := expr.Col("status_code").IsIn(validCodes)
	fmt.Println("Valid codes:", inCodes.String())

	// Output:
	// In departments: col(department).is_in([lit(Sales), lit(Marketing), lit(IT)])
	// Valid codes: col(status_code).is_in([lit(200), lit(201), lit(204)])
}

// ExampleWhen demonstrates conditional expressions
func ExampleWhen() {
	// Simple if-then-else
	simple := expr.When(expr.Col("age").Gte(18)).
		Then(expr.Lit("adult")).
		Otherwise(expr.Lit("minor"))
	fmt.Println("Simple when:", simple.String())

	// Nested conditions can be built with multiple When expressions
	gradeA := expr.When(expr.Col("score").Gte(90)).
		Then(expr.Lit("A")).
		Otherwise(expr.Lit("Not A"))
	fmt.Println("Grade A check:", gradeA.String())

	// Output:
	// Simple when: when((col(age) >= lit(18))).then(lit(adult)).otherwise(lit(minor))
	// Grade A check: when((col(score) >= lit(90))).then(lit(A)).otherwise(lit(Not A))
}

// Example_aliasing demonstrates aliasing expressions
func Example_aliasing() {
	// Simple alias
	totalPrice := expr.Col("price").Mul(expr.Col("quantity")).Alias("total_price")
	fmt.Println("Aliased:", totalPrice.String())

	// Aggregation with alias
	avgScore := expr.Col("score").Mean().Alias("average_score")
	fmt.Println("Average:", avgScore.String())

	// Complex expression with alias
	profitMargin := expr.Col("revenue").Sub(expr.Col("cost")).
		Div(expr.Col("revenue")).
		Mul(100).
		Alias("profit_margin_pct")
	fmt.Println("Profit margin:", profitMargin.String())

	// Output:
	// Aliased: (col(price) * col(quantity)).alias(total_price)
	// Average: col(score).mean().alias(average_score)
	// Profit margin: (((col(revenue) - col(cost)) / col(revenue)) * lit(100)).alias(profit_margin_pct)
}
