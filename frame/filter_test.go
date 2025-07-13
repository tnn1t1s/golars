package frame

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
)

func TestDataFrameFilter(t *testing.T) {
	// Create test DataFrame
	df, _ := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
		series.NewInt32Series("age", []int32{25, 30, 35, 28, 32}),
		series.NewFloat64Series("score", []float64{85.5, 92.0, 78.5, 95.0, 88.5}),
	)

	t.Run("SimpleComparison", func(t *testing.T) {
		// Filter age > 30
		filtered, err := df.Filter(expr.Col("age").Gt(30))
		assert.NoError(t, err)
		assert.Equal(t, 2, filtered.Height())
		
		// Check filtered values
		ages, err := filtered.Column("age")
		assert.NoError(t, err)
		assert.Equal(t, int32(35), ages.Get(0))
		assert.Equal(t, int32(32), ages.Get(1))
	})

	t.Run("EqualityFilter", func(t *testing.T) {
		// Filter name == "Bob"
		filtered, err := df.Filter(expr.Col("name").Eq("Bob"))
		assert.NoError(t, err)
		assert.Equal(t, 1, filtered.Height())
		
		row, _ := filtered.GetRow(0)
		assert.Equal(t, "Bob", row["name"])
		assert.Equal(t, int32(2), row["id"])
	})

	t.Run("CompoundFilter", func(t *testing.T) {
		// Filter age > 25 AND score < 90
		filtered, err := df.Filter(
			expr.Col("age").Gt(25).And(
				expr.Col("score").Lt(90),
			),
		)
		assert.NoError(t, err)
		assert.Equal(t, 2, filtered.Height())
		
		// Check the results
		names := make([]string, filtered.Height())
		nameCol, err := filtered.Column("name")
		assert.NoError(t, err)
		for i := 0; i < filtered.Height(); i++ {
			names[i] = nameCol.Get(i).(string)
		}
		assert.Contains(t, names, "Charlie")
		assert.Contains(t, names, "Eve")
	})

	t.Run("OrFilter", func(t *testing.T) {
		// Filter age < 26 OR age > 34
		filtered, err := df.Filter(
			expr.Col("age").Lt(26).Or(
				expr.Col("age").Gt(34),
			),
		)
		assert.NoError(t, err)
		assert.Equal(t, 2, filtered.Height())
		
		ages := make([]int32, filtered.Height())
		ageCol, err := filtered.Column("age")
		assert.NoError(t, err)
		for i := 0; i < filtered.Height(); i++ {
			ages[i] = ageCol.Get(i).(int32)
		}
		assert.Contains(t, ages, int32(25))
		assert.Contains(t, ages, int32(35))
	})

	t.Run("NotFilter", func(t *testing.T) {
		// Filter NOT (age == 30)
		filtered, err := df.Filter(
			expr.Col("age").Eq(30).Not(),
		)
		assert.NoError(t, err)
		assert.Equal(t, 4, filtered.Height())
		
		// Verify "Bob" (age 30) is not in results
		nameCol, err := filtered.Column("name")
		assert.NoError(t, err)
		for i := 0; i < filtered.Height(); i++ {
			assert.NotEqual(t, "Bob", nameCol.Get(i))
		}
	})

	t.Run("EmptyResult", func(t *testing.T) {
		// Filter that matches no rows
		filtered, err := df.Filter(expr.Col("age").Gt(100))
		assert.NoError(t, err)
		assert.Equal(t, 0, filtered.Height())
		assert.Equal(t, df.Width(), filtered.Width())
	})

	t.Run("LiteralTrue", func(t *testing.T) {
		// Filter with literal true (should return all rows)
		filtered, err := df.Filter(expr.Lit(true))
		assert.NoError(t, err)
		assert.Equal(t, df.Height(), filtered.Height())
	})

	t.Run("LiteralFalse", func(t *testing.T) {
		// Filter with literal false (should return no rows)
		filtered, err := df.Filter(expr.Lit(false))
		assert.NoError(t, err)
		assert.Equal(t, 0, filtered.Height())
	})
}

func TestDataFrameFilterWithNulls(t *testing.T) {
	// Create DataFrame with null values
	values := []int32{10, 20, 30, 40, 50}
	validity := []bool{true, false, true, false, true}
	
	df, _ := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Int32{}),
	)

	t.Run("IsNull", func(t *testing.T) {
		// Filter for null values
		filtered, err := df.Filter(expr.Col("value").IsNull())
		assert.NoError(t, err)
		assert.Equal(t, 2, filtered.Height())
		
		// Check IDs of null rows
		idCol, err := filtered.Column("id")
		assert.NoError(t, err)
		assert.Equal(t, int32(2), idCol.Get(0))
		assert.Equal(t, int32(4), idCol.Get(1))
	})

	t.Run("IsNotNull", func(t *testing.T) {
		// Filter for non-null values
		filtered, err := df.Filter(expr.Col("value").IsNotNull())
		assert.NoError(t, err)
		assert.Equal(t, 3, filtered.Height())
		
		// Check values
		valueCol, err := filtered.Column("value")
		assert.NoError(t, err)
		assert.Equal(t, int32(10), valueCol.Get(0))
		assert.Equal(t, int32(30), valueCol.Get(1))
		assert.Equal(t, int32(50), valueCol.Get(2))
	})

	t.Run("ComparisonWithNull", func(t *testing.T) {
		// Comparisons with null should exclude null rows
		filtered, err := df.Filter(expr.Col("value").Gt(25))
		assert.NoError(t, err)
		assert.Equal(t, 2, filtered.Height())
		
		// Only non-null values > 25
		valueCol, err := filtered.Column("value")
		assert.NoError(t, err)
		assert.Equal(t, int32(30), valueCol.Get(0))
		assert.Equal(t, int32(50), valueCol.Get(1))
	})
}

func TestDataFrameFilterErrors(t *testing.T) {
	df, _ := NewDataFrame(
		series.NewInt32Series("a", []int32{1, 2, 3}),
		series.NewStringSeries("b", []string{"x", "y", "z"}),
	)

	t.Run("NonExistentColumn", func(t *testing.T) {
		_, err := df.Filter(expr.Col("nonexistent").Gt(0))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("NonBooleanColumn", func(t *testing.T) {
		// Try to use a non-boolean column directly as filter
		_, err := df.Filter(expr.Col("a"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "boolean")
	})
}

func TestComplexFilters(t *testing.T) {
	// Create a larger DataFrame for complex tests
	ids := make([]int64, 100)
	categories := make([]string, 100)
	values := make([]float64, 100)
	flags := make([]bool, 100)
	
	for i := 0; i < 100; i++ {
		ids[i] = int64(i + 1)
		if i%3 == 0 {
			categories[i] = "A"
		} else if i%3 == 1 {
			categories[i] = "B"
		} else {
			categories[i] = "C"
		}
		values[i] = float64(i) * 1.5
		flags[i] = i%2 == 0
	}
	
	df, _ := NewDataFrame(
		series.NewInt64Series("id", ids),
		series.NewStringSeries("category", categories),
		series.NewFloat64Series("value", values),
		series.NewBooleanSeries("flag", flags),
	)

	t.Run("ComplexCompoundFilter", func(t *testing.T) {
		// (category == "A" OR category == "B") AND value > 50 AND flag == true
		filtered, err := df.Filter(
			expr.Col("category").Eq("A").Or(
				expr.Col("category").Eq("B"),
			).And(
				expr.Col("value").Gt(50),
			).And(
				expr.Col("flag").Eq(true),
			),
		)
		assert.NoError(t, err)
		
		// Verify all results match criteria
		categoryCol, err := filtered.Column("category")
		assert.NoError(t, err)
		valueCol, err := filtered.Column("value")
		assert.NoError(t, err)
		flagCol, err := filtered.Column("flag")
		assert.NoError(t, err)
		
		for i := 0; i < filtered.Height(); i++ {
			cat := categoryCol.Get(i).(string)
			val := valueCol.Get(i).(float64)
			flag := flagCol.Get(i).(bool)
			
			assert.True(t, cat == "A" || cat == "B")
			assert.Greater(t, val, 50.0)
			assert.True(t, flag)
		}
	})

	t.Run("ChainedFilters", func(t *testing.T) {
		// Apply filters in sequence
		filtered1, err := df.Filter(expr.Col("value").Ge(30))
		assert.NoError(t, err)
		
		filtered2, err := filtered1.Filter(expr.Col("category").Ne("C"))
		assert.NoError(t, err)
		
		filtered3, err := filtered2.Filter(expr.Col("flag").Eq(true))
		assert.NoError(t, err)
		
		// Should be equivalent to combining all conditions
		combined, err := df.Filter(
			expr.Col("value").Ge(30).And(
				expr.Col("category").Ne("C"),
			).And(
				expr.Col("flag").Eq(true),
			),
		)
		assert.NoError(t, err)
		
		assert.Equal(t, combined.Height(), filtered3.Height())
	})
}

func BenchmarkDataFrameFilter(b *testing.B) {
	// Create a large DataFrame
	size := 100000
	ids := make([]int64, size)
	values := make([]float64, size)
	categories := make([]string, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		values[i] = float64(i) * 0.1
		categories[i] = string(rune('A' + i%26))
	}
	
	df, _ := NewDataFrame(
		series.NewInt64Series("id", ids),
		series.NewFloat64Series("value", values),
		series.NewStringSeries("category", categories),
	)
	
	b.Run("SimpleFilter", func(b *testing.B) {
		filterExpr := expr.Col("value").Gt(50000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filtered, _ := df.Filter(filterExpr)
			_ = filtered.Height()
		}
	})
	
	b.Run("CompoundFilter", func(b *testing.B) {
		filterExpr := expr.Col("value").Gt(25000).And(
			expr.Col("value").Lt(75000),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filtered, _ := df.Filter(filterExpr)
			_ = filtered.Height()
		}
	})
	
	b.Run("StringFilter", func(b *testing.B) {
		filterExpr := expr.Col("category").Eq("A")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filtered, _ := df.Filter(filterExpr)
			_ = filtered.Height()
		}
	})
}