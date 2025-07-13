package frame

import (
	"testing"

	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
)

func TestInnerJoin(t *testing.T) {
	// Create left DataFrame
	left, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
	)
	assert.NoError(t, err)

	// Create right DataFrame
	right, err := NewDataFrame(
		series.NewInt32Series("id", []int32{2, 3, 4, 5}),
		series.NewStringSeries("city", []string{"NYC", "LA", "Chicago", "Houston"}),
	)
	assert.NoError(t, err)

	// Perform inner join
	result, err := left.Join(right, "id", InnerJoin)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 3 rows (ids 2, 3, 4 match)
	assert.Equal(t, 3, result.Height())

	// Check columns
	assert.Equal(t, 3, len(result.columns))
	assert.True(t, result.HasColumn("id"))
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("city"))

	// Verify data
	idCol, err := result.Column("id")
	assert.NoError(t, err)
	nameCol, err := result.Column("name")
	assert.NoError(t, err)
	cityCol, err := result.Column("city")
	assert.NoError(t, err)

	// Build maps to handle potential row reordering
	resultMap := make(map[int32]struct {
		name string
		city string
	})

	for i := 0; i < result.Height(); i++ {
		id := idCol.Get(i).(int32)
		name := nameCol.Get(i).(string)
		city := cityCol.Get(i).(string)
		resultMap[id] = struct {
			name string
			city string
		}{name, city}
	}

	// Verify expected matches
	assert.Equal(t, "Bob", resultMap[2].name)
	assert.Equal(t, "NYC", resultMap[2].city)

	assert.Equal(t, "Charlie", resultMap[3].name)
	assert.Equal(t, "LA", resultMap[3].city)

	assert.Equal(t, "David", resultMap[4].name)
	assert.Equal(t, "Chicago", resultMap[4].city)
}

func TestLeftJoin(t *testing.T) {
	// Create left DataFrame
	left, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
	)
	assert.NoError(t, err)

	// Create right DataFrame with partial matches
	right, err := NewDataFrame(
		series.NewInt32Series("id", []int32{2, 4}),
		series.NewStringSeries("score", []string{"85", "92"}),
	)
	assert.NoError(t, err)

	// Perform left join
	result, err := left.Join(right, "id", LeftJoin)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have all 3 rows from left
	assert.Equal(t, 3, result.Height())

	// Check columns
	assert.Equal(t, 3, len(result.columns))
	assert.True(t, result.HasColumn("id"))
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("score"))
}

func TestMultiColumnJoin(t *testing.T) {
	// Create left DataFrame
	left, err := NewDataFrame(
		series.NewInt32Series("year", []int32{2020, 2020, 2021, 2021}),
		series.NewInt32Series("month", []int32{1, 2, 1, 2}),
		series.NewFloat64Series("sales", []float64{100.0, 150.0, 200.0, 250.0}),
	)
	assert.NoError(t, err)

	// Create right DataFrame
	right, err := NewDataFrame(
		series.NewInt32Series("year", []int32{2020, 2020, 2021}),
		series.NewInt32Series("month", []int32{1, 3, 1}),
		series.NewFloat64Series("budget", []float64{90.0, 160.0, 180.0}),
	)
	assert.NoError(t, err)

	// Perform join on multiple columns
	result, err := left.JoinOn(right, []string{"year", "month"}, []string{"year", "month"}, InnerJoin)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 2 matches: (2020,1) and (2021,1)
	assert.Equal(t, 2, result.Height())
}

func TestCrossJoin(t *testing.T) {
	// Create small DataFrames for cross join
	left, err := NewDataFrame(
		series.NewStringSeries("letter", []string{"A", "B"}),
	)
	assert.NoError(t, err)

	right, err := NewDataFrame(
		series.NewInt32Series("number", []int32{1, 2, 3}),
	)
	assert.NoError(t, err)

	// Perform cross join
	result, err := left.JoinWithConfig(right, JoinConfig{
		How: CrossJoin,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 2 * 3 = 6 rows
	assert.Equal(t, 6, result.Height())
	assert.Equal(t, 2, len(result.columns))
}

func TestAntiJoin(t *testing.T) {
	// Create left DataFrame
	left, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
	)
	assert.NoError(t, err)

	// Create right DataFrame
	right, err := NewDataFrame(
		series.NewInt32Series("id", []int32{2, 4}),
	)
	assert.NoError(t, err)

	// Perform anti join
	result, err := left.JoinWithConfig(right, JoinConfig{
		How:    AntiJoin,
		LeftOn: []string{"id"},
		RightOn: []string{"id"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 2 rows (ids 1 and 3 not in right)
	assert.Equal(t, 2, result.Height())
}

func TestSemiJoin(t *testing.T) {
	// Create left DataFrame
	left, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
	)
	assert.NoError(t, err)

	// Create right DataFrame
	right, err := NewDataFrame(
		series.NewInt32Series("id", []int32{2, 4, 4}), // Duplicate 4
	)
	assert.NoError(t, err)

	// Perform semi join
	result, err := left.JoinWithConfig(right, JoinConfig{
		How:     SemiJoin,
		LeftOn:  []string{"id"},
		RightOn: []string{"id"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 2 rows (ids 2 and 4 exist in right)
	assert.Equal(t, 2, result.Height())
}

func TestJoinWithDifferentColumnNames(t *testing.T) {
	// Create left DataFrame
	left, err := NewDataFrame(
		series.NewInt32Series("customer_id", []int32{1, 2, 3}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
	)
	assert.NoError(t, err)

	// Create right DataFrame
	right, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 4}),
		series.NewStringSeries("email", []string{"alice@example.com", "bob@example.com", "david@example.com"}),
	)
	assert.NoError(t, err)

	// Join on different column names
	result, err := left.JoinOn(right, []string{"customer_id"}, []string{"id"}, InnerJoin)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 2 matches
	assert.Equal(t, 2, result.Height())
}

func TestJoinWithColumnNameConflicts(t *testing.T) {
	// Create DataFrames with overlapping column names
	left, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2}),
		series.NewStringSeries("value", []string{"left1", "left2"}),
	)
	assert.NoError(t, err)

	right, err := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2}),
		series.NewStringSeries("value", []string{"right1", "right2"}),
	)
	assert.NoError(t, err)

	// Join with custom suffix
	result, err := left.JoinWithConfig(right, JoinConfig{
		How:     InnerJoin,
		LeftOn:  []string{"id"},
		RightOn: []string{"id"},
		Suffix:  "_r",
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have columns: id, value, value_r
	assert.Equal(t, 3, len(result.columns))
	assert.True(t, result.HasColumn("id"))
	assert.True(t, result.HasColumn("value"))
	assert.True(t, result.HasColumn("value_r"))
}

func TestJoinValidation(t *testing.T) {
	left, _ := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3}),
	)
	right, _ := NewDataFrame(
		series.NewStringSeries("id", []string{"1", "2", "3"}), // Different type
	)

	// Should fail due to type mismatch
	_, err := left.Join(right, "id", InnerJoin)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "incompatible types")

	// Should fail due to missing column
	_, err = left.Join(right, "missing", InnerJoin)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestEmptyJoinResults(t *testing.T) {
	// Create DataFrames with no matching keys
	left, _ := NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3}),
	)
	right, _ := NewDataFrame(
		series.NewInt32Series("id", []int32{4, 5, 6}),
	)

	// Inner join should return empty result
	result, err := left.Join(right, "id", InnerJoin)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 0, result.Height())

	// Left join should return all left rows
	result, err = left.Join(right, "id", LeftJoin)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, result.Height())
}

func BenchmarkInnerJoin(b *testing.B) {
	// Create larger DataFrames for benchmarking
	size := 10000
	leftIDs := make([]int32, size)
	rightIDs := make([]int32, size/2)
	
	for i := 0; i < size; i++ {
		leftIDs[i] = int32(i)
	}
	for i := 0; i < size/2; i++ {
		rightIDs[i] = int32(i * 2) // Every other ID
	}

	left, _ := NewDataFrame(
		series.NewInt32Series("id", leftIDs),
		series.NewFloat64Series("value", make([]float64, size)),
	)
	right, _ := NewDataFrame(
		series.NewInt32Series("id", rightIDs),
		series.NewFloat64Series("score", make([]float64, size/2)),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = left.Join(right, "id", InnerJoin)
	}
}

func BenchmarkMultiColumnJoin(b *testing.B) {
	size := 10000
	years := make([]int32, size)
	months := make([]int32, size)
	
	for i := 0; i < size; i++ {
		years[i] = 2020 + int32(i%5)
		months[i] = 1 + int32(i%12)
	}

	left, _ := NewDataFrame(
		series.NewInt32Series("year", years),
		series.NewInt32Series("month", months),
		series.NewFloat64Series("value", make([]float64, size)),
	)
	right, _ := NewDataFrame(
		series.NewInt32Series("year", years[:size/2]),
		series.NewInt32Series("month", months[:size/2]),
		series.NewFloat64Series("score", make([]float64, size/2)),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = left.JoinOn(right, []string{"year", "month"}, []string{"year", "month"}, InnerJoin)
	}
}