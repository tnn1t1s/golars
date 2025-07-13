package frame

import (
	"fmt"
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
)

func TestDataFrameSort(t *testing.T) {
	// Create test DataFrame
	df, err := NewDataFrame(
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
		series.NewInt32Series("age", []int32{25, 30, 25, 35, 30}),
		series.NewFloat64Series("score", []float64{85.5, 90.0, 88.0, 92.5, 87.0}),
	)
	assert.NoError(t, err)

	t.Run("Sort by single column ascending", func(t *testing.T) {
		sorted, err := df.Sort("name")
		assert.NoError(t, err)
		
		nameCol, _ := sorted.Column("name")
		expected := []string{"Alice", "Bob", "Charlie", "David", "Eve"}
		for i, exp := range expected {
			assert.Equal(t, exp, nameCol.Get(i))
		}
	})

	t.Run("Sort by single column descending", func(t *testing.T) {
		sorted, err := df.SortDesc("age")
		assert.NoError(t, err)
		
		ageCol, _ := sorted.Column("age")
		nameCol, _ := sorted.Column("name")
		
		// Age should be: 35, 30, 30, 25, 25
		assert.Equal(t, int32(35), ageCol.Get(0))
		assert.Equal(t, "David", nameCol.Get(0))
		
		assert.Equal(t, int32(30), ageCol.Get(1))
		assert.Equal(t, int32(30), ageCol.Get(2))
		
		assert.Equal(t, int32(25), ageCol.Get(3))
		assert.Equal(t, int32(25), ageCol.Get(4))
	})

	t.Run("Sort by multiple columns", func(t *testing.T) {
		// Sort by age (ascending), then by score (descending)
		sorted, err := df.SortBy(SortOptions{
			Columns: []string{"age", "score"},
			Orders:  []series.SortOrder{series.Ascending, series.Descending},
			Stable:  true,
		})
		assert.NoError(t, err)
		
		ageCol, _ := sorted.Column("age")
		scoreCol, _ := sorted.Column("score")
		nameCol, _ := sorted.Column("name")
		
		// First two rows should have age 25
		assert.Equal(t, int32(25), ageCol.Get(0))
		assert.Equal(t, int32(25), ageCol.Get(1))
		
		// Among age 25, Charlie (88.0) should come before Alice (85.5)
		if nameCol.Get(0) == "Charlie" {
			assert.Equal(t, 88.0, scoreCol.Get(0))
			assert.Equal(t, "Alice", nameCol.Get(1))
			assert.Equal(t, 85.5, scoreCol.Get(1))
		} else {
			// If stable sort preserved original order
			assert.Equal(t, "Alice", nameCol.Get(0))
			assert.Equal(t, 85.5, scoreCol.Get(0))
			assert.Equal(t, "Charlie", nameCol.Get(1))
			assert.Equal(t, 88.0, scoreCol.Get(1))
		}
	})

	t.Run("Sort with nulls", func(t *testing.T) {
		// Create DataFrame with nulls
		values := []float64{3.0, 1.0, 4.0, 2.0, 5.0}
		validity := []bool{true, false, true, false, true}
		
		dfNull, err := NewDataFrame(
			series.NewStringSeries("id", []string{"A", "B", "C", "D", "E"}),
			series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
		)
		assert.NoError(t, err)
		
		// Sort by value - nulls should go to end by default
		sorted, err := dfNull.Sort("value")
		assert.NoError(t, err)
		
		valueCol, _ := sorted.Column("value")
		idCol, _ := sorted.Column("id")
		
		// Non-null values should be sorted: 3.0, 4.0, 5.0
		assert.Equal(t, 3.0, valueCol.Get(0))
		assert.Equal(t, 4.0, valueCol.Get(1))
		assert.Equal(t, 5.0, valueCol.Get(2))
		
		// Last two should be null
		assert.True(t, valueCol.IsNull(3))
		assert.True(t, valueCol.IsNull(4))
		
		// Check corresponding IDs
		assert.Equal(t, "A", idCol.Get(0)) // 3.0
		assert.Equal(t, "C", idCol.Get(1)) // 4.0
		assert.Equal(t, "E", idCol.Get(2)) // 5.0
	})
}

func TestDataFrameSortErrors(t *testing.T) {
	df, err := NewDataFrame(
		series.NewInt32Series("a", []int32{1, 2, 3}),
		series.NewInt32Series("b", []int32{4, 5, 6}),
	)
	assert.NoError(t, err)

	// Sort by non-existent column
	_, err = df.Sort("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Sort with empty columns
	_, err = df.Sort()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one column")
}

func TestDataFrameTake(t *testing.T) {
	df, err := NewDataFrame(
		series.NewStringSeries("letter", []string{"A", "B", "C", "D", "E"}),
		series.NewInt32Series("number", []int32{1, 2, 3, 4, 5}),
	)
	assert.NoError(t, err)

	// Take specific indices
	indices := []int{4, 2, 0, 3, 1}
	taken, err := df.Take(indices)
	assert.NoError(t, err)
	
	letterCol, _ := taken.Column("letter")
	numberCol, _ := taken.Column("number")
	
	assert.Equal(t, 5, taken.Height())
	assert.Equal(t, "E", letterCol.Get(0))
	assert.Equal(t, int32(5), numberCol.Get(0))
	assert.Equal(t, "C", letterCol.Get(1))
	assert.Equal(t, int32(3), numberCol.Get(1))

	// Test with out of bounds index
	_, err = df.Take([]int{0, 10, 2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of bounds")
}

func BenchmarkDataFrameSort(b *testing.B) {
	// Create larger DataFrame for benchmarking
	size := 10000
	names := make([]string, size)
	ages := make([]int32, size)
	scores := make([]float64, size)
	
	for i := 0; i < size; i++ {
		names[i] = fmt.Sprintf("Person_%05d", i*7%size)
		ages[i] = int32(20 + i%50)
		scores[i] = float64(50 + i%50)
	}
	
	df, _ := NewDataFrame(
		series.NewStringSeries("name", names),
		series.NewInt32Series("age", ages),
		series.NewFloat64Series("score", scores),
	)

	b.Run("SingleColumn", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = df.Sort("age")
		}
	})

	b.Run("MultiColumn", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = df.SortBy(SortOptions{
				Columns: []string{"age", "score"},
				Orders:  []series.SortOrder{series.Ascending, series.Descending},
			})
		}
	})
}