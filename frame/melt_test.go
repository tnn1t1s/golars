package frame

import (
	"testing"

	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMelt(t *testing.T) {
	t.Run("Basic melt", func(t *testing.T) {
		// Create a wide DataFrame
		df, err := NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"}),
			series.NewInt64Series("math", []int64{90, 85, 95}),
			series.NewInt64Series("english", []int64{85, 90, 88}),
			series.NewInt64Series("science", []int64{92, 88, 90}),
		)
		require.NoError(t, err)
		
		// Melt with name as ID variable
		melted, err := df.Melt(MeltOptions{
			IDVars: []string{"name"},
		})
		require.NoError(t, err)
		
		// Should have 3 * 3 = 9 rows
		assert.Equal(t, 9, melted.Height())
		
		// Check columns
		assert.Equal(t, []string{"name", "variable", "value"}, melted.Columns())
		
		// Check first few rows
		nameCol, err := melted.Column("name")
		require.NoError(t, err)
		assert.Equal(t, "Alice", nameCol.Get(0))
		assert.Equal(t, "Alice", nameCol.Get(1))
		assert.Equal(t, "Alice", nameCol.Get(2))
		
		// Variable column should cycle through subjects
		varCol, err := melted.Column("variable")
		require.NoError(t, err)
		assert.Equal(t, "math", varCol.Get(0))
		assert.Equal(t, "english", varCol.Get(1))
		assert.Equal(t, "science", varCol.Get(2))
		
		// Values should match original
		valCol, err := melted.Column("value")
		require.NoError(t, err)
		assert.Equal(t, int64(90), valCol.Get(0)) // Alice's math
		assert.Equal(t, int64(85), valCol.Get(1)) // Alice's english
		assert.Equal(t, int64(92), valCol.Get(2)) // Alice's science
	})
	
	t.Run("Melt with specific value vars", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("name", []string{"Alice", "Bob"}),
			series.NewInt64Series("age", []int64{25, 30}),
			series.NewInt64Series("math", []int64{90, 85}),
			series.NewInt64Series("english", []int64{85, 90}),
		)
		require.NoError(t, err)
		
		// Only melt math and english scores
		melted, err := df.Melt(MeltOptions{
			IDVars:    []string{"name", "age"},
			ValueVars: []string{"math", "english"},
		})
		require.NoError(t, err)
		
		assert.Equal(t, 4, melted.Height()) // 2 students * 2 subjects
		assert.Equal(t, []string{"name", "age", "variable", "value"}, melted.Columns())
	})
	
	t.Run("Custom column names", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("product", []string{"A", "B"}),
			series.NewFloat64Series("q1", []float64{100.5, 200.5}),
			series.NewFloat64Series("q2", []float64{110.5, 210.5}),
		)
		require.NoError(t, err)
		
		melted, err := df.Melt(MeltOptions{
			IDVars:       []string{"product"},
			VariableName: "quarter",
			ValueName:    "sales",
		})
		require.NoError(t, err)
		
		assert.Equal(t, []string{"product", "quarter", "sales"}, melted.Columns())
		quarterCol, err := melted.Column("quarter")
		require.NoError(t, err)
		assert.Equal(t, "q1", quarterCol.Get(0))
		salesCol, err := melted.Column("sales")
		require.NoError(t, err)
		assert.Equal(t, 100.5, salesCol.Get(0))
	})
	
	t.Run("No ID vars (melt all)", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
			series.NewInt64Series("b", []int64{3, 4}),
		)
		require.NoError(t, err)
		
		melted, err := df.Melt(MeltOptions{})
		require.NoError(t, err)
		
		assert.Equal(t, 4, melted.Height()) // 2 rows * 2 columns
		assert.Equal(t, []string{"variable", "value"}, melted.Columns())
	})
	
	t.Run("Error cases", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 2}),
			series.NewInt64Series("b", []int64{3, 4}),
		)
		require.NoError(t, err)
		
		// Invalid ID column
		_, err = df.Melt(MeltOptions{
			IDVars: []string{"nonexistent"},
		})
		assert.Error(t, err)
		
		// Invalid value column
		_, err = df.Melt(MeltOptions{
			ValueVars: []string{"nonexistent"},
		})
		assert.Error(t, err)
		
		// All columns as ID vars (no value columns)
		_, err = df.Melt(MeltOptions{
			IDVars: []string{"a", "b"},
		})
		assert.Error(t, err)
	})
}