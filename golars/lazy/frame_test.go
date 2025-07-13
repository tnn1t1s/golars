package lazy

import (
	"testing"

	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestDataFrame() *frame.DataFrame {
	cols := []series.Series{
		series.NewInt64Series("a", []int64{1, 2, 3, 4, 5}),
		series.NewInt64Series("b", []int64{10, 20, 30, 40, 50}),
		series.NewStringSeries("c", []string{"A", "B", "A", "B", "A"}),
	}
	
	df, err := frame.NewDataFrame(cols...)
	if err != nil {
		panic(err)
	}
	return df
}

func TestLazyFrame_Basic(t *testing.T) {
	df := createTestDataFrame()
	
	// Create lazy frame
	lf := NewLazyFrameFromDataFrame(df)
	assert.NotNil(t, lf)
	
	// Check schema
	schema, err := lf.Schema()
	require.NoError(t, err)
	assert.Equal(t, 3, len(schema.Fields))
	
	// Collect should return the same DataFrame
	result, err := lf.Collect()
	require.NoError(t, err)
	assert.Equal(t, df.Height(), result.Height())
	assert.Equal(t, df.Width(), result.Width())
}

func TestLazyFrame_Select(t *testing.T) {
	df := createTestDataFrame()
	lf := NewLazyFrameFromDataFrame(df)
	
	// Select columns
	selected := lf.SelectColumns("a", "c")
	
	// Check plan
	plan := selected.Explain()
	assert.Contains(t, plan, "Project")
	
	// Collect and verify
	result, err := selected.Collect()
	require.NoError(t, err)
	assert.Equal(t, 2, result.Width())
	assert.Equal(t, 5, result.Height())
	
	// Check columns
	cols := result.Columns()
	assert.Equal(t, []string{"a", "c"}, cols)
}

func TestLazyFrame_Filter(t *testing.T) {
	df := createTestDataFrame()
	lf := NewLazyFrameFromDataFrame(df)
	
	// Filter where a > 2
	filtered := lf.Filter(expr.ColBuilder("a").Gt(int64(2)).Build())
	
	// Check plan
	plan := filtered.Explain()
	assert.Contains(t, plan, "Filter")
	
	// Collect and verify
	result, err := filtered.Collect()
	require.NoError(t, err)
	assert.Equal(t, 3, result.Height()) // Should have 3 rows where a > 2
	
	// Verify values
	colA, err := result.Column("a")
	require.NoError(t, err)
	assert.Equal(t, int64(3), colA.Get(0))
	assert.Equal(t, int64(4), colA.Get(1))
	assert.Equal(t, int64(5), colA.Get(2))
}

func TestLazyFrame_ChainedOperations(t *testing.T) {
	df := createTestDataFrame()
	
	// Chain multiple operations
	result, err := NewLazyFrameFromDataFrame(df).
		Filter(expr.ColBuilder("a").Gt(int64(2)).Build()).
		SelectColumns("a", "b").
		Sort("a", true).
		Limit(2).
		Collect()
	
	require.NoError(t, err)
	assert.Equal(t, 2, result.Height())
	assert.Equal(t, 2, result.Width())
	
	// Check values
	colA, err := result.Column("a")
	require.NoError(t, err)
	assert.Equal(t, int64(5), colA.Get(0)) // Sorted descending
	assert.Equal(t, int64(4), colA.Get(1))
}

func TestLazyFrame_GroupBy(t *testing.T) {
	df := createTestDataFrame()
	
	// Group by c and sum a
	result, err := NewLazyFrameFromDataFrame(df).
		GroupBy("c").
		Sum("a", "b").
		Collect()
	
	require.NoError(t, err)
	// Should have 2 groups (A and B)
	assert.Equal(t, 2, result.Height())
	
	// Check that we have the expected columns
	cols := result.Columns()
	assert.Contains(t, cols, "c")
	assert.Contains(t, cols, "a_sum")
	assert.Contains(t, cols, "b_sum")
}

func TestLazyFrame_Explain(t *testing.T) {
	df := createTestDataFrame()
	
	// Create a complex query
	lf := NewLazyFrameFromDataFrame(df).
		Filter(expr.ColBuilder("a").Gt(int64(1)).Build()).
		SelectColumns("a", "b").
		Sort("a", false)
	
	// Get the plan
	plan := lf.Explain()
	assert.Contains(t, plan, "Sort")
	assert.Contains(t, plan, "Project")
	assert.Contains(t, plan, "Filter")
	assert.Contains(t, plan, "Scan")
}

func TestLazyFrame_MultipleFilters(t *testing.T) {
	df := createTestDataFrame()
	
	// Apply multiple filters
	result, err := NewLazyFrameFromDataFrame(df).
		Filter(expr.ColBuilder("a").Gt(int64(2)).Build()).
		Filter(expr.ColBuilder("b").Lt(int64(50)).Build()).
		Collect()
	
	require.NoError(t, err)
	// Should have rows where a > 2 AND b < 50
	assert.Equal(t, 2, result.Height()) // rows with a=3,4
}

func TestLazyFrame_SortMultiple(t *testing.T) {
	// Create DataFrame with duplicate values
	cols := []series.Series{
		series.NewStringSeries("x", []string{"B", "A", "B", "A"}),
		series.NewInt64Series("y", []int64{2, 1, 1, 2}),
	}
	df, err := frame.NewDataFrame(cols...)
	require.NoError(t, err)
	
	// Sort by x ascending, then y descending
	result, err := NewLazyFrameFromDataFrame(df).
		SortBy([]string{"x", "y"}, []bool{false, true}).
		Collect()
	
	require.NoError(t, err)
	
	// Check order
	colX, err := result.Column("x")
	require.NoError(t, err)
	colY, err := result.Column("y")
	require.NoError(t, err)
	
	// Should be sorted: A,2 | A,1 | B,2 | B,1
	assert.Equal(t, "A", colX.Get(0))
	assert.Equal(t, int64(2), colY.Get(0))
	assert.Equal(t, "A", colX.Get(1))
	assert.Equal(t, int64(1), colY.Get(1))
	assert.Equal(t, "B", colX.Get(2))
	assert.Equal(t, int64(2), colY.Get(2))
	assert.Equal(t, "B", colX.Get(3))
	assert.Equal(t, int64(1), colY.Get(3))
}

func TestLazyFrame_Clone(t *testing.T) {
	df := createTestDataFrame()
	lf1 := NewLazyFrameFromDataFrame(df).Filter(expr.ColBuilder("a").Gt(int64(2)).Build())
	
	// Clone the lazy frame
	lf2 := lf1.Clone()
	
	// Modify the clone
	lf2 = lf2.SelectColumns("a")
	
	// Collect both
	result1, err := lf1.Collect()
	require.NoError(t, err)
	result2, err := lf2.Collect()
	require.NoError(t, err)
	
	// They should be different
	assert.Equal(t, 3, result1.Width())
	assert.Equal(t, 1, result2.Width())
}

func TestLazyFrame_Head(t *testing.T) {
	df := createTestDataFrame()
	
	// Use Head (alias for Limit)
	result, err := NewLazyFrameFromDataFrame(df).Head(3).Collect()
	require.NoError(t, err)
	assert.Equal(t, 3, result.Height())
}

// Benchmark tests
func BenchmarkLazyFrame_vs_Eager(b *testing.B) {
	// Create larger test data
	n := 10000
	aData := make([]int64, n)
	bData := make([]int64, n)
	cData := make([]string, n)
	
	for i := 0; i < n; i++ {
		aData[i] = int64(i)
		bData[i] = int64(i * 10)
		if i%2 == 0 {
			cData[i] = "A"
		} else {
			cData[i] = "B"
		}
	}
	
	cols := []series.Series{
		series.NewInt64Series("a", aData),
		series.NewInt64Series("b", bData),
		series.NewStringSeries("c", cData),
	}
	df, _ := frame.NewDataFrame(cols...)
	
	b.Run("Eager", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Eager evaluation creates intermediate DataFrames
			filtered, _ := df.Filter(expr.ColBuilder("a").Gt(int64(5000)).Build())
			sorted, _ := filtered.Sort("b")
			limited, _ := sorted.Take([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
			_ = limited
		}
	})
	
	b.Run("Lazy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Lazy evaluation builds a plan and executes once
			result, _ := NewLazyFrameFromDataFrame(df).
				Filter(expr.ColBuilder("a").Gt(int64(5000)).Build()).
				Sort("b", false).
				Limit(10).
				Collect()
			_ = result
		}
	})
}