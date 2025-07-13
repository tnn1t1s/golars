package lazy

import (
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/io"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLazyParquetScan(t *testing.T) {
	// Create test data
	df, err := frame.NewDataFrame(
		series.NewInt32Series("id", []int32{1, 2, 3, 4, 5}),
		series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
		series.NewFloat64Series("score", []float64{95.5, 87.3, 92.1, 78.9, 88.4}),
		series.NewStringSeries("city", []string{"NYC", "LA", "Chicago", "NYC", "LA"}),
	)
	require.NoError(t, err)

	// Create temp file
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_lazy.parquet")

	// Write to parquet
	err = io.WriteParquet(df, filename)
	require.NoError(t, err)

	// Test 1: Basic lazy scan
	t.Run("basic_scan", func(t *testing.T) {
		lf := NewLazyFrame(NewScanNode(NewParquetSource(filename)))
		result, err := lf.Collect()
		require.NoError(t, err)

		assert.Equal(t, df.Height(), result.Height())
		assert.Equal(t, df.Width(), result.Width())
		assert.Equal(t, df.Columns(), result.Columns())
	})

	// Test 2: Lazy scan with column selection
	t.Run("column_selection", func(t *testing.T) {
		lf := NewLazyFrame(NewScanNode(NewParquetSource(filename))).
			SelectColumns("id", "name", "score")
		
		result, err := lf.Collect()
		require.NoError(t, err)

		assert.Equal(t, df.Height(), result.Height())
		assert.Equal(t, 3, result.Width())
		assert.Equal(t, []string{"id", "name", "score"}, result.Columns())
	})

	// Test 3: Lazy scan with filter
	t.Run("with_filter", func(t *testing.T) {
		lf := NewLazyFrame(NewScanNode(NewParquetSource(filename))).
			Filter(expr.Col("score").Gt(90))
		
		result, err := lf.Collect()
		require.NoError(t, err)

		assert.Equal(t, 2, result.Height()) // Alice (95.5) and Charlie (92.1)
		
		scoreCol, _ := result.Column("score")
		assert.Equal(t, float64(95.5), scoreCol.Get(0))
		assert.Equal(t, float64(92.1), scoreCol.Get(1))
	})

	// Test 4: Lazy scan with filter and projection
	t.Run("filter_and_projection", func(t *testing.T) {
		lf := NewLazyFrame(NewScanNode(NewParquetSource(filename))).
			Filter(expr.Col("city").Eq("NYC")).
			SelectColumns("name", "score", "city")  // Need to keep city for now
		
		result, err := lf.Collect()
		require.NoError(t, err)

		assert.Equal(t, 2, result.Height()) // Alice and David
		assert.Equal(t, 3, result.Width())
		assert.Equal(t, []string{"name", "score", "city"}, result.Columns())
		
		nameCol, _ := result.Column("name")
		assert.Equal(t, "Alice", nameCol.Get(0))
		assert.Equal(t, "David", nameCol.Get(1))
	})

	// Test 5: Query optimization with predicate pushdown
	t.Run("predicate_pushdown", func(t *testing.T) {
		lf := NewLazyFrame(NewScanNode(NewParquetSource(filename))).
			SelectColumns("id", "name", "score").
			Filter(expr.Col("score").Gt(85))
		
		// Get optimized plan
		optimizedPlan, err := lf.ExplainOptimized()
		require.NoError(t, err)
		
		// The optimized plan should show that the filter was pushed down
		assert.Contains(t, optimizedPlan, "Parquet[")
		assert.Contains(t, optimizedPlan, "Filters:")
		
		// Execute and verify results
		result, err := lf.Collect()
		require.NoError(t, err)
		
		assert.Equal(t, 4, result.Height()) // All except David (78.9)
	})
}

func TestLazyParquetIntegration(t *testing.T) {
	// Create larger test data for more complex operations
	size := 1000
	ids := make([]int32, size)
	names := make([]string, size)
	scores := make([]float64, size)
	categories := make([]string, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int32(i)
		names[i] = []string{"Alice", "Bob", "Charlie", "David", "Eve"}[i%5]
		scores[i] = float64(50 + (i%50))
		categories[i] = []string{"A", "B", "C"}[i%3]
	}

	df, err := frame.NewDataFrame(
		series.NewInt32Series("id", ids),
		series.NewStringSeries("name", names),
		series.NewFloat64Series("score", scores),
		series.NewStringSeries("category", categories),
	)
	require.NoError(t, err)

	// Write to parquet
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_integration.parquet")
	err = io.WriteParquet(df, filename)
	require.NoError(t, err)

	// Test complex lazy operations
	t.Run("complex_operations", func(t *testing.T) {
		lf := NewLazyFrame(NewScanNode(NewParquetSource(filename))).
			Filter(expr.Col("score").Gt(70)).
			GroupBy("category").
			Agg(map[string]expr.Expr{
				"avg_score": expr.Col("score").Mean(),
				"count":     expr.Col("category").Count(),
			}).
			Sort("avg_score", true)
		
		result, err := lf.Collect()
		require.NoError(t, err)
		
		// Should have 3 categories
		assert.Equal(t, 3, result.Height())
		assert.Equal(t, 3, result.Width())
		
		// Check that results are sorted by avg_score descending
		avgScoreCol, _ := result.Column("avg_score")
		for i := 1; i < avgScoreCol.Len(); i++ {
			prev := avgScoreCol.Get(i - 1).(float64)
			curr := avgScoreCol.Get(i).(float64)
			assert.GreaterOrEqual(t, prev, curr)
		}
	})
}

func TestParquetSourceNotFound(t *testing.T) {
	// Test error handling for non-existent file
	lf := NewLazyFrame(NewScanNode(NewParquetSource("/non/existent/file.parquet")))
	
	_, err := lf.Collect()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read Parquet")
}