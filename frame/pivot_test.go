package frame

import (
	"testing"

	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPivot(t *testing.T) {
	t.Run("Basic pivot", func(t *testing.T) {
		// Create a long format DataFrame
		df, err := NewDataFrame(
			series.NewStringSeries("date", []string{"2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"}),
			series.NewStringSeries("product", []string{"A", "B", "A", "B"}),
			series.NewInt64Series("sales", []int64{100, 150, 120, 180}),
		)
		require.NoError(t, err)
		
		// Pivot with date as index, product as columns, sales as values
		pivoted, err := df.Pivot(PivotOptions{
			Index:   []string{"date"},
			Columns: "product",
			Values:  "sales",
		})
		require.NoError(t, err)
		
		// Should have 2 rows (2 unique dates)
		assert.Equal(t, 2, pivoted.Height())
		
		// Check columns
		assert.Equal(t, []string{"date", "A", "B"}, pivoted.Columns())
		
		// Check values
		dateCol, err := pivoted.Column("date")
		require.NoError(t, err)
		assert.Equal(t, "2024-01-01", dateCol.Get(0))
		assert.Equal(t, "2024-01-02", dateCol.Get(1))
		
		aCol, err := pivoted.Column("A")
		require.NoError(t, err)
		assert.Equal(t, int64(100), aCol.Get(0))
		assert.Equal(t, int64(120), aCol.Get(1))
		
		bCol, err := pivoted.Column("B")
		require.NoError(t, err)
		assert.Equal(t, int64(150), bCol.Get(0))
		assert.Equal(t, int64(180), bCol.Get(1))
	})
	
	t.Run("Pivot with aggregation", func(t *testing.T) {
		// Create data with duplicates that need aggregation
		df, err := NewDataFrame(
			series.NewStringSeries("month", []string{"Jan", "Jan", "Jan", "Feb", "Feb", "Feb"}),
			series.NewStringSeries("category", []string{"A", "A", "B", "A", "B", "B"}),
			series.NewInt64Series("amount", []int64{10, 20, 30, 40, 50, 60}),
		)
		require.NoError(t, err)
		
		// Pivot with sum aggregation
		pivoted, err := df.Pivot(PivotOptions{
			Index:   []string{"month"},
			Columns: "category", 
			Values:  "amount",
			AggFunc: "sum",
		})
		require.NoError(t, err)
		
		assert.Equal(t, 2, pivoted.Height())
		assert.Equal(t, []string{"month", "A", "B"}, pivoted.Columns())
		
		// Jan: A=10+20=30, B=30
		aCol, err := pivoted.Column("A")
		require.NoError(t, err)
		assert.Equal(t, int64(30), aCol.Get(0))
		assert.Equal(t, int64(40), aCol.Get(1))
		
		bCol, err := pivoted.Column("B")
		require.NoError(t, err)
		assert.Equal(t, int64(30), bCol.Get(0))
		assert.Equal(t, int64(110), bCol.Get(1))
	})
	
	t.Run("Pivot with mean aggregation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("group", []string{"X", "X", "Y", "Y"}),
			series.NewStringSeries("type", []string{"A", "A", "A", "B"}),
			series.NewFloat64Series("value", []float64{10.0, 20.0, 30.0, 40.0}),
		)
		require.NoError(t, err)
		
		pivoted, err := df.Pivot(PivotOptions{
			Index:   []string{"group"},
			Columns: "type",
			Values:  "value",
			AggFunc: "mean",
		})
		require.NoError(t, err)
		
		// X: A=(10+20)/2=15, B=nil
		// Y: A=30, B=40
		aCol, err := pivoted.Column("A")
		require.NoError(t, err)
		assert.Equal(t, 15.0, aCol.Get(0))
		assert.Equal(t, 30.0, aCol.Get(1))
		
		bCol, err := pivoted.Column("B")
		require.NoError(t, err)
		assert.True(t, bCol.IsNull(0))
		assert.Equal(t, 40.0, bCol.Get(1))
	})
	
	t.Run("Pivot with fill value", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("x", []string{"a", "b"}),
			series.NewStringSeries("y", []string{"1", "2"}),
			series.NewInt64Series("val", []int64{10, 20}),
		)
		require.NoError(t, err)
		
		pivoted, err := df.Pivot(PivotOptions{
			Index:     []string{"x"},
			Columns:   "y",
			Values:    "val",
			FillValue: int64(0),
		})
		require.NoError(t, err)
		
		// Should fill missing combinations with 0
		col1, err := pivoted.Column("1")
		require.NoError(t, err)
		assert.Equal(t, int64(10), col1.Get(0)) // a,1
		assert.Equal(t, int64(0), col1.Get(1))  // b,1 (filled)
		
		col2, err := pivoted.Column("2")
		require.NoError(t, err)
		assert.Equal(t, int64(0), col2.Get(0))  // a,2 (filled)
		assert.Equal(t, int64(20), col2.Get(1)) // b,2
	})
	
	t.Run("Multiple index columns", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("year", []string{"2023", "2023", "2024", "2024"}),
			series.NewStringSeries("quarter", []string{"Q1", "Q2", "Q1", "Q2"}),
			series.NewStringSeries("product", []string{"A", "A", "A", "A"}),
			series.NewInt64Series("revenue", []int64{100, 120, 130, 150}),
		)
		require.NoError(t, err)
		
		pivoted, err := df.Pivot(PivotOptions{
			Index:   []string{"year", "quarter"},
			Columns: "product",
			Values:  "revenue",
		})
		require.NoError(t, err)
		
		assert.Equal(t, 4, pivoted.Height())
		assert.Equal(t, []string{"year", "quarter", "A"}, pivoted.Columns())
	})
	
	t.Run("Error cases", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewStringSeries("a", []string{"x", "y"}),
			series.NewInt64Series("b", []int64{1, 2}),
		)
		require.NoError(t, err)
		
		// Missing columns parameter
		_, err = df.Pivot(PivotOptions{
			Values: "b",
		})
		assert.Error(t, err)
		
		// Missing values parameter
		_, err = df.Pivot(PivotOptions{
			Columns: "a",
		})
		assert.Error(t, err)
		
		// Invalid column names
		_, err = df.Pivot(PivotOptions{
			Columns: "nonexistent",
			Values:  "b",
		})
		assert.Error(t, err)
		
		_, err = df.Pivot(PivotOptions{
			Columns: "a",
			Values:  "nonexistent",
		})
		assert.Error(t, err)
		
		_, err = df.Pivot(PivotOptions{
			Index:   []string{"nonexistent"},
			Columns: "a",
			Values:  "b",
		})
		assert.Error(t, err)
	})
}