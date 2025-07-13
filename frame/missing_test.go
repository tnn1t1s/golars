package frame

import (
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwardFill(t *testing.T) {
	t.Run("Basic forward fill", func(t *testing.T) {
		// Create series with nulls
		values := []float64{1.0, 0, 0, 4.0, 0, 6.0}
		validity := []bool{true, false, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Forward fill
		filled, err := df.ForwardFill()
		require.NoError(t, err)
		
		col, err := filled.Column("col")
		require.NoError(t, err)
		
		// Check values
		assert.Equal(t, 1.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1)) // Filled with previous value
		assert.Equal(t, 1.0, col.Get(2)) // Filled with previous value
		assert.Equal(t, 4.0, col.Get(3))
		assert.Equal(t, 4.0, col.Get(4)) // Filled with previous value
		assert.Equal(t, 6.0, col.Get(5))
	})
	
	t.Run("Forward fill with limit", func(t *testing.T) {
		values := []float64{1.0, 0, 0, 0, 5.0}
		validity := []bool{true, false, false, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Forward fill with limit of 2
		filled, err := df.FillNull(FillNullOptions{
			Method: "forward",
			Limit:  2,
		})
		require.NoError(t, err)
		
		col, err := filled.Column("col")
		require.NoError(t, err)
		
		// Check values
		assert.Equal(t, 1.0, col.Get(0))
		assert.Equal(t, 1.0, col.Get(1)) // Filled (1st consecutive null)
		assert.Equal(t, 1.0, col.Get(2)) // Filled (2nd consecutive null)
		assert.True(t, col.IsNull(3))    // Not filled (exceeds limit)
		assert.Equal(t, 5.0, col.Get(4))
	})
	
	t.Run("Forward fill specific columns", func(t *testing.T) {
		// Create DataFrame with multiple columns
		s1 := series.NewSeriesWithValidity("a", []int64{1, 0, 3}, []bool{true, false, true}, datatypes.Int64{})
		s2 := series.NewSeriesWithValidity("b", []int64{0, 2, 0}, []bool{false, true, false}, datatypes.Int64{})
		
		df, err := NewDataFrame(s1, s2)
		require.NoError(t, err)
		
		// Forward fill only column 'a'
		filled, err := df.ForwardFill("a")
		require.NoError(t, err)
		
		colA, err := filled.Column("a")
		require.NoError(t, err)
		colB, err := filled.Column("b")
		require.NoError(t, err)
		
		// Column 'a' should be filled
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(1), colA.Get(1)) // Filled
		assert.Equal(t, int64(3), colA.Get(2))
		
		// Column 'b' should remain unchanged
		assert.True(t, colB.IsNull(0))
		assert.Equal(t, int64(2), colB.Get(1))
		assert.True(t, colB.IsNull(2))
	})
}

func TestBackwardFill(t *testing.T) {
	t.Run("Basic backward fill", func(t *testing.T) {
		values := []float64{0, 2.0, 0, 0, 5.0, 0}
		validity := []bool{false, true, false, false, true, false}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Backward fill
		filled, err := df.BackwardFill()
		require.NoError(t, err)
		
		col, err := filled.Column("col")
		require.NoError(t, err)
		
		// Check values
		assert.Equal(t, 2.0, col.Get(0)) // Filled with next value
		assert.Equal(t, 2.0, col.Get(1))
		assert.Equal(t, 5.0, col.Get(2)) // Filled with next value
		assert.Equal(t, 5.0, col.Get(3)) // Filled with next value
		assert.Equal(t, 5.0, col.Get(4))
		assert.True(t, col.IsNull(5))    // No next value to fill with
	})
	
	t.Run("Backward fill with limit", func(t *testing.T) {
		values := []float64{0, 0, 0, 4.0}
		validity := []bool{false, false, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Backward fill with limit of 2
		filled, err := df.FillNull(FillNullOptions{
			Method: "backward",
			Limit:  2,
		})
		require.NoError(t, err)
		
		col, err := filled.Column("col")
		require.NoError(t, err)
		
		// Check values
		assert.True(t, col.IsNull(0))    // Not filled (exceeds limit)
		assert.Equal(t, 4.0, col.Get(1)) // Filled (2nd consecutive null from end)
		assert.Equal(t, 4.0, col.Get(2)) // Filled (1st consecutive null from end)
		assert.Equal(t, 4.0, col.Get(3))
	})
}

func TestFillValue(t *testing.T) {
	t.Run("Fill with constant value", func(t *testing.T) {
		s := series.NewSeriesWithValidity("col", []int64{1, 0, 3, 0, 5}, []bool{true, false, true, false, true}, datatypes.Int64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Fill nulls with -1
		filled, err := df.FillNull(FillNullOptions{
			Value: int64(-1),
		})
		require.NoError(t, err)
		
		col, err := filled.Column("col")
		require.NoError(t, err)
		
		// Check values
		assert.Equal(t, int64(1), col.Get(0))
		assert.Equal(t, int64(-1), col.Get(1)) // Filled
		assert.Equal(t, int64(3), col.Get(2))
		assert.Equal(t, int64(-1), col.Get(3)) // Filled
		assert.Equal(t, int64(5), col.Get(4))
	})
	
	t.Run("Fill different types", func(t *testing.T) {
		// String column
		s1 := series.NewSeriesWithValidity("str", []string{"a", "", "c"}, []bool{true, false, true}, datatypes.String{})
		// Float column
		s2 := series.NewSeriesWithValidity("float", []float64{1.1, 0, 3.3}, []bool{true, false, true}, datatypes.Float64{})
		
		df, err := NewDataFrame(s1, s2)
		require.NoError(t, err)
		
		// Fill string column with "missing"
		filled1, err := df.FillNull(FillNullOptions{
			Value:   "missing",
			Columns: []string{"str"},
		})
		require.NoError(t, err)
		
		// Fill float column with NaN
		filled2, err := filled1.FillNull(FillNullOptions{
			Value:   0.0,
			Columns: []string{"float"},
		})
		require.NoError(t, err)
		
		strCol, err := filled2.Column("str")
		require.NoError(t, err)
		floatCol, err := filled2.Column("float")
		require.NoError(t, err)
		
		assert.Equal(t, "missing", strCol.Get(1))
		assert.Equal(t, 0.0, floatCol.Get(1))
	})
}

func TestDropNull(t *testing.T) {
	t.Run("Drop rows with any null", func(t *testing.T) {
		s1 := series.NewSeriesWithValidity("a", []int64{1, 2, 3, 4}, []bool{true, false, true, true}, datatypes.Int64{})
		s2 := series.NewSeriesWithValidity("b", []int64{5, 6, 7, 8}, []bool{true, true, false, true}, datatypes.Int64{})
		
		df, err := NewDataFrame(s1, s2)
		require.NoError(t, err)
		
		// Drop rows with any null
		cleaned, err := df.DropNull()
		require.NoError(t, err)
		
		// Should only keep rows 0 and 3
		assert.Equal(t, 2, cleaned.Height())
		
		colA, err := cleaned.Column("a")
		require.NoError(t, err)
		colB, err := cleaned.Column("b")
		require.NoError(t, err)
		
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(4), colA.Get(1))
		assert.Equal(t, int64(5), colB.Get(0))
		assert.Equal(t, int64(8), colB.Get(1))
	})
	
	t.Run("Drop null from subset", func(t *testing.T) {
		s1 := series.NewSeriesWithValidity("a", []int64{1, 2, 3}, []bool{true, false, true}, datatypes.Int64{})
		s2 := series.NewSeriesWithValidity("b", []int64{4, 5, 6}, []bool{false, true, true}, datatypes.Int64{})
		
		df, err := NewDataFrame(s1, s2)
		require.NoError(t, err)
		
		// Drop rows where column 'a' is null
		cleaned, err := df.DropNull("a")
		require.NoError(t, err)
		
		// Should keep rows 0 and 2 (where 'a' is not null)
		assert.Equal(t, 2, cleaned.Height())
		
		colA, err := cleaned.Column("a")
		require.NoError(t, err)
		colB, err := cleaned.Column("b")
		require.NoError(t, err)
		
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(3), colA.Get(1))
		assert.True(t, colB.IsNull(0))  // b is null in row 0
		assert.Equal(t, int64(6), colB.Get(1))
	})
}

func TestDropDuplicates(t *testing.T) {
	t.Run("Drop duplicate rows", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 1, 2, 2, 3}),
			series.NewStringSeries("b", []string{"x", "x", "y", "y", "z"}),
		)
		require.NoError(t, err)
		
		// Drop duplicates keeping first
		deduped, err := df.DropDuplicates(DropDuplicatesOptions{
			Keep: "first",
		})
		require.NoError(t, err)
		
		assert.Equal(t, 3, deduped.Height())
		
		colA, err := deduped.Column("a")
		require.NoError(t, err)
		colB, err := deduped.Column("b")
		require.NoError(t, err)
		
		// Should keep first occurrence of each unique combination
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(2), colA.Get(1))
		assert.Equal(t, int64(3), colA.Get(2))
		assert.Equal(t, "x", colB.Get(0))
		assert.Equal(t, "y", colB.Get(1))
		assert.Equal(t, "z", colB.Get(2))
	})
	
	t.Run("Drop duplicates keeping last", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 1, 2}),
			series.NewStringSeries("b", []string{"x", "y", "z"}),
		)
		require.NoError(t, err)
		
		// Drop duplicates on column 'a' keeping last
		deduped, err := df.DropDuplicates(DropDuplicatesOptions{
			Subset: []string{"a"},
			Keep:   "last",
		})
		require.NoError(t, err)
		
		assert.Equal(t, 2, deduped.Height())
		
		colA, err := deduped.Column("a")
		require.NoError(t, err)
		colB, err := deduped.Column("b")
		require.NoError(t, err)
		
		// Should keep last occurrence
		assert.Equal(t, int64(1), colA.Get(0))
		assert.Equal(t, int64(2), colA.Get(1))
		assert.Equal(t, "y", colB.Get(0)) // Last occurrence of a=1
		assert.Equal(t, "z", colB.Get(1))
	})
	
	t.Run("Drop duplicates keeping none", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("a", []int64{1, 1, 2, 3, 3}),
		)
		require.NoError(t, err)
		
		// Drop all duplicates
		deduped, err := df.DropDuplicates(DropDuplicatesOptions{
			Keep: "none",
		})
		require.NoError(t, err)
		
		assert.Equal(t, 1, deduped.Height())
		
		colA, err := deduped.Column("a")
		require.NoError(t, err)
		
		// Should only keep non-duplicated value
		assert.Equal(t, int64(2), colA.Get(0))
	})
}