package frame

import (
	"fmt"
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDataFrame(t *testing.T) {
	s1 := series.NewInt32Series("id", []int32{1, 2, 3})
	s2 := series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"})
	s3 := series.NewFloat64Series("score", []float64{95.5, 87.3, 92.1})
	
	df, err := NewDataFrame(s1, s2, s3)
	require.NoError(t, err)
	assert.NotNil(t, df)
	
	assert.Equal(t, 3, df.Height())
	assert.Equal(t, 3, df.Width())
	
	cols := df.Columns()
	assert.Equal(t, []string{"id", "name", "score"}, cols)
}

func TestNewDataFrameFromMap(t *testing.T) {
	data := map[string]interface{}{
		"id":   []int64{1, 2, 3, 4},
		"name": []string{"A", "B", "C", "D"},
		"age":  []int32{25, 30, 35, 40},
	}
	
	df, err := NewDataFrameFromMap(data)
	require.NoError(t, err)
	assert.NotNil(t, df)
	
	assert.Equal(t, 4, df.Height())
	assert.Equal(t, 3, df.Width())
}

func TestDataFrameValidation(t *testing.T) {
	// Test mismatched column lengths
	s1 := series.NewInt32Series("a", []int32{1, 2, 3})
	s2 := series.NewInt32Series("b", []int32{4, 5})
	
	_, err := NewDataFrame(s1, s2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "same length")
}

func TestEmptyDataFrame(t *testing.T) {
	df, err := NewDataFrame()
	require.NoError(t, err)
	assert.NotNil(t, df)
	
	assert.Equal(t, 0, df.Height())
	assert.Equal(t, 0, df.Width())
	assert.True(t, df.IsEmpty())
}

func TestDataFrameColumn(t *testing.T) {
	s1 := series.NewInt32Series("col1", []int32{1, 2, 3})
	s2 := series.NewStringSeries("col2", []string{"a", "b", "c"})
	
	df, err := NewDataFrame(s1, s2)
	require.NoError(t, err)
	
	// Test Column by name
	col, err := df.Column("col1")
	require.NoError(t, err)
	assert.Equal(t, "col1", col.Name())
	assert.Equal(t, 3, col.Len())
	
	// Test non-existent column
	_, err = df.Column("nonexistent")
	assert.Error(t, err)
	
	// Test ColumnAt
	col, err = df.ColumnAt(1)
	require.NoError(t, err)
	assert.Equal(t, "col2", col.Name())
	
	// Test out of range
	_, err = df.ColumnAt(2)
	assert.Error(t, err)
	
	_, err = df.ColumnAt(-1)
	assert.Error(t, err)
}

func TestDataFrameSelect(t *testing.T) {
	s1 := series.NewInt32Series("a", []int32{1, 2, 3})
	s2 := series.NewInt32Series("b", []int32{4, 5, 6})
	s3 := series.NewInt32Series("c", []int32{7, 8, 9})
	
	df, err := NewDataFrame(s1, s2, s3)
	require.NoError(t, err)
	
	// Select subset of columns
	selected, err := df.Select("a", "c")
	require.NoError(t, err)
	assert.Equal(t, 2, selected.Width())
	assert.Equal(t, []string{"a", "c"}, selected.Columns())
	
	// Select non-existent column
	_, err = df.Select("a", "nonexistent")
	assert.Error(t, err)
}

func TestDataFrameDrop(t *testing.T) {
	s1 := series.NewInt32Series("a", []int32{1, 2, 3})
	s2 := series.NewInt32Series("b", []int32{4, 5, 6})
	s3 := series.NewInt32Series("c", []int32{7, 8, 9})
	
	df, err := NewDataFrame(s1, s2, s3)
	require.NoError(t, err)
	
	// Drop one column
	dropped, err := df.Drop("b")
	require.NoError(t, err)
	assert.Equal(t, 2, dropped.Width())
	assert.Equal(t, []string{"a", "c"}, dropped.Columns())
	
	// Drop multiple columns
	dropped, err = df.Drop("a", "c")
	require.NoError(t, err)
	assert.Equal(t, 1, dropped.Width())
	assert.Equal(t, []string{"b"}, dropped.Columns())
	
	// Drop non-existent column
	_, err = df.Drop("nonexistent")
	assert.Error(t, err)
}

func TestDataFrameHeadTail(t *testing.T) {
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i)
	}
	s := series.NewInt64Series("nums", values)
	df, err := NewDataFrame(s)
	require.NoError(t, err)
	
	// Test Head
	head := df.Head(5)
	assert.Equal(t, 5, head.Height())
	
	row, err := head.GetRow(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), row["nums"])
	
	row, err = head.GetRow(4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), row["nums"])
	
	// Test Tail
	tail := df.Tail(5)
	assert.Equal(t, 5, tail.Height())
	
	row, err = tail.GetRow(0)
	require.NoError(t, err)
	assert.Equal(t, int64(95), row["nums"])
	
	row, err = tail.GetRow(4)
	require.NoError(t, err)
	assert.Equal(t, int64(99), row["nums"])
	
	// Test edge cases
	assert.Equal(t, 100, df.Head(200).Height())
	assert.Equal(t, 100, df.Tail(200).Height())
}

func TestDataFrameSlice(t *testing.T) {
	s := series.NewInt32Series("vals", []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	df, err := NewDataFrame(s)
	require.NoError(t, err)
	
	// Normal slice
	sliced, err := df.Slice(2, 7)
	require.NoError(t, err)
	assert.Equal(t, 5, sliced.Height())
	
	row, err := sliced.GetRow(0)
	require.NoError(t, err)
	assert.Equal(t, int32(2), row["vals"])
	
	// Invalid slices
	_, err = df.Slice(-1, 5)
	assert.Error(t, err)
	
	_, err = df.Slice(0, 11)
	assert.Error(t, err)
	
	_, err = df.Slice(5, 3)
	assert.Error(t, err)
}

func TestDataFrameGetRow(t *testing.T) {
	s1 := series.NewInt32Series("id", []int32{1, 2, 3})
	s2 := series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"})
	s3 := series.NewBooleanSeries("active", []bool{true, false, true})
	
	df, err := NewDataFrame(s1, s2, s3)
	require.NoError(t, err)
	
	// Get valid row
	row, err := df.GetRow(1)
	require.NoError(t, err)
	assert.Equal(t, int32(2), row["id"])
	assert.Equal(t, "Bob", row["name"])
	assert.Equal(t, false, row["active"])
	
	// Get invalid row
	_, err = df.GetRow(-1)
	assert.Error(t, err)
	
	_, err = df.GetRow(3)
	assert.Error(t, err)
}

func TestDataFrameClone(t *testing.T) {
	s1 := series.NewFloat64Series("x", []float64{1.1, 2.2, 3.3})
	s2 := series.NewFloat64Series("y", []float64{4.4, 5.5, 6.6})
	
	df, err := NewDataFrame(s1, s2)
	require.NoError(t, err)
	cloned := df.Clone()
	
	assert.Equal(t, df.Height(), cloned.Height())
	assert.Equal(t, df.Width(), cloned.Width())
	assert.Equal(t, df.Columns(), cloned.Columns())
	
	// Verify data is independent
	row1, err := df.GetRow(0)
	require.NoError(t, err)
	row2, err := cloned.GetRow(0)
	require.NoError(t, err)
	assert.Equal(t, row1, row2)
}

func TestDataFrameAddColumn(t *testing.T) {
	s1 := series.NewInt32Series("a", []int32{1, 2, 3})
	s2 := series.NewInt32Series("b", []int32{4, 5, 6})
	
	df, err := NewDataFrame(s1, s2)
	require.NoError(t, err)
	
	// Add new column
	s3 := series.NewInt32Series("c", []int32{7, 8, 9})
	newDf, err := df.AddColumn(s3)
	require.NoError(t, err)
	assert.Equal(t, 3, newDf.Width())
	assert.Equal(t, []string{"a", "b", "c"}, newDf.Columns())
	
	// Add column with wrong length
	s4 := series.NewInt32Series("d", []int32{10, 11})
	_, err = df.AddColumn(s4)
	assert.Error(t, err)
	
	// Add column with duplicate name
	s5 := series.NewInt32Series("a", []int32{10, 11, 12})
	_, err = df.AddColumn(s5)
	assert.Error(t, err)
}

func TestDataFrameRenameColumn(t *testing.T) {
	s1 := series.NewInt32Series("old_name", []int32{1, 2, 3})
	s2 := series.NewInt32Series("other", []int32{4, 5, 6})
	
	df, err := NewDataFrame(s1, s2)
	require.NoError(t, err)
	
	// Rename existing column
	renamed, err := df.RenameColumn("old_name", "new_name")
	require.NoError(t, err)
	assert.Equal(t, []string{"new_name", "other"}, renamed.Columns())
	
	// Rename non-existent column
	_, err = df.RenameColumn("nonexistent", "new")
	assert.Error(t, err)
	
	// Rename to existing name
	_, err = df.RenameColumn("old_name", "other")
	assert.Error(t, err)
}

func TestDataFrameShape(t *testing.T) {
	s1 := series.NewInt32Series("a", []int32{1, 2, 3, 4, 5})
	s2 := series.NewInt32Series("b", []int32{6, 7, 8, 9, 10})
	s3 := series.NewInt32Series("c", []int32{11, 12, 13, 14, 15})
	
	df, err := NewDataFrame(s1, s2, s3)
	require.NoError(t, err)
	
	height, width := df.Shape()
	assert.Equal(t, 5, height)
	assert.Equal(t, 3, width)
}

func TestDataFrameString(t *testing.T) {
	s1 := series.NewInt32Series("id", []int32{1, 2, 3})
	s2 := series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"})
	
	df, err := NewDataFrame(s1, s2)
	require.NoError(t, err)
	
	str := df.String()
	assert.Contains(t, str, "DataFrame: 3 × 2")
	assert.Contains(t, str, "id")
	assert.Contains(t, str, "name")
	assert.Contains(t, str, "Alice")
	assert.Contains(t, str, "Charlie")
}

func TestDataFrameWithNulls(t *testing.T) {
	values := []float64{1.1, 2.2, 3.3, 4.4}
	validity := []bool{true, false, true, false}
	
	s := series.NewSeriesWithValidity("nums", values, validity, datatypes.Float64{})
	df, err := NewDataFrame(s)
	require.NoError(t, err)
	
	row, err := df.GetRow(1)
	require.NoError(t, err)
	assert.Nil(t, row["nums"])
	
	row, err = df.GetRow(2)
	require.NoError(t, err)
	assert.Equal(t, 3.3, row["nums"])
}

func BenchmarkDataFrameCreation(b *testing.B) {
	cols := make([]series.Series, 10)
	for i := 0; i < 10; i++ {
		values := make([]int64, 10000)
		for j := range values {
			values[j] = int64(j)
		}
		cols[i] = series.NewInt64Series(fmt.Sprintf("col_%d", i), values)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df, err := NewDataFrame(cols...)
		if err != nil {
			b.Fatal(err)
		}
		_ = df.Height()
	}
}

func BenchmarkDataFrameSelect(b *testing.B) {
	cols := make([]series.Series, 20)
	for i := 0; i < 20; i++ {
		values := make([]int64, 1000)
		for j := range values {
			values[j] = int64(j)
		}
		cols[i] = series.NewInt64Series(fmt.Sprintf("col_%d", i), values)
	}
	
	df, err := NewDataFrame(cols...)
	if err != nil {
		b.Fatal(err)
	}
	selectCols := []string{"col_5", "col_10", "col_15"}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected, err := df.Select(selectCols...)
		if err != nil {
			b.Fatal(err)
		}
		_ = selected.Width()
	}
}