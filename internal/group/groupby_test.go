package group

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// mockDataFrame implements DataFrameInterface for testing
type mockDataFrame struct {
	columns map[string]series.Series
	height  int
}

func newMockDataFrame(cols ...series.Series) *mockDataFrame {
	df := &mockDataFrame{
		columns: make(map[string]series.Series),
	}
	if len(cols) > 0 {
		df.height = cols[0].Len()
		for _, col := range cols {
			df.columns[col.Name()] = col
		}
	}
	return df
}

func (df *mockDataFrame) Column(name string) (series.Series, error) {
	col, exists := df.columns[name]
	if !exists {
		return nil, fmt.Errorf("column %s not found", name)
	}
	return col, nil
}

func (df *mockDataFrame) Height() int {
	return df.height
}

func TestGroupByCreation(t *testing.T) {
	// Create test DataFrame
	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
		series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
		series.NewFloat64Series("score", []float64{1.1, 2.2, 3.3, 4.4, 5.5}),
	)

	// Test single column groupby
	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)
	assert.NotNil(t, gb)
	assert.Equal(t, 2, gb.Groups()) // Should have 2 groups: A and B
}

func TestGroupByCount(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
		series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
	)

	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)

	result, err := gb.Count()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.Columns)) // category + count columns

	// Check count column exists
	var countCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "count" {
			countCol = col
			break
		}
	}
	assert.NotNil(t, countCol)
	assert.True(t, countCol.DataType().Equals(datatypes.Int64{}))

	// Verify counts (order may vary due to map iteration)
	counts := make(map[string]int64)
	var catCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "category" {
			catCol = col
			break
		}
	}
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		count := countCol.Get(i).(int64)
		counts[cat] = count
	}

	assert.Equal(t, int64(3), counts["A"]) // A appears 3 times
	assert.Equal(t, int64(2), counts["B"]) // B appears 2 times
}

func TestGroupBySum(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
		series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
		series.NewFloat64Series("score", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)

	// Sum single column
	result, err := gb.Sum("value")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.Columns)) // category + value_sum

	// Check results
	var sumCol, catCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "value_sum" {
			sumCol = col
		} else if col.Name() == "category" {
			catCol = col
		}
	}
	assert.NotNil(t, sumCol)
	assert.NotNil(t, catCol)

	sums := make(map[string]int32)
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		sum := sumCol.Get(i).(int32)
		sums[cat] = sum
	}

	assert.Equal(t, int32(9), sums["A"]) // 1 + 3 + 5 = 9
	assert.Equal(t, int32(6), sums["B"]) // 2 + 4 = 6

	// Sum multiple columns
	result, err = gb.Sum("value", "score")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Columns)) // category + value_sum + score_sum
}

func TestGroupByMean(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
		series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
	)

	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)

	result, err := gb.Mean("value")
	assert.NoError(t, err)

	var meanCol, catCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "value_mean" {
			meanCol = col
		} else if col.Name() == "category" {
			catCol = col
		}
	}
	assert.NotNil(t, meanCol)
	assert.True(t, meanCol.DataType().Equals(datatypes.Float64{})) // Mean always returns float64

	means := make(map[string]float64)
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		mean := meanCol.Get(i).(float64)
		means[cat] = mean
	}

	assert.InDelta(t, 3.0, means["A"], 0.001) // (1 + 3 + 5) / 3 = 3
	assert.InDelta(t, 3.0, means["B"], 0.001) // (2 + 4) / 2 = 3
}

func TestGroupByMinMax(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
		series.NewInt32Series("value", []int32{1, 2, 3, 4, 5}),
	)

	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)

	// Test Min
	result, err := gb.Min("value")
	assert.NoError(t, err)

	var minCol, catCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "value_min" {
			minCol = col
		} else if col.Name() == "category" {
			catCol = col
		}
	}
	mins := make(map[string]int32)
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		min := minCol.Get(i).(int32)
		mins[cat] = min
	}

	assert.Equal(t, int32(1), mins["A"])
	assert.Equal(t, int32(2), mins["B"])

	// Test Max
	result, err = gb.Max("value")
	assert.NoError(t, err)

	var maxCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "value_max" {
			maxCol = col
		} else if col.Name() == "category" {
			catCol = col
		}
	}
	maxs := make(map[string]int32)
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		max := maxCol.Get(i).(int32)
		maxs[cat] = max
	}

	assert.Equal(t, int32(5), maxs["A"])
	assert.Equal(t, int32(4), maxs["B"])
}

func TestGroupByMultipleColumns(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("year", []string{"2021", "2021", "2022", "2022", "2021", "2022"}),
		series.NewStringSeries("month", []string{"Jan", "Feb", "Jan", "Feb", "Jan", "Jan"}),
		series.NewInt32Series("sales", []int32{100, 200, 150, 250, 120, 180}),
	)

	gb, err := NewGroupBy(df, []string{"year", "month"})
	assert.NoError(t, err)

	result, err := gb.Sum("sales")
	assert.NoError(t, err)

	// Should have 4 groups: (2021,Jan), (2021,Feb), (2022,Jan), (2022,Feb)
	assert.Equal(t, 3, len(result.Columns)) // year + month + sales_sum

	// Get columns
	var yearCol, monthCol, sumCol series.Series
	for _, col := range result.Columns {
		switch col.Name() {
		case "year":
			yearCol = col
		case "month":
			monthCol = col
		case "sales_sum":
			sumCol = col
		}
	}

	sums := make(map[string]int32)
	for i := 0; i < yearCol.Len(); i++ {
		year := yearCol.Get(i).(string)
		month := monthCol.Get(i).(string)
		sum := sumCol.Get(i).(int32)
		key := year + "-" + month
		sums[key] = sum
	}

	assert.Equal(t, int32(220), sums["2021-Jan"]) // 100 + 120 = 220
	assert.Equal(t, int32(200), sums["2021-Feb"]) // 200
	assert.Equal(t, int32(330), sums["2022-Jan"]) // 150 + 180 = 330
	assert.Equal(t, int32(250), sums["2022-Feb"]) // 250
}

func TestGroupByWithNulls(t *testing.T) {
	// Create series with null values
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	validity := []bool{true, false, true, true, true} // Second value is null

	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A", "B", "A"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)

	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)

	// Sum should ignore nulls
	result, err := gb.Sum("value")
	assert.NoError(t, err)

	var sumCol, catCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "value_sum" {
			sumCol = col
		} else if col.Name() == "category" {
			catCol = col
		}
	}

	sums := make(map[string]float64)
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		sum := sumCol.Get(i).(float64)
		sums[cat] = sum
	}

	assert.InDelta(t, 9.0, sums["A"], 0.001) // 1 + 3 + 5 = 9
	assert.InDelta(t, 4.0, sums["B"], 0.001) // null + 4 = 4 (null ignored)

	// Mean should also ignore nulls
	result, err = gb.Mean("value")
	assert.NoError(t, err)

	var meanCol series.Series
	for _, col := range result.Columns {
		if col.Name() == "value_mean" {
			meanCol = col
		} else if col.Name() == "category" {
			catCol = col
		}
	}
	means := make(map[string]float64)
	for i := 0; i < catCol.Len(); i++ {
		cat := catCol.Get(i).(string)
		mean := meanCol.Get(i).(float64)
		means[cat] = mean
	}

	assert.InDelta(t, 3.0, means["A"], 0.001) // (1 + 3 + 5) / 3 = 3
	assert.InDelta(t, 4.0, means["B"], 0.001) // 4 / 1 = 4 (only one non-null value)
}

func TestGroupByErrors(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("category", []string{"A", "B", "A"}),
		series.NewInt32Series("value", []int32{1, 2, 3}),
	)

	// Test groupby with non-existent column
	_, err := NewGroupBy(df, []string{"nonexistent"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test aggregation on non-existent column
	gb, err := NewGroupBy(df, []string{"category"})
	assert.NoError(t, err)

	_, err = gb.Sum("nonexistent")
	assert.Error(t, err)
}

func BenchmarkGroupBy(b *testing.B) {
	// Create larger dataset
	size := 10000
	categories := []string{"A", "B", "C", "D", "E"}
	catData := make([]string, size)
	valueData := make([]int32, size)

	for i := 0; i < size; i++ {
		catData[i] = categories[i%len(categories)]
		valueData[i] = int32(i)
	}

	df := newMockDataFrame(
		series.NewStringSeries("category", catData),
		series.NewInt32Series("value", valueData),
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		gb, _ := NewGroupBy(df, []string{"category"})
		_, _ = gb.Sum("value")
	}
}

func BenchmarkGroupByMultipleColumns(b *testing.B) {
	size := 10000
	years := []string{"2020", "2021", "2022"}
	months := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun"}

	yearData := make([]string, size)
	monthData := make([]string, size)
	valueData := make([]float64, size)

	for i := 0; i < size; i++ {
		yearData[i] = years[i%len(years)]
		monthData[i] = months[i%len(months)]
		valueData[i] = float64(i) * 1.5
	}

	df := newMockDataFrame(
		series.NewStringSeries("year", yearData),
		series.NewStringSeries("month", monthData),
		series.NewFloat64Series("value", valueData),
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		gb, _ := NewGroupBy(df, []string{"year", "month"})
		_, _ = gb.Mean("value")
	}
}
