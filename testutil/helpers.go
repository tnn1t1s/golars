package testutil

import (
	"fmt"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// CreateTestDataFrame creates a standard DataFrame for testing with the specified number of rows.
// The DataFrame contains columns: id (int32), name (string), value (float64), active (bool)
func CreateTestDataFrame(t *testing.T, rows int) *frame.DataFrame {
	t.Helper()

	if rows < 0 {
		t.Fatalf("rows must be non-negative, got %d", rows)
	}

	ids := make([]int32, rows)
	names := make([]string, rows)
	values := make([]float64, rows)
	actives := make([]bool, rows)

	for i := 0; i < rows; i++ {
		ids[i] = int32(i + 1)
		names[i] = fmt.Sprintf("Person_%d", i+1)
		values[i] = float64(i+1) * 10.5
		actives[i] = i%2 == 0
	}

	df, err := frame.NewDataFrame(
		series.NewInt32Series("id", ids),
		series.NewStringSeries("name", names),
		series.NewFloat64Series("value", values),
		series.NewBooleanSeries("active", actives),
	)
	if err != nil {
		t.Fatalf("failed to create test DataFrame: %v", err)
	}

	return df
}

// CreateTestSeries creates a test series with the specified type and size
func CreateTestSeries(t *testing.T, name string, dtype datatypes.DataType, size int) series.Series {
	t.Helper()

	switch dtype.(type) {
	case datatypes.Int32:
		values := make([]int32, size)
		for i := 0; i < size; i++ {
			values[i] = int32(i + 1)
		}
		return series.NewInt32Series(name, values)

	case datatypes.Int64:
		values := make([]int64, size)
		for i := 0; i < size; i++ {
			values[i] = int64(i + 1)
		}
		return series.NewInt64Series(name, values)

	case datatypes.Float64:
		values := make([]float64, size)
		for i := 0; i < size; i++ {
			values[i] = float64(i+1) * 1.5
		}
		return series.NewFloat64Series(name, values)

	case datatypes.String:
		values := make([]string, size)
		for i := 0; i < size; i++ {
			values[i] = fmt.Sprintf("value_%d", i+1)
		}
		return series.NewStringSeries(name, values)

	case datatypes.Boolean:
		values := make([]bool, size)
		for i := 0; i < size; i++ {
			values[i] = i%2 == 0
		}
		return series.NewBooleanSeries(name, values)

	default:
		t.Fatalf("unsupported data type for test series: %v", dtype)
		return nil
	}
}

// CreateTestSeriesWithNulls creates a test series with null values at specified positions
func CreateTestSeriesWithNulls(t *testing.T, name string, dtype datatypes.DataType, size int, nullPositions []int) series.Series {
	t.Helper()

	// Create validity mask
	validity := make([]bool, size)
	for i := 0; i < size; i++ {
		validity[i] = true
	}
	for _, pos := range nullPositions {
		if pos >= 0 && pos < size {
			validity[pos] = false
		}
	}

	switch dtype.(type) {
	case datatypes.Int32:
		values := make([]int32, size)
		for i := 0; i < size; i++ {
			values[i] = int32(i + 1)
		}
		return series.NewSeriesWithValidity(name, values, validity, dtype)

	case datatypes.Float64:
		values := make([]float64, size)
		for i := 0; i < size; i++ {
			values[i] = float64(i+1) * 1.5
		}
		return series.NewSeriesWithValidity(name, values, validity, dtype)

	case datatypes.String:
		values := make([]string, size)
		for i := 0; i < size; i++ {
			values[i] = fmt.Sprintf("value_%d", i+1)
		}
		return series.NewSeriesWithValidity(name, values, validity, dtype)

	default:
		t.Fatalf("unsupported data type for test series with nulls: %v", dtype)
		return nil
	}
}

// CreateLargeTestDataFrame creates a large DataFrame for benchmark testing
func CreateLargeTestDataFrame(t testing.TB, rows int) *frame.DataFrame {
	t.Helper()

	// Generate diverse column types
	intCol := make([]int32, rows)
	floatCol := make([]float64, rows)
	stringCol := make([]string, rows)
	boolCol := make([]bool, rows)

	for i := 0; i < rows; i++ {
		intCol[i] = int32(i % 1000)
		floatCol[i] = float64(i) * 0.1
		stringCol[i] = fmt.Sprintf("str_%d", i%100)
		boolCol[i] = i%3 == 0
	}

	df, err := frame.NewDataFrame(
		series.NewInt32Series("int_col", intCol),
		series.NewFloat64Series("float_col", floatCol),
		series.NewStringSeries("string_col", stringCol),
		series.NewBooleanSeries("bool_col", boolCol),
	)
	if err != nil {
		t.Fatalf("failed to create large test DataFrame: %v", err)
	}

	return df
}

// MustCreateDataFrame creates a DataFrame and panics on error.
// Useful for test setup where failure should stop the test.
func MustCreateDataFrame(columns ...series.Series) *frame.DataFrame {
	df, err := frame.NewDataFrame(columns...)
	if err != nil {
		panic(fmt.Sprintf("failed to create DataFrame: %v", err))
	}
	return df
}

// MustCreateDataFrameFrom creates a DataFrame using the new API and panics on error.
func MustCreateDataFrameFrom(data interface{}, options ...golars.DataFrameOption) *frame.DataFrame {
	df, err := golars.DataFrameFrom(data, options...)
	if err != nil {
		panic(fmt.Sprintf("failed to create DataFrame: %v", err))
	}
	return df
}
