package group

import (
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

func TestMedianAggregation(t *testing.T) {
	// Create test data
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)

	// Test median aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"median_value": expr.Col("value").Median(),
	})
	if err != nil {
		t.Fatalf("Median aggregation failed: %v", err)
	}

	// Check result columns
	if result.Columns == nil || len(result.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Check group column
	groupCol := result.Columns[0]
	if groupCol.Name() != "group" {
		t.Errorf("Expected first column to be 'group', got %s", groupCol.Name())
	}

	// Check median column
	medianCol := result.Columns[1]
	if medianCol.Name() != "median_value" {
		t.Errorf("Expected second column to be 'median_value', got %s", medianCol.Name())
	}

	// Check data type
	if !medianCol.DataType().Equals(datatypes.Float64{}) {
		t.Errorf("Expected median column to be Float64, got %v", medianCol.DataType())
	}

	// Check values
	// Group A: [1, 2, 3] -> median = 2.0
	// Group B: [4, 5, 6] -> median = 5.0
	expectedMedians := []float64{2.0, 5.0}
	for i := 0; i < medianCol.Len(); i++ {
		val := medianCol.Get(i)
		if val == nil {
			t.Errorf("Median value at index %d is nil", i)
			continue
		}
		medianVal, ok := val.(float64)
		if !ok {
			t.Errorf("Median value at index %d is not float64: %T", i, val)
			continue
		}
		// Find which group this belongs to
		groupVal := groupCol.Get(i).(string)
		expectedIdx := 0
		if groupVal == "B" {
			expectedIdx = 1
		}
		if medianVal != expectedMedians[expectedIdx] {
			t.Errorf("Group %s: expected median %f, got %f", groupVal, expectedMedians[expectedIdx], medianVal)
		}
	}
}

func TestMedianWithNulls(t *testing.T) {
	// Create test data with null values
	values := []float64{1.0, 2.0, 0.0, 4.0, 5.0, 0.0}
	validity := []bool{true, true, false, true, true, false}
	
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)

	// Test median aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"median_value": expr.Col("value").Median(),
	})
	if err != nil {
		t.Fatalf("Median aggregation with nulls failed: %v", err)
	}

	// Check values
	// Group A: [1, 2, null] -> median = 1.5 (average of 1 and 2)
	// Group B: [4, 5, null] -> median = 4.5 (average of 4 and 5)
	medianCol := result.Columns[1]
	groupCol := result.Columns[0]
	
	expectedMedians := map[string]float64{"A": 1.5, "B": 4.5}
	
	for i := 0; i < medianCol.Len(); i++ {
		groupVal := groupCol.Get(i).(string)
		medianVal := medianCol.Get(i).(float64)
		
		if expected, ok := expectedMedians[groupVal]; ok {
			if medianVal != expected {
				t.Errorf("Group %s: expected median %f, got %f", groupVal, expected, medianVal)
			}
		}
	}
}

func TestMedianOddEven(t *testing.T) {
	// Test with odd number of values
	dfOdd := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A"}),
		series.NewFloat64Series("value", []float64{1.0, 3.0, 5.0}),
	)

	gbOdd, err := NewGroupBy(dfOdd, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	resultOdd, err := gbOdd.Agg(map[string]expr.Expr{
		"median": expr.Col("value").Median(),
	})
	if err != nil {
		t.Fatalf("Median aggregation (odd) failed: %v", err)
	}

	// Median of [1, 3, 5] = 3
	medianOdd := resultOdd.Columns[1].Get(0).(float64)
	if medianOdd != 3.0 {
		t.Errorf("Odd median: expected 3.0, got %f", medianOdd)
	}

	// Test with even number of values
	dfEven := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "A"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0}),
	)

	gbEven, err := NewGroupBy(dfEven, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	resultEven, err := gbEven.Agg(map[string]expr.Expr{
		"median": expr.Col("value").Median(),
	})
	if err != nil {
		t.Fatalf("Median aggregation (even) failed: %v", err)
	}

	// Median of [1, 2, 3, 4] = 2.5
	medianEven := resultEven.Columns[1].Get(0).(float64)
	if medianEven != 2.5 {
		t.Errorf("Even median: expected 2.5, got %f", medianEven)
	}
}