package group

import (
	"testing"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

func TestFirstLastAggregation(t *testing.T) {
	// Create test data
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)

	// Test first and last aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"first_value": expr.Col("value").First(),
		"last_value": expr.Col("value").Last(),
	})
	if err != nil {
		t.Fatalf("First/Last aggregation failed: %v", err)
	}

	// Check result columns
	if result.Columns == nil || len(result.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.Columns))
	}

	// Check values
	groupCol := result.Columns[0]
	firstCol := result.Columns[1]
	lastCol := result.Columns[2]
	
	expectedFirst := map[string]float64{"A": 1.0, "B": 4.0}
	expectedLast := map[string]float64{"A": 3.0, "B": 6.0}
	
	for i := 0; i < groupCol.Len(); i++ {
		groupVal := groupCol.Get(i).(string)
		firstVal := firstCol.Get(i).(float64)
		lastVal := lastCol.Get(i).(float64)
		
		if expected, ok := expectedFirst[groupVal]; ok {
			if firstVal != expected {
				t.Errorf("Group %s: expected first %f, got %f", groupVal, expected, firstVal)
			}
		}
		
		if expected, ok := expectedLast[groupVal]; ok {
			if lastVal != expected {
				t.Errorf("Group %s: expected last %f, got %f", groupVal, expected, lastVal)
			}
		}
	}
}

func TestFirstLastWithNulls(t *testing.T) {
	// Create test data with null values
	values := []float64{0.0, 1.0, 2.0, 0.0, 3.0, 4.0}
	validity := []bool{false, true, true, false, true, true}
	
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)

	// Test first and last aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"first_value": expr.Col("value").First(),
		"last_value": expr.Col("value").Last(),
	})
	if err != nil {
		t.Fatalf("First/Last aggregation with nulls failed: %v", err)
	}

	// Check values - should skip nulls
	// Group A: [null, 1, 2] -> first = 1, last = 2
	// Group B: [null, 3, 4] -> first = 3, last = 4
	groupCol := result.Columns[0]
	firstCol := result.Columns[1]
	lastCol := result.Columns[2]
	
	expectedFirst := map[string]float64{"A": 1.0, "B": 3.0}
	expectedLast := map[string]float64{"A": 2.0, "B": 4.0}
	
	for i := 0; i < groupCol.Len(); i++ {
		groupVal := groupCol.Get(i).(string)
		firstVal := firstCol.Get(i).(float64)
		lastVal := lastCol.Get(i).(float64)
		
		if expected, ok := expectedFirst[groupVal]; ok {
			if firstVal != expected {
				t.Errorf("Group %s: expected first %f, got %f", groupVal, expected, firstVal)
			}
		}
		
		if expected, ok := expectedLast[groupVal]; ok {
			if lastVal != expected {
				t.Errorf("Group %s: expected last %f, got %f", groupVal, expected, lastVal)
			}
		}
	}
}

func TestFirstLastAllNulls(t *testing.T) {
	// Create test data with all null values
	values := []float64{0.0, 0.0}
	validity := []bool{false, false}
	
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)

	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"first_value": expr.Col("value").First(),
		"last_value": expr.Col("value").Last(),
	})
	if err != nil {
		t.Fatalf("First/Last aggregation failed: %v", err)
	}

	// Both should be null
	firstCol := result.Columns[1]
	lastCol := result.Columns[2]
	
	if !firstCol.IsNull(0) {
		t.Errorf("Expected first to be null with all null values, got %v", firstCol.Get(0))
	}
	
	if !lastCol.IsNull(0) {
		t.Errorf("Expected last to be null with all null values, got %v", lastCol.Get(0))
	}
}