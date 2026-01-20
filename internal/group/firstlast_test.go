package group

import (
	"testing"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
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
		"last_value":  expr.Col("value").Last(),
	})
	if err != nil {
		t.Fatalf("First/Last aggregation failed: %v", err)
	}

	// Check result columns
	if result.Columns == nil || len(result.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.Columns))
	}

	// Check values
	// Result should have group column and aggregation columns
	// but order may vary due to map iteration
	if len(result.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.Columns))
	}

	// Create a map of actual results grouped by group value
	type groupResult struct {
		first, last float64
	}
	actualResults := make(map[string]groupResult)

	// First column should be the group column
	groupCol := result.Columns[0]
	for i := 0; i < groupCol.Len(); i++ {
		groupVal := groupCol.Get(i).(string)
		// Find first and last values for this group
		var first, last float64
		for _, col := range result.Columns[1:] {
			val := col.Get(i).(float64)
			if col.Name() == "first_value" {
				first = val
			} else if col.Name() == "last_value" {
				last = val
			}
		}
		actualResults[groupVal] = groupResult{first: first, last: last}
	}

	// Check expected values
	expected := map[string]groupResult{
		"A": {first: 1.0, last: 3.0},
		"B": {first: 4.0, last: 6.0},
	}

	for group, exp := range expected {
		if actual, ok := actualResults[group]; ok {
			if actual.first != exp.first {
				t.Errorf("Group %s: expected first %f, got %f", group, exp.first, actual.first)
			}
			if actual.last != exp.last {
				t.Errorf("Group %s: expected last %f, got %f", group, exp.last, actual.last)
			}
		} else {
			t.Errorf("Group %s not found in results", group)
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
		"last_value":  expr.Col("value").Last(),
	})
	if err != nil {
		t.Fatalf("First/Last aggregation with nulls failed: %v", err)
	}

	// Check values - should skip nulls
	// Group A: [null, 1, 2] -> first = 1, last = 2
	// Group B: [null, 3, 4] -> first = 3, last = 4

	// Result should have group column and aggregation columns
	if len(result.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.Columns))
	}

	// Create a map of actual results grouped by group value
	type groupResult struct {
		first, last float64
	}
	actualResults := make(map[string]groupResult)

	// First column should be the group column
	groupCol := result.Columns[0]
	for i := 0; i < groupCol.Len(); i++ {
		groupVal := groupCol.Get(i).(string)
		// Find first and last values for this group
		var first, last float64
		for _, col := range result.Columns[1:] {
			val := col.Get(i).(float64)
			if col.Name() == "first_value" {
				first = val
			} else if col.Name() == "last_value" {
				last = val
			}
		}
		actualResults[groupVal] = groupResult{first: first, last: last}
	}

	// Check expected values
	expected := map[string]groupResult{
		"A": {first: 1.0, last: 2.0},
		"B": {first: 3.0, last: 4.0},
	}

	for group, exp := range expected {
		if actual, ok := actualResults[group]; ok {
			if actual.first != exp.first {
				t.Errorf("Group %s: expected first %f, got %f", group, exp.first, actual.first)
			}
			if actual.last != exp.last {
				t.Errorf("Group %s: expected last %f, got %f", group, exp.last, actual.last)
			}
		} else {
			t.Errorf("Group %s not found in results", group)
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
		"last_value":  expr.Col("value").Last(),
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
