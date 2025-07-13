package group

import (
	"math"
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/series"
)

func TestStdAggregation(t *testing.T) {
	// Create test data
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)

	// Test std aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"std_value": expr.Col("value").Std(),
	})
	if err != nil {
		t.Fatalf("Std aggregation failed: %v", err)
	}

	// Check result columns
	if result.Columns == nil || len(result.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Check std column
	stdCol := result.Columns[1]
	if stdCol.Name() != "std_value" {
		t.Errorf("Expected second column to be 'std_value', got %s", stdCol.Name())
	}

	// Check data type
	if !stdCol.DataType().Equals(datatypes.Float64{}) {
		t.Errorf("Expected std column to be Float64, got %v", stdCol.DataType())
	}

	// Check values
	// Group A: [1, 2, 3] -> mean = 2, variance = 1, std = 1
	// Group B: [4, 5, 6] -> mean = 5, variance = 1, std = 1
	groupCol := result.Columns[0]
	expectedStds := map[string]float64{"A": 1.0, "B": 1.0}
	
	for i := 0; i < stdCol.Len(); i++ {
		groupVal := groupCol.Get(i).(string)
		stdVal := stdCol.Get(i).(float64)
		
		if expected, ok := expectedStds[groupVal]; ok {
			if math.Abs(stdVal - expected) > 1e-10 {
				t.Errorf("Group %s: expected std %f, got %f", groupVal, expected, stdVal)
			}
		}
	}
}

func TestVarAggregation(t *testing.T) {
	// Create test data
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "A"}),
		series.NewFloat64Series("value", []float64{2.0, 4.0, 4.0, 6.0}),
	)

	// Test var aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"var_value": expr.Col("value").Var(),
	})
	if err != nil {
		t.Fatalf("Var aggregation failed: %v", err)
	}

	// Check values
	// Group A: [2, 4, 4, 6] -> mean = 4
	// Sample variance = ((2-4)² + (4-4)² + (4-4)² + (6-4)²) / (4-1) = (4 + 0 + 0 + 4) / 3 = 8/3 ≈ 2.667
	varCol := result.Columns[1]
	varVal := varCol.Get(0).(float64)
	expectedVar := 8.0 / 3.0
	
	if math.Abs(varVal - expectedVar) > 1e-10 {
		t.Errorf("Expected variance %f, got %f", expectedVar, varVal)
	}
}

func TestStdVarWithNulls(t *testing.T) {
	// Create test data with null values
	values := []float64{1.0, 2.0, 0.0, 3.0}
	validity := []bool{true, true, false, true}
	
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "A"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)

	// Test std and var aggregation
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"std_value": expr.Col("value").Std(),
		"var_value": expr.Col("value").Var(),
	})
	if err != nil {
		t.Fatalf("Std/Var aggregation with nulls failed: %v", err)
	}

	// Values after filtering nulls: [1, 2, 3]
	// Mean = 2, Variance = 1, Std = 1
	stdCol := result.Columns[1]
	varCol := result.Columns[2]
	
	stdVal := stdCol.Get(0).(float64)
	varVal := varCol.Get(0).(float64)
	
	if math.Abs(stdVal - 1.0) > 1e-10 {
		t.Errorf("Expected std 1.0, got %f", stdVal)
	}
	
	if math.Abs(varVal - 1.0) > 1e-10 {
		t.Errorf("Expected variance 1.0, got %f", varVal)
	}
}

func TestStdVarInsufficientData(t *testing.T) {
	// Test with single value (insufficient for sample std/var with ddof=1)
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A"}),
		series.NewFloat64Series("value", []float64{5.0}),
	)

	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	
	result, err := gb.Agg(map[string]expr.Expr{
		"std_value": expr.Col("value").Std(),
		"var_value": expr.Col("value").Var(),
	})
	if err != nil {
		t.Fatalf("Std/Var aggregation failed: %v", err)
	}

	// With only one value and ddof=1, std and var should be nil
	stdCol := result.Columns[1]
	varCol := result.Columns[2]
	
	if stdCol.Get(0) != nil {
		t.Errorf("Expected std to be nil with single value, got %v", stdCol.Get(0))
	}
	
	if varCol.Get(0) != nil {
		t.Errorf("Expected variance to be nil with single value, got %v", varCol.Get(0))
	}
}