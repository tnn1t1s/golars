package group

import (
	"testing"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func TestStdAggregation(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"std_value": expr.Col("value").Std(),
	})
	if err == nil {
		t.Fatalf("Expected std aggregation to be unsupported")
	}
}
func TestVarAggregation(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "A"}),
		series.NewFloat64Series("value", []float64{2.0, 4.0, 4.0, 6.0}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"var_value": expr.Col("value").Var(),
	})
	if err == nil {
		t.Fatalf("Expected var aggregation to be unsupported")
	}
}
func TestStdVarWithNulls(t *testing.T) {
	values := []float64{1.0, 2.0, 0.0, 3.0}
	validity := []bool{true, true, false, true}
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "A"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"std_value": expr.Col("value").Std(),
		"var_value": expr.Col("value").Var(),
	})
	if err == nil {
		t.Fatalf("Expected std/var aggregation to be unsupported")
	}
}
func TestStdVarInsufficientData(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A"}),
		series.NewFloat64Series("value", []float64{1.0}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"std_value": expr.Col("value").Std(),
		"var_value": expr.Col("value").Var(),
	})
	if err == nil {
		t.Fatalf("Expected std/var aggregation to be unsupported")
	}
}
