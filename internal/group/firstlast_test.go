package group

import (
	"testing"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func TestFirstLastAggregation(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"first_value": expr.Col("value").First(),
		"last_value":  expr.Col("value").Last(),
	})
	if err == nil {
		t.Fatalf("Expected first/last aggregation to be unsupported")
	}
}
func TestFirstLastWithNulls(t *testing.T) {
	values := []float64{0.0, 1.0, 2.0, 0.0, 3.0, 4.0}
	validity := []bool{false, true, true, false, true, true}
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"first_value": expr.Col("value").First(),
		"last_value":  expr.Col("value").Last(),
	})
	if err == nil {
		t.Fatalf("Expected first/last aggregation to be unsupported")
	}
}
func TestFirstLastAllNulls(t *testing.T) {
	values := []float64{0.0, 0.0, 0.0, 0.0}
	validity := []bool{false, false, false, false}
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "B", "B"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)
	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}
	_, err = gb.Agg(map[string]expr.Expr{
		"first_value": expr.Col("value").First(),
		"last_value":  expr.Col("value").Last(),
	})
	if err == nil {
		t.Fatalf("Expected first/last aggregation to be unsupported")
	}
}
