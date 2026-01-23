package group

import (
	"testing"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func TestMedianAggregation(t *testing.T) {
	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewFloat64Series("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)

	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}

	_, err = gb.Agg(map[string]expr.Expr{
		"median_value": expr.Col("value").Median(),
	})
	if err == nil {
		t.Fatalf("Expected median aggregation to be unsupported")
	}
}

func TestMedianWithNulls(t *testing.T) {
	values := []float64{1.0, 2.0, 0.0, 4.0, 5.0, 0.0}
	validity := []bool{true, true, false, true, true, false}

	df := newMockDataFrame(
		series.NewStringSeries("group", []string{"A", "A", "A", "B", "B", "B"}),
		series.NewSeriesWithValidity("value", values, validity, datatypes.Float64{}),
	)

	gb, err := NewGroupBy(df, []string{"group"})
	if err != nil {
		t.Fatalf("Failed to create GroupBy: %v", err)
	}

	_, err = gb.Agg(map[string]expr.Expr{
		"median_value": expr.Col("value").Median(),
	})
	if err == nil {
		t.Fatalf("Expected median aggregation to be unsupported")
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

	_, err = gbOdd.Agg(map[string]expr.Expr{
		"median": expr.Col("value").Median(),
	})
	if err == nil {
		t.Fatalf("Expected median aggregation to be unsupported")
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

	_, err = gbEven.Agg(map[string]expr.Expr{
		"median": expr.Col("value").Median(),
	})
	if err == nil {
		t.Fatalf("Expected median aggregation to be unsupported")
	}
}
