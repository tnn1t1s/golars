package lazy

import (
	"context"
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
	"github.com/tnn1t1s/golars/series"
)

func TestLazyFrameFilterSelect(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
		series.NewStringSeries("b", []string{"x", "y", "z"}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lf := FromDataFrame(df)
	out, err := lf.
		Filter(lf.Col("a").Gt(lf.Lit(int64(1)))).
		Select(lf.Col("b")).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("unexpected rows: %d", out.Height())
	}
}

func TestLazyFrameGroupBy(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewStringSeries("k", []string{"a", "a", "b"}),
		series.NewInt64Series("v", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lf := FromDataFrame(df)
	out, err := lf.
		GroupBy(lf.Col("k")).
		Agg(lf.Col("v").Sum()).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("unexpected rows: %d", out.Height())
	}
}

func TestLazyFrameSelectWildcard(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
		series.NewStringSeries("b", []string{"x", "y", "z"}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lf := FromDataFrame(df)
	out, err := lf.Select(lf.Col("*")).Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Width() != 2 {
		t.Fatalf("unexpected columns: %d", out.Width())
	}
	if _, err := out.Column("a"); err != nil {
		t.Fatalf("missing column a: %v", err)
	}
	if _, err := out.Column("b"); err != nil {
		t.Fatalf("missing column b: %v", err)
	}
}

func TestLazyFrameSelectType(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
		series.NewStringSeries("b", []string{"x", "y", "z"}),
		series.NewInt64Series("c", []int64{4, 5, 6}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lf := FromDataFrame(df)
	out, err := lf.Select(lf.ColType(datatypes.Int64{})).Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Width() != 2 {
		t.Fatalf("unexpected columns: %d", out.Width())
	}
	schema := out.Schema()
	if schema.Fields[0].Name != "a" || schema.Fields[1].Name != "c" {
		t.Fatalf("unexpected column order: %v", schema.FieldNames())
	}
}

func TestLazyFrameWindowRowNumber(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewStringSeries("k", []string{"a", "a", "b"}),
		series.NewInt64Series("v", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spec := window.NewSpec().PartitionBy("k")
	lf := FromDataFrame(df)
	out, err := lf.Select(lf.Col("*"), lf.RowNumber().Over(spec)).Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Width() != 3 {
		t.Fatalf("unexpected columns: %d", out.Width())
	}
	if _, err := out.Column("row_number"); err != nil {
		t.Fatalf("missing row_number column: %v", err)
	}
}

func TestLazyFrameWindowSum(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewStringSeries("k", []string{"a", "a", "b"}),
		series.NewInt64Series("v", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spec := window.NewSpec().PartitionBy("k")
	lf := FromDataFrame(df)
	out, err := lf.Select(lf.Col("k"), lf.Col("v").Sum().Over(spec)).Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Width() != 2 {
		t.Fatalf("unexpected columns: %d", out.Width())
	}
	col, err := out.Column("v_sum")
	if err != nil {
		t.Fatalf("missing v_sum column: %v", err)
	}
	if col.Get(0).(int64) != 3 || col.Get(1).(int64) != 3 || col.Get(2).(int64) != 3 {
		t.Fatalf("unexpected window sum values: %v", []interface{}{col.Get(0), col.Get(1), col.Get(2)})
	}
}

func TestLazyFrameWithColumn(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lf := FromDataFrame(df)
	out, err := lf.WithColumn("c", lf.Lit(int64(10))).Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Width() != 2 {
		t.Fatalf("unexpected columns: %d", out.Width())
	}
	col, err := out.Column("c")
	if err != nil {
		t.Fatalf("missing column c: %v", err)
	}
	if col.Get(0).(int64) != 10 {
		t.Fatalf("unexpected with column value: %v", col.Get(0))
	}
}

func TestLazyFrameWithColumnsOrdered(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lf := FromDataFrame(df)
	names := []string{"b", "c"}
	exprs := []Expr{lf.Lit(int64(7)), lf.Lit(int64(8))}
	out, err := lf.WithColumnsOrdered(names, exprs).Collect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Width() != 3 {
		t.Fatalf("unexpected columns: %d", out.Width())
	}
	if _, err := out.Column("b"); err != nil {
		t.Fatalf("missing column b: %v", err)
	}
	if _, err := out.Column("c"); err != nil {
		t.Fatalf("missing column c: %v", err)
	}
}
