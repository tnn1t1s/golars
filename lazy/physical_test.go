package lazy

import (
	"context"
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

func TestPhysicalFilter(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewInt64Series("a", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	source := &FrameSource{NameValue: "test", Frame: df}
	arena := NewArena()
	col := arena.AddColumn("a")
	lit := arena.AddLiteral(int64(2))
	pred := arena.AddBinary(OpGt, col, lit)

	plan := &FilterPlan{
		Input:     &ScanPlan{Source: source},
		Predicate: pred,
		Arena:     arena,
	}

	physical, err := Compile(plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out, err := physical.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Height() != 1 {
		t.Fatalf("unexpected rows: %d", out.Height())
	}
}

func TestPhysicalAggregate(t *testing.T) {
	df, err := frame.NewDataFrame(
		series.NewStringSeries("k", []string{"a", "a", "b"}),
		series.NewInt64Series("v", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	source := &FrameSource{NameValue: "test", Frame: df}
	arena := NewArena()
	key := arena.AddColumn("k")
	val := arena.AddColumn("v")
	agg := arena.AddAgg(AggSum, val)

	plan := &AggregatePlan{
		Input: &ScanPlan{Source: source},
		Keys:  []NodeID{key},
		Aggs:  []NodeID{agg},
		Arena: arena,
	}

	physical, err := Compile(plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out, err := physical.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("unexpected rows: %d", out.Height())
	}
}
