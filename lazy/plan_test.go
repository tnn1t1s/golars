package lazy

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

type testSource struct {
	name   string
	schema *datatypes.Schema
}

func (s testSource) Name() string                       { return s.name }
func (s testSource) Schema() (*datatypes.Schema, error) { return s.schema, nil }

func TestScanPlanSchema(t *testing.T) {
	schema := datatypes.NewSchema(datatypes.Field{Name: "a", DataType: datatypes.Int64{}})
	plan := &ScanPlan{Source: testSource{name: "test", schema: schema}}
	got, err := plan.Schema()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != schema {
		t.Fatalf("unexpected schema")
	}
}

func TestFilterPlanSchema(t *testing.T) {
	schema := datatypes.NewSchema(datatypes.Field{Name: "a", DataType: datatypes.Int64{}})
	scan := &ScanPlan{Source: testSource{name: "test", schema: schema}}
	filter := &FilterPlan{Input: scan}
	got, err := filter.Schema()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != schema {
		t.Fatalf("unexpected schema")
	}
}

func TestPlanWithChildren(t *testing.T) {
	scan := &ScanPlan{Source: testSource{name: "test"}}
	filter := &FilterPlan{Input: scan}
	newScan := &ScanPlan{Source: testSource{name: "other"}}

	next, err := filter.WithChildren([]LogicalPlan{newScan})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.(*FilterPlan).Input != newScan {
		t.Fatalf("expected updated child")
	}
}

func TestProjectionSchemaInference(t *testing.T) {
	arena := NewArena()
	col := arena.AddColumn("a")
	lit := arena.AddLiteral(1)
	expr := arena.AddBinary(OpAdd, col, lit)

	schema := datatypes.NewSchema(datatypes.Field{Name: "a", DataType: datatypes.Int64{}})
	scan := &ScanPlan{Source: testSource{name: "test", schema: schema}}
	proj := &ProjectionPlan{
		Input: scan,
		Exprs: []NodeID{expr},
		Arena: arena,
	}

	out, err := proj.Schema()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Fields) != 1 {
		t.Fatalf("unexpected schema size")
	}
}

func TestAggregateSchemaInference(t *testing.T) {
	arena := NewArena()
	key := arena.AddColumn("a")
	val := arena.AddColumn("b")
	agg := arena.AddAgg(AggSum, val)

	schema := datatypes.NewSchema(
		datatypes.Field{Name: "a", DataType: datatypes.Int64{}},
		datatypes.Field{Name: "b", DataType: datatypes.Int64{}},
	)
	scan := &ScanPlan{Source: testSource{name: "test", schema: schema}}
	plan := &AggregatePlan{
		Input: scan,
		Keys:  []NodeID{key},
		Aggs:  []NodeID{agg},
		Arena: arena,
	}

	out, err := plan.Schema()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Fields) != 2 {
		t.Fatalf("unexpected schema size")
	}
}
