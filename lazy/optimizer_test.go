package lazy

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

func TestPredicatePushdown(t *testing.T) {
	arena := NewArena()
	predicate := arena.AddLiteral(true)
	scan := &ScanPlan{Source: testSource{name: "test", schema: datatypes.NewSchema()}}
	filter := &FilterPlan{Input: scan, Predicate: predicate}

	opt := &PredicatePushdown{}
	out, err := opt.Optimize(filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	scanOut, ok := out.(*ScanPlan)
	if !ok {
		t.Fatalf("expected scan plan")
	}
	if len(scanOut.Predicates) != 1 {
		t.Fatalf("expected predicate pushed to scan")
	}
}

func TestProjectionPushdown(t *testing.T) {
	arena := NewArena()
	col := arena.AddColumn("a")
	scan := &ScanPlan{Source: testSource{name: "test", schema: datatypes.NewSchema()}}
	proj := &ProjectionPlan{Input: scan, Exprs: []NodeID{col}, Arena: arena}

	opt := &ProjectionPushdown{}
	out, err := opt.Optimize(proj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	scanOut, ok := out.(*ScanPlan)
	if !ok {
		t.Fatalf("expected scan plan")
	}
	if len(scanOut.Projections) != 1 {
		t.Fatalf("expected projection pushed to scan")
	}
}

func TestConstantFolding(t *testing.T) {
	arena := NewArena()
	left := arena.AddLiteral(int64(1))
	right := arena.AddLiteral(int64(2))
	sum := arena.AddBinary(OpAdd, left, right)

	scan := &ScanPlan{Source: testSource{name: "test", schema: datatypes.NewSchema()}}
	filter := &FilterPlan{Input: scan, Predicate: sum}

	opt := &ConstantFolding{Arena: arena}
	out, err := opt.Optimize(filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	filterOut := out.(*FilterPlan)
	node, ok := arena.Get(filterOut.Predicate)
	if !ok || node.Kind != KindLiteral {
		t.Fatalf("expected folded literal predicate")
	}
}

func TestTypeCoercionAddsCast(t *testing.T) {
	arena := NewArena()
	schema := datatypes.NewSchema(datatypes.Field{Name: "a", DataType: datatypes.Int64{}})
	scan := &ScanPlan{Source: testSource{name: "test", schema: schema}, SchemaHint: schema}
	left := arena.AddColumn("a")
	right := arena.AddLiteral(float64(1.5))
	expr := arena.AddBinary(OpAdd, left, right)
	proj := &ProjectionPlan{Input: scan, Exprs: []NodeID{expr}, Arena: arena}

	opt := &TypeCoercion{Arena: arena}
	out, err := opt.Optimize(proj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	projOut := out.(*ProjectionPlan)
	node := arena.MustGet(projOut.Exprs[0])
	if node.Kind != KindBinary {
		t.Fatalf("expected binary expression")
	}
	leftNode := arena.MustGet(node.Children[0])
	if leftNode.Kind != KindCast {
		t.Fatalf("expected cast on left operand")
	}
	payload, ok := leftNode.Payload.(Cast)
	if !ok {
		t.Fatalf("invalid cast payload")
	}
	name, ok := arena.String(payload.TypeID)
	if !ok || name != "f64" {
		t.Fatalf("unexpected cast target: %s", name)
	}
}

func TestCommonSubexpressionElimination(t *testing.T) {
	arena := NewArena()
	schema := datatypes.NewSchema(datatypes.Field{Name: "a", DataType: datatypes.Int64{}})
	scan := &ScanPlan{Source: testSource{name: "test", schema: schema}}
	col := arena.AddColumn("a")
	lit := arena.AddLiteral(int64(1))
	expr1 := arena.AddBinary(OpAdd, col, lit)
	expr2 := arena.AddBinary(OpAdd, col, lit)
	if expr1 == expr2 {
		t.Fatalf("expected distinct expression nodes")
	}
	proj := &ProjectionPlan{Input: scan, Exprs: []NodeID{expr1, expr2}, Arena: arena}

	opt := &CommonSubexpressionElimination{Arena: arena}
	out, err := opt.Optimize(proj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	projOut := out.(*ProjectionPlan)
	if projOut.Exprs[0] != projOut.Exprs[1] {
		t.Fatalf("expected shared expression after CSE")
	}
}

func TestBooleanSimplify(t *testing.T) {
	arena := NewArena()
	col := arena.AddColumn("a")
	lit := arena.AddLiteral(true)
	expr := arena.AddBinary(OpAnd, col, lit)
	scan := &ScanPlan{Source: testSource{name: "test", schema: datatypes.NewSchema()}}
	filter := &FilterPlan{Input: scan, Predicate: expr}

	opt := &BooleanSimplify{Arena: arena}
	out, err := opt.Optimize(filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	filterOut := out.(*FilterPlan)
	if filterOut.Predicate != col {
		t.Fatalf("expected simplified predicate")
	}
}
