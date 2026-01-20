package lazy

import "testing"

func TestArenaInternString(t *testing.T) {
	arena := NewArena()
	id1 := arena.InternString("col")
	id2 := arena.InternString("col")
	if id1 != id2 {
		t.Fatalf("expected same id, got %d and %d", id1, id2)
	}
}

func TestArenaAddAndGet(t *testing.T) {
	arena := NewArena()
	col := arena.AddColumn("a")
	lit := arena.AddLiteral(10)
	expr := arena.AddBinary(OpAdd, col, lit)

	node, ok := arena.Get(expr)
	if !ok {
		t.Fatalf("expected node for %d", expr)
	}
	if node.Kind != KindBinary {
		t.Fatalf("expected KindBinary, got %v", node.Kind)
	}
}

func TestCollectColumns(t *testing.T) {
	arena := NewArena()
	colA := arena.AddColumn("a")
	colB := arena.AddColumn("b")
	lit := arena.AddLiteral(1)
	sum := arena.AddBinary(OpAdd, colA, lit)
	_ = arena.AddBinary(OpMul, sum, colB)

	cols := CollectColumns(arena, sum)
	if len(cols) != 1 || cols[0] != "a" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}
