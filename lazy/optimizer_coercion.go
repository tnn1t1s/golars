package lazy

import "github.com/tnn1t1s/golars/internal/datatypes"

// TypeCoercion inserts casts to reconcile numeric types.
type TypeCoercion struct {
	Arena *Arena
}

func (t *TypeCoercion) Name() string { panic("not implemented") }

func (t *TypeCoercion) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func coercePlan(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	panic("not implemented")

}

func coerceExpr(arena *Arena, id NodeID, input *datatypes.Schema) (NodeID, bool, error) {
	panic("not implemented")

}

func isCoercibleOp(op BinaryOp) bool {
	panic("not implemented")

}
