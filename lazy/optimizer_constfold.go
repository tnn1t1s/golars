package lazy

import _ "fmt"

// ConstantFolding performs simple constant folding on expressions.
type ConstantFolding struct {
	Arena *Arena
}

func (c *ConstantFolding) Name() string { panic("not implemented") }

func (c *ConstantFolding) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func foldConstants(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	panic("not implemented")

}

// FoldExpr rewrites constant expressions into literal nodes.
func FoldExpr(a *Arena, id NodeID) NodeID {
	panic("not implemented")

}

func evalBinary(op BinaryOp, left, right interface{}) (interface{}, bool) {
	panic("not implemented")

}

func evalUnary(op UnaryOp, value interface{}) (interface{}, bool) {
	panic("not implemented")

}

func evalNumeric(op BinaryOp, left, right interface{}) (interface{}, bool) {
	panic("not implemented")

}

func toFloat64(v interface{}) (float64, bool) {
	panic("not implemented")

}

func isNullPropOp(op BinaryOp) bool {
	panic("not implemented")

}

func (c *ConstantFolding) String() string {
	panic("not implemented")

}
