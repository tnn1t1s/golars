package lazy

import (
	_ "fmt"
	_ "reflect"
)

// CommonSubexpressionElimination rewrites identical expressions to share nodes.
type CommonSubexpressionElimination struct {
	Arena *Arena
}

func (c *CommonSubexpressionElimination) Name() string { panic("not implemented") }

func (c *CommonSubexpressionElimination) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func csePlan(plan LogicalPlan, arena *Arena, memo map[exprKey]NodeID) (LogicalPlan, error) {
	panic("not implemented")

}

func rewriteIDs(arena *Arena, ids []NodeID, memo map[exprKey]NodeID) ([]NodeID, bool, error) {
	panic("not implemented")

}

func cseExpr(arena *Arena, id NodeID, memo map[exprKey]NodeID) (NodeID, bool, error) {
	panic("not implemented")

}

type exprKey struct {
	kind     NodeKind
	payload  string
	children string
}

func nodeKey(arena *Arena, node Node) (exprKey, bool) {
	panic("not implemented")

}

func literalKey(value interface{}) (string, bool) {
	panic("not implemented")

}
