package lazy

// BooleanSimplify rewrites boolean expressions using identity rules.
type BooleanSimplify struct {
	Arena *Arena
}

func (b *BooleanSimplify) Name() string { panic("not implemented") }

func (b *BooleanSimplify) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func simplifyPlan(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	panic("not implemented")

}

func simplifyExpr(arena *Arena, id NodeID) (NodeID, bool, error) {
	panic("not implemented")

}
