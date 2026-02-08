package lazy

// ProjectionPushdown pushes column-only projections into scans.
type ProjectionPushdown struct{}

func (p *ProjectionPushdown) Name() string { panic("not implemented") }

func (p *ProjectionPushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func pushdownProjection(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func canPushProjection(a *Arena, exprs, existing []NodeID) bool {
	panic("not implemented")

}
