package lazy

// PredicatePushdown pushes filters into scans when safe.
type PredicatePushdown struct{}

func (p *PredicatePushdown) Name() string { panic("not implemented") }

func (p *PredicatePushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func pushdownPredicate(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}
