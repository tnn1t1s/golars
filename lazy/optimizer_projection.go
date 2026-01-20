package lazy

// ProjectionPushdown pushes column-only projections into scans.
type ProjectionPushdown struct{}

func (p *ProjectionPushdown) Name() string { return "projection_pushdown" }

func (p *ProjectionPushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return pushdownProjection(plan)
}

func pushdownProjection(plan LogicalPlan) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ProjectionPlan:
		child, err := pushdownProjection(node.Input)
		if err != nil {
			return nil, err
		}

		if scan, ok := child.(*ScanPlan); ok && canPushProjection(node.Arena, node.Exprs, scan.Projections) {
			cp := *scan
			cp.Projections = node.Exprs
			return &cp, nil
		}

		if filter, ok := child.(*FilterPlan); ok {
			if scan, ok := filter.Input.(*ScanPlan); ok && canPushProjection(node.Arena, node.Exprs, scan.Projections) {
				scanCopy := *scan
				scanCopy.Projections = node.Exprs
				filterCopy := *filter
				filterCopy.Input = &scanCopy
				return &filterCopy, nil
			}
		}

		cp := *node
		cp.Input = child
		return &cp, nil
	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, nil
		}
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			next, err := pushdownProjection(child)
			if err != nil {
				return nil, err
			}
			newChildren[i] = next
		}
		return plan.WithChildren(newChildren)
	}
}

func canPushProjection(a *Arena, exprs, existing []NodeID) bool {
	if a == nil || len(exprs) == 0 {
		return false
	}
	if len(existing) > 0 {
		return false
	}
	for _, expr := range exprs {
		node, ok := a.Get(expr)
		if !ok || node.Kind != KindColumn {
			return false
		}
	}
	return true
}
