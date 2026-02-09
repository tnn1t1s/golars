package lazy

// ProjectionPushdown pushes column-only projections into scans.
type ProjectionPushdown struct{}

func (p *ProjectionPushdown) Name() string { return "ProjectionPushdown" }

func (p *ProjectionPushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return pushdownProjection(plan)
}

func pushdownProjection(plan LogicalPlan) (LogicalPlan, error) {
	switch p := plan.(type) {
	case *ProjectionPlan:
		// First, optimize the child
		child, err := pushdownProjection(p.Input)
		if err != nil {
			return nil, err
		}

		// If child is a scan and all exprs are simple columns, push down
		if scan, ok := child.(*ScanPlan); ok {
			if canPushProjection(p.Arena, p.Exprs, scan.Projections) {
				newScan := *scan
				newScan.Projections = append(append([]NodeID(nil), scan.Projections...), p.Exprs...)
				return &newScan, nil
			}
		}

		if child != p.Input {
			return &ProjectionPlan{Input: child, Exprs: p.Exprs, Arena: p.Arena}, nil
		}
		return p, nil

	case *FilterPlan:
		child, err := pushdownProjection(p.Input)
		if err != nil {
			return nil, err
		}
		if child != p.Input {
			return &FilterPlan{Input: child, Predicate: p.Predicate, Arena: p.Arena}, nil
		}
		return p, nil

	case *AggregatePlan:
		child, err := pushdownProjection(p.Input)
		if err != nil {
			return nil, err
		}
		if child != p.Input {
			return &AggregatePlan{Input: child, Keys: p.Keys, Aggs: p.Aggs, Arena: p.Arena}, nil
		}
		return p, nil

	default:
		return plan, nil
	}
}

func canPushProjection(a *Arena, exprs, existing []NodeID) bool {
	if a == nil {
		return false
	}
	for _, id := range exprs {
		node, ok := a.Get(id)
		if !ok {
			return false
		}
		if node.Kind != KindColumn {
			return false
		}
	}
	return true
}
