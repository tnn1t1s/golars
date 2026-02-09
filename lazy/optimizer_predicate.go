package lazy

// PredicatePushdown pushes filters into scans when safe.
type PredicatePushdown struct{}

func (p *PredicatePushdown) Name() string { return "PredicatePushdown" }

func (p *PredicatePushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return pushdownPredicate(plan)
}

func pushdownPredicate(plan LogicalPlan) (LogicalPlan, error) {
	switch p := plan.(type) {
	case *FilterPlan:
		// First, optimize the child
		child, err := pushdownPredicate(p.Input)
		if err != nil {
			return nil, err
		}

		// If child is a scan, push predicate into it
		if scan, ok := child.(*ScanPlan); ok {
			newScan := *scan
			newScan.Predicates = append(append([]NodeID(nil), scan.Predicates...), p.Predicate)
			return &newScan, nil
		}

		// Otherwise keep the filter with the optimized child
		if child != p.Input {
			return &FilterPlan{Input: child, Predicate: p.Predicate, Arena: p.Arena}, nil
		}
		return p, nil

	case *ProjectionPlan:
		child, err := pushdownPredicate(p.Input)
		if err != nil {
			return nil, err
		}
		if child != p.Input {
			return &ProjectionPlan{Input: child, Exprs: p.Exprs, Arena: p.Arena}, nil
		}
		return p, nil

	case *AggregatePlan:
		child, err := pushdownPredicate(p.Input)
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
