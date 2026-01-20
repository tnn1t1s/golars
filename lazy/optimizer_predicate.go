package lazy

// PredicatePushdown pushes filters into scans when safe.
type PredicatePushdown struct{}

func (p *PredicatePushdown) Name() string { return "predicate_pushdown" }

func (p *PredicatePushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return pushdownPredicate(plan)
}

func pushdownPredicate(plan LogicalPlan) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *FilterPlan:
		child, err := pushdownPredicate(node.Input)
		if err != nil {
			return nil, err
		}

		if scan, ok := child.(*ScanPlan); ok {
			cp := *scan
			cp.Predicates = append(cp.Predicates, node.Predicate)
			return &cp, nil
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
			next, err := pushdownPredicate(child)
			if err != nil {
				return nil, err
			}
			newChildren[i] = next
		}
		return plan.WithChildren(newChildren)
	}
}
