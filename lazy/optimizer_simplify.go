package lazy

// BooleanSimplify rewrites boolean expressions using identity rules.
type BooleanSimplify struct {
	Arena *Arena
}

func (b *BooleanSimplify) Name() string { return "boolean_simplify" }

func (b *BooleanSimplify) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	if b.Arena == nil {
		return plan, nil
	}
	return simplifyPlan(plan, b.Arena)
}

func simplifyPlan(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ScanPlan:
		cp := *node
		changed := false
		for i, pred := range cp.Predicates {
			next, didChange, err := simplifyExpr(arena, pred)
			if err != nil {
				return nil, err
			}
			cp.Predicates[i] = next
			if didChange {
				changed = true
			}
		}
		if !changed {
			return node, nil
		}
		return &cp, nil
	case *FilterPlan:
		cp := *node
		next, didChange, err := simplifyExpr(arena, node.Predicate)
		if err != nil {
			return nil, err
		}
		cp.Predicate = next
		child, err := simplifyPlan(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		if !didChange && child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *ProjectionPlan:
		cp := *node
		changed := false
		for i, exprID := range cp.Exprs {
			next, didChange, err := simplifyExpr(arena, exprID)
			if err != nil {
				return nil, err
			}
			cp.Exprs[i] = next
			if didChange {
				changed = true
			}
		}
		child, err := simplifyPlan(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		if changed {
			cp.SchemaCache = nil
		}
		if !changed && child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *AggregatePlan:
		cp := *node
		changed := false
		for i, key := range cp.Keys {
			next, didChange, err := simplifyExpr(arena, key)
			if err != nil {
				return nil, err
			}
			cp.Keys[i] = next
			if didChange {
				changed = true
			}
		}
		for i, agg := range cp.Aggs {
			next, didChange, err := simplifyExpr(arena, agg)
			if err != nil {
				return nil, err
			}
			cp.Aggs[i] = next
			if didChange {
				changed = true
			}
		}
		child, err := simplifyPlan(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		if changed {
			cp.SchemaCache = nil
		}
		if !changed && child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *JoinPlan:
		cp := *node
		changed := false
		for i, key := range cp.LeftOn {
			next, didChange, err := simplifyExpr(arena, key)
			if err != nil {
				return nil, err
			}
			cp.LeftOn[i] = next
			if didChange {
				changed = true
			}
		}
		for i, key := range cp.RightOn {
			next, didChange, err := simplifyExpr(arena, key)
			if err != nil {
				return nil, err
			}
			cp.RightOn[i] = next
			if didChange {
				changed = true
			}
		}
		left, err := simplifyPlan(node.Left, arena)
		if err != nil {
			return nil, err
		}
		right, err := simplifyPlan(node.Right, arena)
		if err != nil {
			return nil, err
		}
		cp.Left = left
		cp.Right = right
		if changed {
			cp.SchemaCache = nil
		}
		if !changed && left == node.Left && right == node.Right {
			return node, nil
		}
		return &cp, nil
	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, nil
		}
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			next, err := simplifyPlan(child, arena)
			if err != nil {
				return nil, err
			}
			newChildren[i] = next
			if next != child {
				changed = true
			}
		}
		if !changed {
			return plan, nil
		}
		return plan.WithChildren(newChildren)
	}
}

func simplifyExpr(arena *Arena, id NodeID) (NodeID, bool, error) {
	node, ok := arena.Get(id)
	if !ok {
		return id, false, nil
	}

	changed := false
	if len(node.Children) > 0 {
		children := make([]NodeID, len(node.Children))
		for i, child := range node.Children {
			next, childChanged, err := simplifyExpr(arena, child)
			if err != nil {
				return id, false, err
			}
			children[i] = next
			if childChanged || next != child {
				changed = true
			}
		}
		if changed {
			id = arena.WithChildren(id, children)
			node = arena.MustGet(id)
		}
	}

	switch node.Kind {
	case KindBinary:
		payload, ok := node.Payload.(Binary)
		if !ok || len(node.Children) != 2 {
			return id, changed, nil
		}
		if payload.Op != OpAnd && payload.Op != OpOr {
			return id, changed, nil
		}
		leftID := node.Children[0]
		rightID := node.Children[1]
		left, okLeft := arena.Get(leftID)
		right, okRight := arena.Get(rightID)
		if okLeft {
			if lit, ok := left.Payload.(Literal); ok {
				if val, ok := lit.Value.(bool); ok {
					if payload.Op == OpAnd {
						if val {
							return rightID, true, nil
						}
						return leftID, true, nil
					}
					if val {
						return leftID, true, nil
					}
					return rightID, true, nil
				}
			}
		}
		if okRight {
			if lit, ok := right.Payload.(Literal); ok {
				if val, ok := lit.Value.(bool); ok {
					if payload.Op == OpAnd {
						if val {
							return leftID, true, nil
						}
						return rightID, true, nil
					}
					if val {
						return rightID, true, nil
					}
					return leftID, true, nil
				}
			}
		}
	case KindUnary:
		payload, ok := node.Payload.(Unary)
		if !ok || payload.Op != OpNot || len(node.Children) != 1 {
			return id, changed, nil
		}
		child := node.Children[0]
		childNode, ok := arena.Get(child)
		if !ok || childNode.Kind != KindUnary {
			return id, changed, nil
		}
		childPayload, ok := childNode.Payload.(Unary)
		if !ok || childPayload.Op != OpNot || len(childNode.Children) != 1 {
			return id, changed, nil
		}
		return childNode.Children[0], true, nil
	}

	return id, changed, nil
}
