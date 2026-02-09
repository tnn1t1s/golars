package lazy

// BooleanSimplify rewrites boolean expressions using identity rules.
type BooleanSimplify struct {
	Arena *Arena
}

func (b *BooleanSimplify) Name() string { return "BooleanSimplify" }

func (b *BooleanSimplify) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return simplifyPlan(plan, b.Arena)
}

func simplifyPlan(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	// Recurse into children
	children := plan.Children()
	if len(children) > 0 {
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			nc, err := simplifyPlan(child, arena)
			if err != nil {
				return nil, err
			}
			newChildren[i] = nc
			if nc != child {
				changed = true
			}
		}
		if changed {
			var err error
			plan, err = plan.WithChildren(newChildren)
			if err != nil {
				return nil, err
			}
		}
	}

	switch p := plan.(type) {
	case *FilterPlan:
		newPred, changed, err := simplifyExpr(arena, p.Predicate)
		if err != nil {
			return nil, err
		}
		if changed {
			return &FilterPlan{Input: p.Input, Predicate: newPred, Arena: p.Arena}, nil
		}
	case *ProjectionPlan:
		newExprs := make([]NodeID, len(p.Exprs))
		anyChanged := false
		for i, e := range p.Exprs {
			ne, changed, err := simplifyExpr(arena, e)
			if err != nil {
				return nil, err
			}
			newExprs[i] = ne
			if changed {
				anyChanged = true
			}
		}
		if anyChanged {
			return &ProjectionPlan{Input: p.Input, Exprs: newExprs, Arena: arena}, nil
		}
	}

	return plan, nil
}

func simplifyExpr(arena *Arena, id NodeID) (NodeID, bool, error) {
	if arena == nil {
		return id, false, nil
	}
	node, ok := arena.Get(id)
	if !ok {
		return id, false, nil
	}

	// Simplify children first
	if len(node.Children) > 0 {
		newChildren := make([]NodeID, len(node.Children))
		childChanged := false
		for i, child := range node.Children {
			nc, changed, err := simplifyExpr(arena, child)
			if err != nil {
				return id, false, err
			}
			newChildren[i] = nc
			if changed {
				childChanged = true
			}
		}
		if childChanged {
			id = arena.WithChildren(id, newChildren)
			node = arena.MustGet(id)
		}
	}

	if node.Kind != KindBinary {
		return id, false, nil
	}

	bin := node.Payload.(Binary)
	if len(node.Children) != 2 {
		return id, false, nil
	}

	leftNode, lok := arena.Get(node.Children[0])
	rightNode, rok := arena.Get(node.Children[1])

	switch bin.Op {
	case OpAnd:
		// x AND true -> x
		if rok && rightNode.Kind == KindLiteral {
			if v, ok := rightNode.Payload.(Literal).Value.(bool); ok && v {
				return node.Children[0], true, nil
			}
		}
		// true AND x -> x
		if lok && leftNode.Kind == KindLiteral {
			if v, ok := leftNode.Payload.(Literal).Value.(bool); ok && v {
				return node.Children[1], true, nil
			}
		}
		// x AND false -> false
		if rok && rightNode.Kind == KindLiteral {
			if v, ok := rightNode.Payload.(Literal).Value.(bool); ok && !v {
				return node.Children[1], true, nil
			}
		}
		// false AND x -> false
		if lok && leftNode.Kind == KindLiteral {
			if v, ok := leftNode.Payload.(Literal).Value.(bool); ok && !v {
				return node.Children[0], true, nil
			}
		}

	case OpOr:
		// x OR false -> x
		if rok && rightNode.Kind == KindLiteral {
			if v, ok := rightNode.Payload.(Literal).Value.(bool); ok && !v {
				return node.Children[0], true, nil
			}
		}
		// false OR x -> x
		if lok && leftNode.Kind == KindLiteral {
			if v, ok := leftNode.Payload.(Literal).Value.(bool); ok && !v {
				return node.Children[1], true, nil
			}
		}
		// x OR true -> true
		if rok && rightNode.Kind == KindLiteral {
			if v, ok := rightNode.Payload.(Literal).Value.(bool); ok && v {
				return node.Children[1], true, nil
			}
		}
		// true OR x -> true
		if lok && leftNode.Kind == KindLiteral {
			if v, ok := leftNode.Payload.(Literal).Value.(bool); ok && v {
				return node.Children[0], true, nil
			}
		}
	}

	return id, false, nil
}
