package lazy

import "github.com/tnn1t1s/golars/internal/datatypes"

// TypeCoercion inserts casts to reconcile numeric types.
type TypeCoercion struct {
	Arena *Arena
}

func (t *TypeCoercion) Name() string { return "TypeCoercion" }

func (t *TypeCoercion) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return coercePlan(plan, t.Arena)
}

func coercePlan(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	// Recurse into children
	children := plan.Children()
	if len(children) > 0 {
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			nc, err := coercePlan(child, arena)
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
	case *ProjectionPlan:
		inputSchema, _ := p.Input.Schema()
		newExprs := make([]NodeID, len(p.Exprs))
		anyChanged := false
		for i, e := range p.Exprs {
			ne, changed, err := coerceExpr(arena, e, inputSchema)
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

	case *FilterPlan:
		inputSchema, _ := p.Input.Schema()
		ne, changed, err := coerceExpr(arena, p.Predicate, inputSchema)
		if err != nil {
			return nil, err
		}
		if changed {
			return &FilterPlan{Input: p.Input, Predicate: ne, Arena: arena}, nil
		}
	}

	return plan, nil
}

func coerceExpr(arena *Arena, id NodeID, input *datatypes.Schema) (NodeID, bool, error) {
	if arena == nil {
		return id, false, nil
	}
	node, ok := arena.Get(id)
	if !ok {
		return id, false, nil
	}

	// Recurse into children first
	if len(node.Children) > 0 {
		newChildren := make([]NodeID, len(node.Children))
		childChanged := false
		for i, child := range node.Children {
			nc, changed, err := coerceExpr(arena, child, input)
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
	if !isCoercibleOp(bin.Op) {
		return id, false, nil
	}

	if len(node.Children) != 2 {
		return id, false, nil
	}

	leftType, err := TypeOf(arena, node.Children[0], input)
	if err != nil {
		return id, false, nil
	}
	rightType, err := TypeOf(arena, node.Children[1], input)
	if err != nil {
		return id, false, nil
	}

	if leftType.Equals(rightType) {
		return id, false, nil
	}

	// Determine target type
	target := mergeNumeric(leftType, rightType)
	targetName := target.String()

	changed := false
	leftChild := node.Children[0]
	rightChild := node.Children[1]

	if !leftType.Equals(target) {
		leftChild = arena.AddCast(targetName, node.Children[0])
		changed = true
	}
	if !rightType.Equals(target) {
		rightChild = arena.AddCast(targetName, node.Children[1])
		changed = true
	}

	if changed {
		newID := arena.AddBinary(bin.Op, leftChild, rightChild)
		return newID, true, nil
	}

	return id, false, nil
}

func isCoercibleOp(op BinaryOp) bool {
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv, OpLt, OpLte, OpGt, OpGte, OpEq, OpNeq:
		return true
	default:
		return false
	}
}
