package lazy

import "github.com/tnn1t1s/golars/internal/datatypes"

// TypeCoercion inserts casts to reconcile numeric types.
type TypeCoercion struct {
	Arena *Arena
}

func (t *TypeCoercion) Name() string { return "type_coercion" }

func (t *TypeCoercion) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	if t.Arena == nil {
		return plan, nil
	}
	return coercePlan(plan, t.Arena)
}

func coercePlan(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ScanPlan:
		cp := *node
		schema, _ := node.Schema()
		changed := false
		for i, pred := range cp.Predicates {
			next, didChange, err := coerceExpr(arena, pred, schema)
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
		inputSchema, err := node.Input.Schema()
		if err != nil {
			return nil, err
		}
		next, _, err := coerceExpr(arena, node.Predicate, inputSchema)
		if err != nil {
			return nil, err
		}
		cp.Predicate = next
		child, err := coercePlan(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		return &cp, nil
	case *ProjectionPlan:
		cp := *node
		inputSchema, err := node.Input.Schema()
		if err != nil {
			return nil, err
		}
		changed := false
		for i, exprID := range cp.Exprs {
			next, didChange, err := coerceExpr(arena, exprID, inputSchema)
			if err != nil {
				return nil, err
			}
			cp.Exprs[i] = next
			if didChange {
				changed = true
			}
		}
		child, err := coercePlan(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		if changed {
			cp.SchemaCache = nil
			return &cp, nil
		}
		if child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *AggregatePlan:
		cp := *node
		inputSchema, err := node.Input.Schema()
		if err != nil {
			return nil, err
		}
		changed := false
		for i, key := range cp.Keys {
			next, didChange, err := coerceExpr(arena, key, inputSchema)
			if err != nil {
				return nil, err
			}
			cp.Keys[i] = next
			if didChange {
				changed = true
			}
		}
		for i, agg := range cp.Aggs {
			next, didChange, err := coerceExpr(arena, agg, inputSchema)
			if err != nil {
				return nil, err
			}
			cp.Aggs[i] = next
			if didChange {
				changed = true
			}
		}
		child, err := coercePlan(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		if changed {
			cp.SchemaCache = nil
			return &cp, nil
		}
		if child == node.Input {
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
			next, err := coercePlan(child, arena)
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

func coerceExpr(arena *Arena, id NodeID, input *datatypes.Schema) (NodeID, bool, error) {
	node, ok := arena.Get(id)
	if !ok {
		return id, false, nil
	}
	if len(node.Children) == 0 {
		return id, false, nil
	}

	changed := false
	children := make([]NodeID, len(node.Children))
	for i, child := range node.Children {
		next, didChange, err := coerceExpr(arena, child, input)
		if err != nil {
			return id, false, err
		}
		children[i] = next
		if didChange {
			changed = true
		}
	}
	if changed {
		id = arena.WithChildren(id, children)
		node = arena.MustGet(id)
	}

	if node.Kind != KindBinary || len(node.Children) != 2 {
		return id, changed, nil
	}
	payload, ok := node.Payload.(Binary)
	if !ok || !isCoercibleOp(payload.Op) {
		return id, changed, nil
	}

	leftType, err := TypeOf(arena, node.Children[0], input)
	if err != nil {
		return id, changed, nil
	}
	rightType, err := TypeOf(arena, node.Children[1], input)
	if err != nil {
		return id, changed, nil
	}
	if leftType == nil || rightType == nil {
		return id, changed, nil
	}
	if !leftType.IsNumeric() || !rightType.IsNumeric() {
		return id, changed, nil
	}
	target := mergeNumeric(leftType, rightType)
	if target.Equals(datatypes.Unknown{}) {
		return id, changed, nil
	}

	leftID := node.Children[0]
	rightID := node.Children[1]
	if !leftType.Equals(target) {
		leftID = arena.AddCast(target.String(), leftID)
		changed = true
	}
	if !rightType.Equals(target) {
		rightID = arena.AddCast(target.String(), rightID)
		changed = true
	}
	if changed {
		id = arena.WithChildren(id, []NodeID{leftID, rightID})
	}
	return id, changed, nil
}

func isCoercibleOp(op BinaryOp) bool {
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv, OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		return true
	default:
		return false
	}
}
