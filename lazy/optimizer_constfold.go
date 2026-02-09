package lazy

import "fmt"

// ConstantFolding performs simple constant folding on expressions.
type ConstantFolding struct {
	Arena *Arena
}

func (c *ConstantFolding) Name() string { return "ConstantFolding" }

func (c *ConstantFolding) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return foldConstants(plan, c.Arena)
}

func foldConstants(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	// Recurse into children first
	children := plan.Children()
	if len(children) > 0 {
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			nc, err := foldConstants(child, arena)
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

	// Fold expressions within this plan node
	switch p := plan.(type) {
	case *FilterPlan:
		newPred := FoldExpr(arena, p.Predicate)
		if newPred != p.Predicate {
			return &FilterPlan{Input: p.Input, Predicate: newPred, Arena: p.Arena}, nil
		}
	case *ProjectionPlan:
		newExprs := make([]NodeID, len(p.Exprs))
		changed := false
		for i, e := range p.Exprs {
			newExprs[i] = FoldExpr(arena, e)
			if newExprs[i] != e {
				changed = true
			}
		}
		if changed {
			return &ProjectionPlan{Input: p.Input, Exprs: newExprs, Arena: arena}, nil
		}
	}

	return plan, nil
}

// FoldExpr rewrites constant expressions into literal nodes.
func FoldExpr(a *Arena, id NodeID) NodeID {
	if a == nil {
		return id
	}
	node, ok := a.Get(id)
	if !ok {
		return id
	}

	// Fold children first
	if len(node.Children) > 0 {
		newChildren := make([]NodeID, len(node.Children))
		changed := false
		for i, child := range node.Children {
			newChildren[i] = FoldExpr(a, child)
			if newChildren[i] != child {
				changed = true
			}
		}
		if changed {
			id = a.WithChildren(id, newChildren)
			node = a.MustGet(id)
		}
	}

	switch node.Kind {
	case KindBinary:
		bin := node.Payload.(Binary)
		if len(node.Children) == 2 {
			leftNode, lok := a.Get(node.Children[0])
			rightNode, rok := a.Get(node.Children[1])
			if lok && rok && leftNode.Kind == KindLiteral && rightNode.Kind == KindLiteral {
				leftVal := leftNode.Payload.(Literal).Value
				rightVal := rightNode.Payload.(Literal).Value
				result, ok := evalBinary(bin.Op, leftVal, rightVal)
				if ok {
					return a.AddLiteral(result)
				}
			}
		}
	case KindUnary:
		un := node.Payload.(Unary)
		if len(node.Children) == 1 {
			childNode, ok := a.Get(node.Children[0])
			if ok && childNode.Kind == KindLiteral {
				val := childNode.Payload.(Literal).Value
				result, ok := evalUnary(un.Op, val)
				if ok {
					return a.AddLiteral(result)
				}
			}
		}
	}

	return id
}

func evalBinary(op BinaryOp, left, right interface{}) (interface{}, bool) {
	// Handle nil
	if left == nil || right == nil {
		if isNullPropOp(op) {
			return nil, true
		}
		return nil, false
	}

	// Boolean operations
	switch op {
	case OpAnd:
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if lok && rok {
			return lb && rb, true
		}
		return nil, false
	case OpOr:
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if lok && rok {
			return lb || rb, true
		}
		return nil, false
	case OpEq:
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), true
	case OpNeq:
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right), true
	}

	// Numeric operations
	return evalNumeric(op, left, right)
}

func evalUnary(op UnaryOp, value interface{}) (interface{}, bool) {
	switch op {
	case OpNot:
		if b, ok := value.(bool); ok {
			return !b, true
		}
		return nil, false
	case OpIsNull:
		return value == nil, true
	case OpIsNotNull:
		return value != nil, true
	case OpNeg:
		f, ok := toFloat64(value)
		if ok {
			return -f, true
		}
		return nil, false
	default:
		return nil, false
	}
}

func evalNumeric(op BinaryOp, left, right interface{}) (interface{}, bool) {
	// Try int64 path first
	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			switch op {
			case OpAdd:
				return lv + rv, true
			case OpSub:
				return lv - rv, true
			case OpMul:
				return lv * rv, true
			case OpDiv:
				if rv == 0 {
					return nil, false
				}
				return lv / rv, true
			case OpLt:
				return lv < rv, true
			case OpLte:
				return lv <= rv, true
			case OpGt:
				return lv > rv, true
			case OpGte:
				return lv >= rv, true
			}
		}
	}

	// Fall back to float64
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if lok && rok {
		switch op {
		case OpAdd:
			return lf + rf, true
		case OpSub:
			return lf - rf, true
		case OpMul:
			return lf * rf, true
		case OpDiv:
			if rf == 0 {
				return nil, false
			}
			return lf / rf, true
		case OpLt:
			return lf < rf, true
		case OpLte:
			return lf <= rf, true
		case OpGt:
			return lf > rf, true
		case OpGte:
			return lf >= rf, true
		}
	}

	return nil, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

func isNullPropOp(op BinaryOp) bool {
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv, OpLt, OpLte, OpGt, OpGte:
		return true
	default:
		return false
	}
}

func (c *ConstantFolding) String() string {
	return "ConstantFolding"
}
