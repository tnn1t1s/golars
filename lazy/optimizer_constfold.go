package lazy

import "fmt"

// ConstantFolding performs simple constant folding on expressions.
type ConstantFolding struct {
	Arena *Arena
}

func (c *ConstantFolding) Name() string { return "constant_folding" }

func (c *ConstantFolding) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	if c.Arena == nil {
		return plan, nil
	}
	return foldConstants(plan, c.Arena)
}

func foldConstants(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ScanPlan:
		cp := *node
		for i, pred := range cp.Predicates {
			cp.Predicates[i] = FoldExpr(arena, pred)
		}
		return &cp, nil
	case *ProjectionPlan:
		cp := *node
		for i, expr := range cp.Exprs {
			cp.Exprs[i] = FoldExpr(arena, expr)
		}
		child, err := foldConstants(cp.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		cp.SchemaCache = nil
		return &cp, nil
	case *FilterPlan:
		cp := *node
		cp.Predicate = FoldExpr(arena, node.Predicate)
		child, err := foldConstants(node.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		return &cp, nil
	case *AggregatePlan:
		cp := *node
		for i, key := range cp.Keys {
			cp.Keys[i] = FoldExpr(arena, key)
		}
		for i, agg := range cp.Aggs {
			cp.Aggs[i] = FoldExpr(arena, agg)
		}
		child, err := foldConstants(cp.Input, arena)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		cp.SchemaCache = nil
		return &cp, nil
	case *JoinPlan:
		cp := *node
		for i, key := range cp.LeftOn {
			cp.LeftOn[i] = FoldExpr(arena, key)
		}
		for i, key := range cp.RightOn {
			cp.RightOn[i] = FoldExpr(arena, key)
		}
		left, err := foldConstants(cp.Left, arena)
		if err != nil {
			return nil, err
		}
		right, err := foldConstants(cp.Right, arena)
		if err != nil {
			return nil, err
		}
		cp.Left = left
		cp.Right = right
		cp.SchemaCache = nil
		return &cp, nil
	default:
		return plan, nil
	}
}

// FoldExpr rewrites constant expressions into literal nodes.
func FoldExpr(a *Arena, id NodeID) NodeID {
	node, ok := a.Get(id)
	if !ok {
		return id
	}

	if len(node.Children) == 0 {
		return id
	}

	children := make([]NodeID, len(node.Children))
	for i, child := range node.Children {
		children[i] = FoldExpr(a, child)
	}

	id = a.WithChildren(id, children)
	node = a.MustGet(id)

	switch node.Kind {
	case KindBinary:
		if len(children) != 2 {
			return id
		}
		left, okLeft := a.Get(children[0])
		right, okRight := a.Get(children[1])
		if !okLeft || !okRight {
			return id
		}
		litLeft, hasLeft := left.Payload.(Literal)
		litRight, hasRight := right.Payload.(Literal)
		if hasLeft && litLeft.Value == nil && isNullPropOp(node.Payload.(Binary).Op) {
			return a.AddLiteral(nil)
		}
		if hasRight && litRight.Value == nil && isNullPropOp(node.Payload.(Binary).Op) {
			return a.AddLiteral(nil)
		}
		if !hasLeft || !hasRight {
			return id
		}
		value, ok := evalBinary(node.Payload.(Binary).Op, litLeft.Value, litRight.Value)
		if !ok {
			return id
		}
		return a.AddLiteral(value)
	case KindUnary:
		if len(children) != 1 {
			return id
		}
		child, okChild := a.Get(children[0])
		if !okChild {
			return id
		}
		lit, ok := child.Payload.(Literal)
		if !ok {
			return id
		}
		value, ok := evalUnary(node.Payload.(Unary).Op, lit.Value)
		if !ok {
			return id
		}
		return a.AddLiteral(value)
	default:
		return id
	}
}

func evalBinary(op BinaryOp, left, right interface{}) (interface{}, bool) {
	switch op {
	case OpAnd:
		l, ok1 := left.(bool)
		r, ok2 := right.(bool)
		if ok1 && ok2 {
			return l && r, true
		}
	case OpOr:
		l, ok1 := left.(bool)
		r, ok2 := right.(bool)
		if ok1 && ok2 {
			return l || r, true
		}
	case OpEq:
		return left == right, true
	case OpNeq:
		return left != right, true
	case OpAdd, OpSub, OpMul, OpDiv:
		return evalNumeric(op, left, right)
	}
	return nil, false
}

func evalUnary(op UnaryOp, value interface{}) (interface{}, bool) {
	switch op {
	case OpNot:
		v, ok := value.(bool)
		return !v, ok
	case OpNeg:
		switch v := value.(type) {
		case int64:
			return -v, true
		case float64:
			return -v, true
		}
	case OpIsNull:
		return value == nil, true
	case OpIsNotNull:
		return value != nil, true
	}
	return nil, false
}

func evalNumeric(op BinaryOp, left, right interface{}) (interface{}, bool) {
	l, lok := toFloat64(left)
	r, rok := toFloat64(right)
	if !lok || !rok {
		return nil, false
	}
	switch op {
	case OpAdd:
		return l + r, true
	case OpSub:
		return l - r, true
	case OpMul:
		return l * r, true
	case OpDiv:
		return l / r, true
	default:
		return nil, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

func isNullPropOp(op BinaryOp) bool {
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv:
		return true
	default:
		return false
	}
}

func (c *ConstantFolding) String() string {
	return fmt.Sprintf("ConstantFolding(Arena=%p)", c.Arena)
}
