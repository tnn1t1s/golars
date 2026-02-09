package lazy

import (
	"fmt"
	"reflect"
)

// CommonSubexpressionElimination rewrites identical expressions to share nodes.
type CommonSubexpressionElimination struct {
	Arena *Arena
}

func (c *CommonSubexpressionElimination) Name() string { return "CSE" }

func (c *CommonSubexpressionElimination) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	memo := make(map[exprKey]NodeID)
	return csePlan(plan, c.Arena, memo)
}

func csePlan(plan LogicalPlan, arena *Arena, memo map[exprKey]NodeID) (LogicalPlan, error) {
	// Recurse into children
	children := plan.Children()
	if len(children) > 0 {
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			nc, err := csePlan(child, arena, memo)
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
		newExprs, changed, err := rewriteIDs(arena, p.Exprs, memo)
		if err != nil {
			return nil, err
		}
		if changed {
			return &ProjectionPlan{Input: p.Input, Exprs: newExprs, Arena: arena}, nil
		}
	case *FilterPlan:
		ne, changed, err := cseExpr(arena, p.Predicate, memo)
		if err != nil {
			return nil, err
		}
		if changed {
			return &FilterPlan{Input: p.Input, Predicate: ne, Arena: arena}, nil
		}
	}

	return plan, nil
}

func rewriteIDs(arena *Arena, ids []NodeID, memo map[exprKey]NodeID) ([]NodeID, bool, error) {
	result := make([]NodeID, len(ids))
	changed := false
	for i, id := range ids {
		ne, c, err := cseExpr(arena, id, memo)
		if err != nil {
			return nil, false, err
		}
		result[i] = ne
		if c {
			changed = true
		}
	}
	return result, changed, nil
}

func cseExpr(arena *Arena, id NodeID, memo map[exprKey]NodeID) (NodeID, bool, error) {
	if arena == nil {
		return id, false, nil
	}
	node, ok := arena.Get(id)
	if !ok {
		return id, false, nil
	}

	// Process children first
	if len(node.Children) > 0 {
		newChildren := make([]NodeID, len(node.Children))
		childChanged := false
		for i, child := range node.Children {
			nc, changed, err := cseExpr(arena, child, memo)
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

	// Compute key for this node
	key, ok := nodeKey(arena, node)
	if !ok {
		return id, false, nil
	}

	if existing, found := memo[key]; found {
		if existing != id {
			return existing, true, nil
		}
		return id, false, nil
	}

	memo[key] = id
	return id, false, nil
}

type exprKey struct {
	kind     NodeKind
	payload  string
	children string
}

func nodeKey(arena *Arena, node Node) (exprKey, bool) {
	var payloadStr string

	switch node.Kind {
	case KindColumn:
		col := node.Payload.(Column)
		payloadStr = fmt.Sprintf("col:%d", col.NameID)
	case KindLiteral:
		lit := node.Payload.(Literal)
		s, ok := literalKey(lit.Value)
		if !ok {
			return exprKey{}, false
		}
		payloadStr = s
	case KindBinary:
		bin := node.Payload.(Binary)
		payloadStr = fmt.Sprintf("bin:%d", bin.Op)
	case KindUnary:
		un := node.Payload.(Unary)
		payloadStr = fmt.Sprintf("un:%d", un.Op)
	case KindAgg:
		agg := node.Payload.(Agg)
		payloadStr = fmt.Sprintf("agg:%d", agg.Op)
	case KindFunction:
		fn := node.Payload.(Function)
		payloadStr = fmt.Sprintf("fn:%d", fn.NameID)
	case KindCast:
		cast := node.Payload.(Cast)
		payloadStr = fmt.Sprintf("cast:%d", cast.TypeID)
	case KindAlias:
		alias := node.Payload.(Alias)
		payloadStr = fmt.Sprintf("alias:%d", alias.NameID)
	default:
		return exprKey{}, false
	}

	childStr := fmt.Sprintf("%v", node.Children)

	return exprKey{
		kind:     node.Kind,
		payload:  payloadStr,
		children: childStr,
	}, true
}

func literalKey(value interface{}) (string, bool) {
	if value == nil {
		return "nil", true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return fmt.Sprintf("%T:%v", value, value), true
	default:
		return "", false
	}
}
