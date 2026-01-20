package lazy

import (
	"fmt"
	"reflect"
)

// CommonSubexpressionElimination rewrites identical expressions to share nodes.
type CommonSubexpressionElimination struct {
	Arena *Arena
}

func (c *CommonSubexpressionElimination) Name() string { return "cse" }

func (c *CommonSubexpressionElimination) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	if c.Arena == nil {
		return plan, nil
	}
	return csePlan(plan, c.Arena, make(map[exprKey]NodeID))
}

func csePlan(plan LogicalPlan, arena *Arena, memo map[exprKey]NodeID) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ScanPlan:
		cp := *node
		changed := false
		if len(cp.Predicates) > 0 {
			preds, predChanged, err := rewriteIDs(arena, cp.Predicates, memo)
			if err != nil {
				return nil, err
			}
			cp.Predicates = preds
			changed = changed || predChanged
		}
		if len(cp.Projections) > 0 {
			projs, projChanged, err := rewriteIDs(arena, cp.Projections, memo)
			if err != nil {
				return nil, err
			}
			cp.Projections = projs
			changed = changed || projChanged
		}
		if !changed {
			return node, nil
		}
		return &cp, nil
	case *FilterPlan:
		cp := *node
		predicate, predChanged, err := cseExpr(arena, node.Predicate, memo)
		if err != nil {
			return nil, err
		}
		cp.Predicate = predicate
		child, err := csePlan(node.Input, arena, memo)
		if err != nil {
			return nil, err
		}
		cp.Input = child
		if !predChanged && child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *ProjectionPlan:
		cp := *node
		exprs, exprChanged, err := rewriteIDs(arena, cp.Exprs, memo)
		if err != nil {
			return nil, err
		}
		child, err := csePlan(node.Input, arena, memo)
		if err != nil {
			return nil, err
		}
		cp.Exprs = exprs
		cp.Input = child
		if exprChanged {
			cp.SchemaCache = nil
		}
		if !exprChanged && child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *AggregatePlan:
		cp := *node
		keys, keysChanged, err := rewriteIDs(arena, cp.Keys, memo)
		if err != nil {
			return nil, err
		}
		aggs, aggsChanged, err := rewriteIDs(arena, cp.Aggs, memo)
		if err != nil {
			return nil, err
		}
		child, err := csePlan(node.Input, arena, memo)
		if err != nil {
			return nil, err
		}
		cp.Keys = keys
		cp.Aggs = aggs
		cp.Input = child
		if keysChanged || aggsChanged {
			cp.SchemaCache = nil
		}
		if !keysChanged && !aggsChanged && child == node.Input {
			return node, nil
		}
		return &cp, nil
	case *JoinPlan:
		cp := *node
		leftKeys, leftChanged, err := rewriteIDs(arena, cp.LeftOn, memo)
		if err != nil {
			return nil, err
		}
		rightKeys, rightChanged, err := rewriteIDs(arena, cp.RightOn, memo)
		if err != nil {
			return nil, err
		}
		left, err := csePlan(node.Left, arena, memo)
		if err != nil {
			return nil, err
		}
		right, err := csePlan(node.Right, arena, memo)
		if err != nil {
			return nil, err
		}
		cp.LeftOn = leftKeys
		cp.RightOn = rightKeys
		cp.Left = left
		cp.Right = right
		if leftChanged || rightChanged {
			cp.SchemaCache = nil
		}
		if !leftChanged && !rightChanged && left == node.Left && right == node.Right {
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
			next, err := csePlan(child, arena, memo)
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

func rewriteIDs(arena *Arena, ids []NodeID, memo map[exprKey]NodeID) ([]NodeID, bool, error) {
	if len(ids) == 0 {
		return ids, false, nil
	}
	out := make([]NodeID, len(ids))
	changed := false
	for i, id := range ids {
		next, didChange, err := cseExpr(arena, id, memo)
		if err != nil {
			return nil, false, err
		}
		out[i] = next
		if didChange || next != id {
			changed = true
		}
	}
	return out, changed, nil
}

func cseExpr(arena *Arena, id NodeID, memo map[exprKey]NodeID) (NodeID, bool, error) {
	node, ok := arena.Get(id)
	if !ok {
		return id, false, nil
	}

	changed := false
	if len(node.Children) > 0 {
		children := make([]NodeID, len(node.Children))
		for i, child := range node.Children {
			next, childChanged, err := cseExpr(arena, child, memo)
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

	key, ok := nodeKey(arena, node)
	if !ok {
		return id, changed, nil
	}
	if existing, found := memo[key]; found {
		if existing == id {
			return id, changed, nil
		}
		return existing, true, nil
	}
	memo[key] = id
	return id, changed, nil
}

type exprKey struct {
	kind     NodeKind
	payload  string
	children string
}

func nodeKey(arena *Arena, node Node) (exprKey, bool) {
	children := fmt.Sprintf("%v", node.Children)
	switch node.Kind {
	case KindColumn:
		payload, ok := node.Payload.(Column)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("col:%d", payload.NameID), children: children}, true
	case KindLiteral:
		payload, ok := node.Payload.(Literal)
		if !ok {
			return exprKey{}, false
		}
		valueKey, ok := literalKey(payload.Value)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: valueKey, children: children}, true
	case KindBinary:
		payload, ok := node.Payload.(Binary)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("bin:%d", payload.Op), children: children}, true
	case KindUnary:
		payload, ok := node.Payload.(Unary)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("un:%d", payload.Op), children: children}, true
	case KindAgg:
		payload, ok := node.Payload.(Agg)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("agg:%d", payload.Op), children: children}, true
	case KindFunction:
		payload, ok := node.Payload.(Function)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("fn:%d", payload.NameID), children: children}, true
	case KindCast:
		payload, ok := node.Payload.(Cast)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("cast:%d", payload.TypeID), children: children}, true
	case KindAlias:
		payload, ok := node.Payload.(Alias)
		if !ok {
			return exprKey{}, false
		}
		return exprKey{kind: node.Kind, payload: fmt.Sprintf("alias:%d", payload.NameID), children: children}, true
	default:
		return exprKey{}, false
	}
}

func literalKey(value interface{}) (string, bool) {
	if value == nil {
		return "literal:nil", true
	}
	typ := reflect.TypeOf(value)
	if typ == nil || !typ.Comparable() {
		return "", false
	}
	return fmt.Sprintf("literal:%T:%v", value, value), true
}
