package lazy

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// ColumnExpansion expands wildcard and type-based column selectors.
type ColumnExpansion struct {
	Arena *Arena
}

func (c *ColumnExpansion) Name() string { return "column_expansion" }

func (c *ColumnExpansion) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	if c.Arena == nil {
		return plan, nil
	}
	return expandColumns(plan, c.Arena)
}

func expandColumns(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ProjectionPlan:
		child, err := expandColumns(node.Input, arena)
		if err != nil {
			return nil, err
		}
		inputSchema, err := child.Schema()
		if err != nil {
			return nil, err
		}
		exprs, changed, err := expandProjectionExprs(arena, node.Exprs, inputSchema)
		if err != nil {
			return nil, err
		}
		if !changed && child == node.Input {
			return node, nil
		}
		cp := *node
		cp.Input = child
		cp.Exprs = exprs
		cp.SchemaCache = nil
		return &cp, nil
	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan, nil
		}
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			next, err := expandColumns(child, arena)
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

func expandProjectionExprs(arena *Arena, exprs []NodeID, input *datatypes.Schema) ([]NodeID, bool, error) {
	if input == nil {
		return exprs, false, fmt.Errorf("missing input schema for expansion")
	}
	out := make([]NodeID, 0, len(exprs))
	changed := false
	for _, exprID := range exprs {
		node, ok := arena.Get(exprID)
		if !ok {
			out = append(out, exprID)
			continue
		}
		if node.Kind == KindColumn {
			payload, ok := node.Payload.(Column)
			if ok {
				name, ok := arena.String(payload.NameID)
				if ok && name == "*" {
					for _, field := range input.Fields {
						out = append(out, arena.AddColumn(field.Name))
					}
					changed = true
					continue
				}
			}
		}
		if node.Kind == KindFunction && isColTypeSelector(arena, node) {
			dt, err := selectorType(arena, node)
			if err != nil {
				return nil, false, err
			}
			for _, field := range input.Fields {
				if field.DataType != nil && field.DataType.Equals(dt) {
					out = append(out, arena.AddColumn(field.Name))
				}
			}
			changed = true
			continue
		}
		out = append(out, exprID)
	}
	return out, changed, nil
}

func isColTypeSelector(arena *Arena, node Node) bool {
	payload, ok := node.Payload.(Function)
	if !ok {
		return false
	}
	name, ok := arena.String(payload.NameID)
	return ok && name == "col_type"
}

func selectorType(arena *Arena, node Node) (datatypes.DataType, error) {
	if len(node.Children) != 1 {
		return nil, fmt.Errorf("col_type requires one argument")
	}
	child, ok := arena.Get(node.Children[0])
	if !ok || child.Kind != KindLiteral {
		return nil, fmt.Errorf("col_type requires a literal type")
	}
	literal, ok := child.Payload.(Literal)
	if !ok {
		return nil, fmt.Errorf("invalid col_type literal")
	}
	switch val := literal.Value.(type) {
	case datatypes.DataType:
		return val, nil
	case string:
		dt := typeFromName(val)
		if dt == nil || dt.Equals(datatypes.Unknown{}) {
			return nil, fmt.Errorf("unknown col_type %q", val)
		}
		return dt, nil
	default:
		return nil, fmt.Errorf("unsupported col_type literal")
	}
}
