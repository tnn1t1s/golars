package lazy

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// ColumnExpansion expands wildcard and type-based column selectors.
type ColumnExpansion struct {
	Arena *Arena
}

func (c *ColumnExpansion) Name() string { return "ColumnExpansion" }

func (c *ColumnExpansion) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return expandColumns(plan, c.Arena)
}

func expandColumns(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	// Recurse into children first
	children := plan.Children()
	if len(children) > 0 {
		newChildren := make([]LogicalPlan, len(children))
		changed := false
		for i, child := range children {
			nc, err := expandColumns(child, arena)
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
		inputSchema, err := p.Input.Schema()
		if err != nil {
			return plan, nil
		}
		newExprs, changed, err := expandProjectionExprs(arena, p.Exprs, inputSchema)
		if err != nil {
			return nil, err
		}
		if changed {
			return &ProjectionPlan{Input: p.Input, Exprs: newExprs, Arena: arena}, nil
		}
	}

	return plan, nil
}

func expandProjectionExprs(arena *Arena, exprs []NodeID, input *datatypes.Schema) ([]NodeID, bool, error) {
	if arena == nil || input == nil {
		return exprs, false, nil
	}

	var result []NodeID
	changed := false

	for _, id := range exprs {
		node, ok := arena.Get(id)
		if !ok {
			result = append(result, id)
			continue
		}

		// Check for wildcard: column named "*"
		if node.Kind == KindColumn {
			col := node.Payload.(Column)
			name, ok := arena.String(col.NameID)
			if ok && name == "*" {
				// Expand to all columns
				for _, field := range input.Fields {
					result = append(result, arena.AddColumn(field.Name))
				}
				changed = true
				continue
			}
		}

		// Check for type selector: function named "__col_type__"
		if isColTypeSelector(arena, node) {
			dt, err := selectorType(arena, node)
			if err != nil {
				result = append(result, id)
				continue
			}
			for _, field := range input.Fields {
				if field.DataType.Equals(dt) {
					result = append(result, arena.AddColumn(field.Name))
				}
			}
			changed = true
			continue
		}

		result = append(result, id)
	}

	return result, changed, nil
}

func isColTypeSelector(arena *Arena, node Node) bool {
	if node.Kind != KindFunction {
		return false
	}
	fn := node.Payload.(Function)
	name, ok := arena.String(fn.NameID)
	return ok && name == "__col_type__"
}

func selectorType(arena *Arena, node Node) (datatypes.DataType, error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("type selector has no children")
	}
	child, ok := arena.Get(node.Children[0])
	if !ok {
		return nil, fmt.Errorf("invalid child node")
	}
	if child.Kind != KindLiteral {
		return nil, fmt.Errorf("type selector child must be literal")
	}
	lit := child.Payload.(Literal)
	if dt, ok := lit.Value.(datatypes.DataType); ok {
		return dt, nil
	}
	return nil, fmt.Errorf("type selector value must be DataType")
}
