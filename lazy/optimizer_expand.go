package lazy

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

// ColumnExpansion expands wildcard and type-based column selectors.
type ColumnExpansion struct {
	Arena *Arena
}

func (c *ColumnExpansion) Name() string { panic("not implemented") }

func (c *ColumnExpansion) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

func expandColumns(plan LogicalPlan, arena *Arena) (LogicalPlan, error) {
	panic("not implemented")

}

func expandProjectionExprs(arena *Arena, exprs []NodeID, input *datatypes.Schema) ([]NodeID, bool, error) {
	panic("not implemented")

}

func isColTypeSelector(arena *Arena, node Node) bool {
	panic("not implemented")

}

func selectorType(arena *Arena, node Node) (datatypes.DataType, error) {
	panic("not implemented")

}
