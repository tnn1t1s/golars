package lazy

// CollectColumns returns a unique list of column names used in the expression.
func CollectColumns(a *Arena, root NodeID) []string {
	visitor := &columnCollector{
		arena:   a,
		seen:    make(map[string]struct{}),
		columns: make([]string, 0),
	}
	_ = Walk(a, root, visitor)
	return visitor.columns
}

type columnCollector struct {
	arena   *Arena
	seen    map[string]struct{}
	columns []string
}

func (c *columnCollector) VisitColumn(_ NodeID, col Column) error {
	name, ok := c.arena.String(col.NameID)
	if !ok {
		return nil
	}
	if _, exists := c.seen[name]; exists {
		return nil
	}
	c.seen[name] = struct{}{}
	c.columns = append(c.columns, name)
	return nil
}

func (c *columnCollector) VisitLiteral(NodeID, Literal) error   { return nil }
func (c *columnCollector) VisitBinary(NodeID, Binary) error     { return nil }
func (c *columnCollector) VisitUnary(NodeID, Unary) error       { return nil }
func (c *columnCollector) VisitAgg(NodeID, Agg) error           { return nil }
func (c *columnCollector) VisitFunction(NodeID, Function) error { return nil }
func (c *columnCollector) VisitOther(NodeID, Node) error        { return nil }
