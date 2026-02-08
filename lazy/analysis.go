package lazy

// CollectColumns returns a unique list of column names used in the expression.
func CollectColumns(a *Arena, root NodeID) []string {
	panic("not implemented")

}

type columnCollector struct {
	arena   *Arena
	seen    map[string]struct{}
	columns []string
}

func (c *columnCollector) VisitColumn(_ NodeID, col Column) error {
	panic("not implemented")

}

func (c *columnCollector) VisitLiteral(NodeID, Literal) error   { panic("not implemented") }
func (c *columnCollector) VisitBinary(NodeID, Binary) error     { panic("not implemented") }
func (c *columnCollector) VisitUnary(NodeID, Unary) error       { panic("not implemented") }
func (c *columnCollector) VisitAgg(NodeID, Agg) error           { panic("not implemented") }
func (c *columnCollector) VisitFunction(NodeID, Function) error { panic("not implemented") }
func (c *columnCollector) VisitOther(NodeID, Node) error        { panic("not implemented") }
