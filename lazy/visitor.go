package lazy

// Visitor walks expression nodes by kind.
type Visitor interface {
	VisitColumn(NodeID, Column) error
	VisitLiteral(NodeID, Literal) error
	VisitBinary(NodeID, Binary) error
	VisitUnary(NodeID, Unary) error
	VisitAgg(NodeID, Agg) error
	VisitFunction(NodeID, Function) error
	VisitOther(NodeID, Node) error
}

// Walk traverses the expression tree in depth-first order.
func Walk(a *Arena, root NodeID, v Visitor) error {
	panic("not implemented")

}

func walkNode(a *Arena, id NodeID, v Visitor) error {
	panic("not implemented")

}
