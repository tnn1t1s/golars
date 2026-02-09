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
	return walkNode(a, root, v)
}

func walkNode(a *Arena, id NodeID, v Visitor) error {
	node, ok := a.Get(id)
	if !ok {
		return nil
	}

	// Visit children first (depth-first)
	for _, child := range node.Children {
		if err := walkNode(a, child, v); err != nil {
			return err
		}
	}

	// Visit this node
	switch node.Kind {
	case KindColumn:
		return v.VisitColumn(id, node.Payload.(Column))
	case KindLiteral:
		return v.VisitLiteral(id, node.Payload.(Literal))
	case KindBinary:
		return v.VisitBinary(id, node.Payload.(Binary))
	case KindUnary:
		return v.VisitUnary(id, node.Payload.(Unary))
	case KindAgg:
		return v.VisitAgg(id, node.Payload.(Agg))
	case KindFunction:
		return v.VisitFunction(id, node.Payload.(Function))
	default:
		return v.VisitOther(id, node)
	}
}
