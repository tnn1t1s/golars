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

	switch node.Kind {
	case KindColumn:
		if payload, ok := node.Payload.(Column); ok {
			if err := v.VisitColumn(id, payload); err != nil {
				return err
			}
		}
	case KindLiteral:
		if payload, ok := node.Payload.(Literal); ok {
			if err := v.VisitLiteral(id, payload); err != nil {
				return err
			}
		}
	case KindBinary:
		if payload, ok := node.Payload.(Binary); ok {
			if err := v.VisitBinary(id, payload); err != nil {
				return err
			}
		}
	case KindUnary:
		if payload, ok := node.Payload.(Unary); ok {
			if err := v.VisitUnary(id, payload); err != nil {
				return err
			}
		}
	case KindAgg:
		if payload, ok := node.Payload.(Agg); ok {
			if err := v.VisitAgg(id, payload); err != nil {
				return err
			}
		}
	case KindFunction:
		if payload, ok := node.Payload.(Function); ok {
			if err := v.VisitFunction(id, payload); err != nil {
				return err
			}
		}
	default:
		if err := v.VisitOther(id, node); err != nil {
			return err
		}
	}

	for _, child := range node.Children {
		if err := walkNode(a, child, v); err != nil {
			return err
		}
	}
	return nil
}
