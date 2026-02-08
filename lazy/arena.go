package lazy

// Arena stores expression nodes and interned strings.
type Arena struct {
	nodes      []Node
	strings    []string
	stringByID map[string]uint32
}

// NewArena creates a new Arena.
func NewArena() *Arena {
	panic("not implemented")

}

// InternString stores a string and returns its ID.
func (a *Arena) InternString(s string) uint32 {
	panic("not implemented")

}

// String returns the string for the given ID.
func (a *Arena) String(id uint32) (string, bool) {
	panic("not implemented")

}

// Add inserts a node and returns its ID.
func (a *Arena) Add(node Node) NodeID {
	panic("not implemented")

}

// Get returns a node by ID.
func (a *Arena) Get(id NodeID) (Node, bool) {
	panic("not implemented")

}

// MustGet returns a node by ID or panics on invalid ID.
func (a *Arena) MustGet(id NodeID) Node {
	panic("not implemented")

}

// Transform creates a new node from an existing node.
func (a *Arena) Transform(id NodeID, fn func(Node) Node) NodeID {
	panic("not implemented")

}

// WithChildren returns a copy of the node with new children.
func (a *Arena) WithChildren(id NodeID, children []NodeID) NodeID {
	panic("not implemented")

}

// AddColumn adds a column reference node.
func (a *Arena) AddColumn(name string) NodeID {
	panic("not implemented")

}

// AddLiteral adds a literal node.
func (a *Arena) AddLiteral(value interface{}) NodeID {
	panic("not implemented")

}

// AddBinary adds a binary node.
func (a *Arena) AddBinary(op BinaryOp, left, right NodeID) NodeID {
	panic("not implemented")

}

// AddUnary adds a unary node.
func (a *Arena) AddUnary(op UnaryOp, input NodeID) NodeID {
	panic("not implemented")

}

// AddAgg adds an aggregation node.
func (a *Arena) AddAgg(op AggOp, input NodeID) NodeID {
	panic("not implemented")

}

// AddFunction adds a function node.
func (a *Arena) AddFunction(name string, args []NodeID) NodeID {
	panic("not implemented")

}

// AddAlias adds an alias node.
func (a *Arena) AddAlias(name string, input NodeID) NodeID {
	panic("not implemented")

}

// AddCast adds a cast node.
func (a *Arena) AddCast(typeName string, input NodeID) NodeID {
	panic("not implemented")

}

// AddWindow adds a window node.
func (a *Arena) AddWindow(fn Window, input NodeID, hasInput bool) NodeID {
	panic("not implemented")

}
