package lazy

// Arena stores expression nodes and interned strings.
type Arena struct {
	nodes      []Node
	strings    []string
	stringByID map[string]uint32
}

// NewArena creates a new Arena.
func NewArena() *Arena {
	return &Arena{
		nodes:      make([]Node, 0),
		strings:    make([]string, 0),
		stringByID: make(map[string]uint32),
	}
}

// InternString stores a string and returns its ID.
func (a *Arena) InternString(s string) uint32 {
	if id, ok := a.stringByID[s]; ok {
		return id
	}
	id := uint32(len(a.strings))
	a.strings = append(a.strings, s)
	a.stringByID[s] = id
	return id
}

// String returns the string for the given ID.
func (a *Arena) String(id uint32) (string, bool) {
	if int(id) < 0 || int(id) >= len(a.strings) {
		return "", false
	}
	return a.strings[id], true
}

// Add inserts a node and returns its ID.
func (a *Arena) Add(node Node) NodeID {
	id := NodeID(len(a.nodes))
	a.nodes = append(a.nodes, node)
	return id
}

// Get returns a node by ID.
func (a *Arena) Get(id NodeID) (Node, bool) {
	if id < 0 || int(id) >= len(a.nodes) {
		return Node{}, false
	}
	return a.nodes[id], true
}

// MustGet returns a node by ID or panics on invalid ID.
func (a *Arena) MustGet(id NodeID) Node {
	node, ok := a.Get(id)
	if !ok {
		panic("invalid node id")
	}
	return node
}

// Transform creates a new node from an existing node.
func (a *Arena) Transform(id NodeID, fn func(Node) Node) NodeID {
	node := a.MustGet(id)
	return a.Add(fn(node))
}

// WithChildren returns a copy of the node with new children.
func (a *Arena) WithChildren(id NodeID, children []NodeID) NodeID {
	return a.Transform(id, func(node Node) Node {
		node.Children = children
		return node
	})
}

// AddColumn adds a column reference node.
func (a *Arena) AddColumn(name string) NodeID {
	nameID := a.InternString(name)
	return a.Add(Node{
		Kind:    KindColumn,
		Payload: Column{NameID: nameID},
	})
}

// AddLiteral adds a literal node.
func (a *Arena) AddLiteral(value interface{}) NodeID {
	return a.Add(Node{
		Kind:    KindLiteral,
		Payload: Literal{Value: value},
	})
}

// AddBinary adds a binary node.
func (a *Arena) AddBinary(op BinaryOp, left, right NodeID) NodeID {
	return a.Add(Node{
		Kind:     KindBinary,
		Payload:  Binary{Op: op},
		Children: []NodeID{left, right},
	})
}

// AddUnary adds a unary node.
func (a *Arena) AddUnary(op UnaryOp, input NodeID) NodeID {
	return a.Add(Node{
		Kind:     KindUnary,
		Payload:  Unary{Op: op},
		Children: []NodeID{input},
	})
}

// AddAgg adds an aggregation node.
func (a *Arena) AddAgg(op AggOp, input NodeID) NodeID {
	return a.Add(Node{
		Kind:     KindAgg,
		Payload:  Agg{Op: op},
		Children: []NodeID{input},
	})
}

// AddFunction adds a function node.
func (a *Arena) AddFunction(name string, args []NodeID) NodeID {
	nameID := a.InternString(name)
	return a.Add(Node{
		Kind:     KindFunction,
		Payload:  Function{NameID: nameID},
		Children: args,
	})
}

// AddAlias adds an alias node.
func (a *Arena) AddAlias(name string, input NodeID) NodeID {
	nameID := a.InternString(name)
	return a.Add(Node{
		Kind:     KindAlias,
		Payload:  Alias{NameID: nameID},
		Children: []NodeID{input},
	})
}

// AddCast adds a cast node.
func (a *Arena) AddCast(typeName string, input NodeID) NodeID {
	typeID := a.InternString(typeName)
	return a.Add(Node{
		Kind:     KindCast,
		Payload:  Cast{TypeID: typeID},
		Children: []NodeID{input},
	})
}

// AddWindow adds a window node.
func (a *Arena) AddWindow(fn Window, input NodeID, hasInput bool) NodeID {
	var children []NodeID
	if hasInput {
		children = []NodeID{input}
	}
	return a.Add(Node{
		Kind:     KindWindow,
		Payload:  fn,
		Children: children,
	})
}
