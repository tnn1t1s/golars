package lazy

import "github.com/tnn1t1s/golars/internal/window"

// NodeID identifies a node in the arena.
type NodeID int32

const (
	InvalidNodeID NodeID = -1
)

// NodeKind describes the node category.
type NodeKind int

const (
	KindInvalid NodeKind = iota
	KindColumn
	KindLiteral
	KindBinary
	KindUnary
	KindAgg
	KindFunction
	KindCast
	KindSort
	KindFilter
	KindSlice
	KindWindow
	KindTernary
	KindAlias
)

// BinaryOp represents a binary operator.
type BinaryOp int

const (
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNeq
	OpLt
	OpLte
	OpGt
	OpGte
	OpAnd
	OpOr
)

// UnaryOp represents a unary operator.
type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNeg
	OpIsNull
	OpIsNotNull
)

// AggOp represents an aggregation operator.
type AggOp int

const (
	AggSum AggOp = iota
	AggMean
	AggMin
	AggMax
	AggCount
	AggStd
	AggVar
	AggFirst
	AggLast
	AggMedian
)

// Node represents a single AST node in the arena.
type Node struct {
	Kind     NodeKind
	Payload  interface{}
	Children []NodeID
}

// Column payload.
type Column struct {
	NameID uint32
}

// Literal payload.
type Literal struct {
	Value interface{}
}

// Binary payload.
type Binary struct {
	Op BinaryOp
}

// Unary payload.
type Unary struct {
	Op UnaryOp
}

// Agg payload.
type Agg struct {
	Op AggOp
}

// Function payload.
type Function struct {
	NameID uint32
}

// Cast payload.
type Cast struct {
	TypeID uint32
}

// Sort payload.
type Sort struct {
	Descending bool
	NullsLast  bool
}

// Filter payload.
type Filter struct{}

// Slice payload.
type Slice struct {
	Offset int
	Length int
}

// Window payload.
type Window struct {
	Func window.Function
	Spec *window.Spec
}

// Ternary payload.
type Ternary struct{}

// Alias payload.
type Alias struct {
	NameID uint32
}
