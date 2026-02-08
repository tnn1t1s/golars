package lazy

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

var errMissingArena = fmt.Errorf("missing arena")

// TypeOf infers the type of an expression node using an input schema.
func TypeOf(a *Arena, id NodeID, input *datatypes.Schema) (datatypes.DataType, error) {
	panic("not implemented")

}

func typeFromValue(v interface{}) datatypes.DataType {
	panic("not implemented")

}

func typeFromName(name string) datatypes.DataType {
	panic("not implemented")

}

func mergeNumeric(left, right datatypes.DataType) datatypes.DataType {
	panic("not implemented")

}

// OutputName returns a deterministic output name for a node.
func OutputName(a *Arena, id NodeID) string {
	panic("not implemented")

}

func aggName(op AggOp) string {
	panic("not implemented")

}

// InferProjectionSchema builds a schema from a list of projection expressions.
func InferProjectionSchema(a *Arena, exprs []NodeID, input *datatypes.Schema) (*datatypes.Schema, error) {
	panic("not implemented")

}

// InferAggregateSchema builds a schema for group keys and aggregations.
func InferAggregateSchema(a *Arena, keys, aggs []NodeID, input *datatypes.Schema) (*datatypes.Schema, error) {
	panic("not implemented")

}

// MergeJoinSchema merges left/right schemas, suffixing duplicates on the right.
func MergeJoinSchema(left, right *datatypes.Schema) *datatypes.Schema {
	panic("not implemented")

}
