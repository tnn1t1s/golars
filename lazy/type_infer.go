package lazy

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
)

var errMissingArena = fmt.Errorf("missing arena")

// TypeOf infers the type of an expression node using an input schema.
func TypeOf(a *Arena, id NodeID, input *datatypes.Schema) (datatypes.DataType, error) {
	if a == nil {
		return nil, errMissingArena
	}
	node, ok := a.Get(id)
	if !ok {
		return nil, fmt.Errorf("invalid node ID %d", id)
	}

	switch node.Kind {
	case KindColumn:
		col := node.Payload.(Column)
		name, ok := a.String(col.NameID)
		if !ok {
			return datatypes.Unknown{}, nil
		}
		if input != nil {
			if f, ok := input.GetField(name); ok {
				return f.DataType, nil
			}
		}
		return datatypes.Unknown{}, nil

	case KindLiteral:
		lit := node.Payload.(Literal)
		return typeFromValue(lit.Value), nil

	case KindBinary:
		bin := node.Payload.(Binary)
		switch bin.Op {
		case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte, OpAnd, OpOr:
			return datatypes.Boolean{}, nil
		default:
			// For arithmetic, infer from children
			if len(node.Children) >= 2 {
				leftType, err := TypeOf(a, node.Children[0], input)
				if err != nil {
					return datatypes.Unknown{}, nil
				}
				rightType, err := TypeOf(a, node.Children[1], input)
				if err != nil {
					return leftType, nil
				}
				return mergeNumeric(leftType, rightType), nil
			}
			return datatypes.Unknown{}, nil
		}

	case KindUnary:
		un := node.Payload.(Unary)
		switch un.Op {
		case OpNot, OpIsNull, OpIsNotNull:
			return datatypes.Boolean{}, nil
		case OpNeg:
			if len(node.Children) > 0 {
				return TypeOf(a, node.Children[0], input)
			}
			return datatypes.Unknown{}, nil
		default:
			return datatypes.Unknown{}, nil
		}

	case KindAgg:
		agg := node.Payload.(Agg)
		switch agg.Op {
		case AggCount:
			return datatypes.Int64{}, nil
		case AggMean, AggStd, AggVar, AggMedian:
			return datatypes.Float64{}, nil
		default:
			// Sum, Min, Max, First, Last keep input type
			if len(node.Children) > 0 {
				return TypeOf(a, node.Children[0], input)
			}
			return datatypes.Unknown{}, nil
		}

	case KindCast:
		cast := node.Payload.(Cast)
		name, ok := a.String(cast.TypeID)
		if !ok {
			return datatypes.Unknown{}, nil
		}
		return typeFromName(name), nil

	case KindAlias:
		if len(node.Children) > 0 {
			return TypeOf(a, node.Children[0], input)
		}
		return datatypes.Unknown{}, nil

	case KindFunction:
		return datatypes.Unknown{}, nil

	case KindWindow:
		win, ok := node.Payload.(Window)
		if ok && win.Func != nil {
			if len(node.Children) > 0 {
				childType, err := TypeOf(a, node.Children[0], input)
				if err != nil {
					return datatypes.Unknown{}, nil
				}
				return win.Func.DataType(childType), nil
			}
			return win.Func.DataType(datatypes.Unknown{}), nil
		}
		return datatypes.Unknown{}, nil

	default:
		return datatypes.Unknown{}, nil
	}
}

func typeFromValue(v interface{}) datatypes.DataType {
	switch v.(type) {
	case bool:
		return datatypes.Boolean{}
	case int:
		return datatypes.Int64{}
	case int8:
		return datatypes.Int8{}
	case int16:
		return datatypes.Int16{}
	case int32:
		return datatypes.Int32{}
	case int64:
		return datatypes.Int64{}
	case uint8:
		return datatypes.UInt8{}
	case uint16:
		return datatypes.UInt16{}
	case uint32:
		return datatypes.UInt32{}
	case uint64:
		return datatypes.UInt64{}
	case float32:
		return datatypes.Float32{}
	case float64:
		return datatypes.Float64{}
	case string:
		return datatypes.String{}
	case nil:
		return datatypes.Null{}
	default:
		return datatypes.Unknown{}
	}
}

func typeFromName(name string) datatypes.DataType {
	switch name {
	case "bool":
		return datatypes.Boolean{}
	case "i8":
		return datatypes.Int8{}
	case "i16":
		return datatypes.Int16{}
	case "i32":
		return datatypes.Int32{}
	case "i64":
		return datatypes.Int64{}
	case "u8":
		return datatypes.UInt8{}
	case "u16":
		return datatypes.UInt16{}
	case "u32":
		return datatypes.UInt32{}
	case "u64":
		return datatypes.UInt64{}
	case "f32":
		return datatypes.Float32{}
	case "f64":
		return datatypes.Float64{}
	case "str":
		return datatypes.String{}
	case "null":
		return datatypes.Null{}
	default:
		return datatypes.Unknown{}
	}
}

func mergeNumeric(left, right datatypes.DataType) datatypes.DataType {
	// If either is float64, result is float64
	if left.IsFloat() || right.IsFloat() {
		if _, ok := left.(datatypes.Float64); ok {
			return datatypes.Float64{}
		}
		if _, ok := right.(datatypes.Float64); ok {
			return datatypes.Float64{}
		}
		return datatypes.Float32{}
	}
	// If either is unknown, use the other
	if _, ok := left.(datatypes.Unknown); ok {
		return right
	}
	if _, ok := right.(datatypes.Unknown); ok {
		return left
	}
	// Default to left type
	return left
}

// OutputName returns a deterministic output name for a node.
func OutputName(a *Arena, id NodeID) string {
	node, ok := a.Get(id)
	if !ok {
		return "unknown"
	}

	switch node.Kind {
	case KindColumn:
		col := node.Payload.(Column)
		name, ok := a.String(col.NameID)
		if ok {
			return name
		}
		return "column"

	case KindAlias:
		alias := node.Payload.(Alias)
		name, ok := a.String(alias.NameID)
		if ok {
			return name
		}
		return "alias"

	case KindAgg:
		agg := node.Payload.(Agg)
		baseName := "unknown"
		if len(node.Children) > 0 {
			baseName = OutputName(a, node.Children[0])
		}
		return baseName + "_" + aggName(agg.Op)

	case KindBinary:
		leftName := "unknown"
		if len(node.Children) > 0 {
			leftName = OutputName(a, node.Children[0])
		}
		return leftName

	case KindWindow:
		win, ok := node.Payload.(Window)
		if ok && win.Func != nil {
			return win.Func.Name()
		}
		return "window"

	case KindLiteral:
		return "literal"

	default:
		return "expr"
	}
}

func aggName(op AggOp) string {
	switch op {
	case AggSum:
		return "sum"
	case AggMean:
		return "mean"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggCount:
		return "count"
	case AggStd:
		return "std"
	case AggVar:
		return "var"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	case AggMedian:
		return "median"
	default:
		return "agg"
	}
}

// InferProjectionSchema builds a schema from a list of projection expressions.
func InferProjectionSchema(a *Arena, exprs []NodeID, input *datatypes.Schema) (*datatypes.Schema, error) {
	fields := make([]datatypes.Field, len(exprs))
	for i, exprID := range exprs {
		dt, err := TypeOf(a, exprID, input)
		if err != nil {
			dt = datatypes.Unknown{}
		}
		name := OutputName(a, exprID)
		fields[i] = datatypes.Field{Name: name, DataType: dt}
	}
	return datatypes.NewSchema(fields...), nil
}

// InferAggregateSchema builds a schema for group keys and aggregations.
func InferAggregateSchema(a *Arena, keys, aggs []NodeID, input *datatypes.Schema) (*datatypes.Schema, error) {
	fields := make([]datatypes.Field, 0, len(keys)+len(aggs))
	for _, keyID := range keys {
		dt, err := TypeOf(a, keyID, input)
		if err != nil {
			dt = datatypes.Unknown{}
		}
		name := OutputName(a, keyID)
		fields = append(fields, datatypes.Field{Name: name, DataType: dt})
	}
	for _, aggID := range aggs {
		dt, err := TypeOf(a, aggID, input)
		if err != nil {
			dt = datatypes.Unknown{}
		}
		name := OutputName(a, aggID)
		fields = append(fields, datatypes.Field{Name: name, DataType: dt})
	}
	return datatypes.NewSchema(fields...), nil
}

// MergeJoinSchema merges left/right schemas, suffixing duplicates on the right.
func MergeJoinSchema(left, right *datatypes.Schema) *datatypes.Schema {
	leftNames := make(map[string]bool)
	for _, f := range left.Fields {
		leftNames[f.Name] = true
	}

	fields := make([]datatypes.Field, 0, len(left.Fields)+len(right.Fields))
	fields = append(fields, left.Fields...)
	for _, f := range right.Fields {
		if leftNames[f.Name] {
			f.Name = f.Name + "_right"
		}
		fields = append(fields, f)
	}
	return datatypes.NewSchema(fields...)
}
