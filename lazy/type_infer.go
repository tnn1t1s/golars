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
		return nil, fmt.Errorf("invalid node id")
	}

	switch node.Kind {
	case KindColumn:
		payload, ok := node.Payload.(Column)
		if !ok {
			return nil, fmt.Errorf("invalid column payload")
		}
		if input == nil {
			return nil, fmt.Errorf("missing input schema")
		}
		name, ok := a.String(payload.NameID)
		if !ok {
			return nil, fmt.Errorf("unknown column name")
		}
		field, ok := input.GetField(name)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		return field.DataType, nil
	case KindLiteral:
		payload, ok := node.Payload.(Literal)
		if !ok {
			return nil, fmt.Errorf("invalid literal payload")
		}
		return typeFromValue(payload.Value), nil
	case KindBinary:
		payload, ok := node.Payload.(Binary)
		if !ok {
			return nil, fmt.Errorf("invalid binary payload")
		}
		if len(node.Children) != 2 {
			return nil, fmt.Errorf("binary node missing children")
		}
		left, err := TypeOf(a, node.Children[0], input)
		if err != nil {
			return nil, err
		}
		right, err := TypeOf(a, node.Children[1], input)
		if err != nil {
			return nil, err
		}
		switch payload.Op {
		case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte, OpAnd, OpOr:
			return datatypes.Boolean{}, nil
		default:
			return mergeNumeric(left, right), nil
		}
	case KindUnary:
		payload, ok := node.Payload.(Unary)
		if !ok {
			return nil, fmt.Errorf("invalid unary payload")
		}
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("unary node missing child")
		}
		childType, err := TypeOf(a, node.Children[0], input)
		if err != nil {
			return nil, err
		}
		switch payload.Op {
		case OpNot, OpIsNull, OpIsNotNull:
			return datatypes.Boolean{}, nil
		default:
			return childType, nil
		}
	case KindAgg:
		payload, ok := node.Payload.(Agg)
		if !ok {
			return nil, fmt.Errorf("invalid agg payload")
		}
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("agg node missing child")
		}
		childType, err := TypeOf(a, node.Children[0], input)
		if err != nil {
			return nil, err
		}
		switch payload.Op {
		case AggMean, AggStd, AggVar, AggMedian:
			return datatypes.Float64{}, nil
		case AggCount:
			return datatypes.Int64{}, nil
		default:
			return childType, nil
		}
	case KindCast:
		payload, ok := node.Payload.(Cast)
		if !ok {
			return nil, fmt.Errorf("invalid cast payload")
		}
		name, ok := a.String(payload.TypeID)
		if !ok {
			return datatypes.Unknown{}, nil
		}
		return typeFromName(name), nil
	case KindFunction:
		return datatypes.Unknown{}, nil
	case KindAlias:
		if len(node.Children) != 1 {
			return datatypes.Unknown{}, nil
		}
		return TypeOf(a, node.Children[0], input)
	case KindWindow:
		payload, ok := node.Payload.(Window)
		if !ok || payload.Func == nil {
			return datatypes.Unknown{}, nil
		}
		var inputType datatypes.DataType = datatypes.Unknown{}
		if len(node.Children) == 1 {
			childType, err := TypeOf(a, node.Children[0], input)
			if err != nil {
				return nil, err
			}
			inputType = childType
		}
		return payload.Func.DataType(inputType), nil
	default:
		return datatypes.Unknown{}, nil
	}
}

func typeFromValue(v interface{}) datatypes.DataType {
	switch v.(type) {
	case bool:
		return datatypes.Boolean{}
	case int8:
		return datatypes.Int8{}
	case int16:
		return datatypes.Int16{}
	case int32:
		return datatypes.Int32{}
	case int64:
		return datatypes.Int64{}
	case int:
		return datatypes.Int64{}
	case uint8:
		return datatypes.UInt8{}
	case uint16:
		return datatypes.UInt16{}
	case uint32:
		return datatypes.UInt32{}
	case uint64:
		return datatypes.UInt64{}
	case uint:
		return datatypes.UInt64{}
	case float32:
		return datatypes.Float32{}
	case float64:
		return datatypes.Float64{}
	case string:
		return datatypes.String{}
	case []byte:
		return datatypes.Binary{}
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
	case "string":
		return datatypes.String{}
	case "binary":
		return datatypes.Binary{}
	case "null":
		return datatypes.Null{}
	default:
		return datatypes.Unknown{}
	}
}

func mergeNumeric(left, right datatypes.DataType) datatypes.DataType {
	if left == nil || right == nil {
		return datatypes.Unknown{}
	}
	if left.Equals(datatypes.Float64{}) || right.Equals(datatypes.Float64{}) {
		return datatypes.Float64{}
	}
	if left.Equals(datatypes.Float32{}) || right.Equals(datatypes.Float32{}) {
		return datatypes.Float32{}
	}
	if left.IsInteger() && right.IsInteger() {
		if left.IsSigned() || right.IsSigned() {
			return datatypes.Int64{}
		}
		return datatypes.UInt64{}
	}
	return datatypes.Unknown{}
}

// OutputName returns a deterministic output name for a node.
func OutputName(a *Arena, id NodeID) string {
	if a == nil {
		return "unknown"
	}
	node, ok := a.Get(id)
	if !ok {
		return "unknown"
	}
	switch node.Kind {
	case KindColumn:
		payload, ok := node.Payload.(Column)
		if !ok {
			return "column"
		}
		if name, ok := a.String(payload.NameID); ok {
			return name
		}
		return "column"
	case KindAgg:
		if len(node.Children) == 1 {
			child := OutputName(a, node.Children[0])
			if payload, ok := node.Payload.(Agg); ok {
				return fmt.Sprintf("%s_%s", child, aggName(payload.Op))
			}
			return child
		}
		return "agg"
	case KindFunction:
		if payload, ok := node.Payload.(Function); ok {
			if name, ok := a.String(payload.NameID); ok {
				return name
			}
		}
		return "function"
	case KindAlias:
		if payload, ok := node.Payload.(Alias); ok {
			if name, ok := a.String(payload.NameID); ok {
				return name
			}
		}
		return "alias"
	case KindWindow:
		name := "window"
		if payload, ok := node.Payload.(Window); ok && payload.Func != nil {
			name = payload.Func.Name()
		}
		if len(node.Children) == 1 {
			child := OutputName(a, node.Children[0])
			return fmt.Sprintf("%s_%s", child, name)
		}
		return name
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
	for i, expr := range exprs {
		dt, err := TypeOf(a, expr, input)
		if err != nil {
			return nil, err
		}
		fields[i] = datatypes.Field{
			Name:     OutputName(a, expr),
			DataType: dt,
			Nullable: true,
		}
	}
	return datatypes.NewSchema(fields...), nil
}

// InferAggregateSchema builds a schema for group keys and aggregations.
func InferAggregateSchema(a *Arena, keys, aggs []NodeID, input *datatypes.Schema) (*datatypes.Schema, error) {
	fields := make([]datatypes.Field, 0, len(keys)+len(aggs))
	for _, key := range keys {
		dt, err := TypeOf(a, key, input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, datatypes.Field{
			Name:     OutputName(a, key),
			DataType: dt,
			Nullable: true,
		})
	}
	for _, agg := range aggs {
		dt, err := TypeOf(a, agg, input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, datatypes.Field{
			Name:     OutputName(a, agg),
			DataType: dt,
			Nullable: true,
		})
	}
	return datatypes.NewSchema(fields...), nil
}

// MergeJoinSchema merges left/right schemas, suffixing duplicates on the right.
func MergeJoinSchema(left, right *datatypes.Schema) *datatypes.Schema {
	if left == nil && right == nil {
		return datatypes.NewSchema()
	}
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}

	leftNames := make(map[string]struct{}, len(left.Fields))
	for _, field := range left.Fields {
		leftNames[field.Name] = struct{}{}
	}

	fields := make([]datatypes.Field, 0, len(left.Fields)+len(right.Fields))
	fields = append(fields, left.Fields...)

	for _, field := range right.Fields {
		if _, exists := leftNames[field.Name]; exists {
			field.Name = field.Name + "_right"
		}
		fields = append(fields, field)
	}
	return datatypes.NewSchema(fields...)
}
