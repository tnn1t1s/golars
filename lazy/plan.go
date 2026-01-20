package lazy

import "github.com/tnn1t1s/golars/internal/datatypes"

// PlanKind identifies the logical plan node type.
type PlanKind int

const (
	PlanScan PlanKind = iota
	PlanProjection
	PlanFilter
	PlanAggregate
	PlanJoin
)

func (k PlanKind) String() string {
	switch k {
	case PlanScan:
		return "Scan"
	case PlanProjection:
		return "Projection"
	case PlanFilter:
		return "Filter"
	case PlanAggregate:
		return "Aggregate"
	case PlanJoin:
		return "Join"
	default:
		return "Unknown"
	}
}

// LogicalPlan describes a logical plan node.
type LogicalPlan interface {
	Kind() PlanKind
	Schema() (*datatypes.Schema, error)
	Children() []LogicalPlan
	WithChildren(children []LogicalPlan) (LogicalPlan, error)
}

// ScanPlan reads from a data source.
type ScanPlan struct {
	Source      DataSource
	Projections []NodeID
	Predicates  []NodeID
	Arena       *Arena
	SchemaHint  *datatypes.Schema
}

func (p *ScanPlan) Kind() PlanKind { return PlanScan }

func (p *ScanPlan) Schema() (*datatypes.Schema, error) {
	if p.SchemaHint != nil {
		return p.SchemaHint, nil
	}
	if p.Source == nil {
		return nil, errMissingSource
	}
	return p.Source.Schema()
}

func (p *ScanPlan) Children() []LogicalPlan { return nil }

func (p *ScanPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	if len(children) != 0 {
		return nil, errInvalidChildren
	}
	return p, nil
}

// ProjectionPlan selects or computes columns.
type ProjectionPlan struct {
	Input       LogicalPlan
	Exprs       []NodeID
	Arena       *Arena
	SchemaCache *datatypes.Schema
}

func (p *ProjectionPlan) Kind() PlanKind { return PlanProjection }

func (p *ProjectionPlan) Schema() (*datatypes.Schema, error) {
	if p.SchemaCache != nil {
		return p.SchemaCache, nil
	}
	if p.Arena == nil {
		return nil, errMissingArena
	}
	if p.Input == nil {
		return nil, errMissingInput
	}
	inputSchema, err := p.Input.Schema()
	if err != nil {
		return nil, err
	}
	schema, err := InferProjectionSchema(p.Arena, p.Exprs, inputSchema)
	if err != nil {
		return nil, err
	}
	p.SchemaCache = schema
	return schema, nil
}

func (p *ProjectionPlan) Children() []LogicalPlan { return []LogicalPlan{p.Input} }

func (p *ProjectionPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	if len(children) != 1 {
		return nil, errInvalidChildren
	}
	cp := *p
	cp.Input = children[0]
	return &cp, nil
}

// FilterPlan applies a predicate.
type FilterPlan struct {
	Input     LogicalPlan
	Predicate NodeID
	Arena     *Arena
}

func (p *FilterPlan) Kind() PlanKind { return PlanFilter }

func (p *FilterPlan) Schema() (*datatypes.Schema, error) {
	if p.Input == nil {
		return nil, errMissingInput
	}
	return p.Input.Schema()
}

func (p *FilterPlan) Children() []LogicalPlan { return []LogicalPlan{p.Input} }

func (p *FilterPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	if len(children) != 1 {
		return nil, errInvalidChildren
	}
	cp := *p
	cp.Input = children[0]
	return &cp, nil
}

// AggregatePlan groups and aggregates.
type AggregatePlan struct {
	Input       LogicalPlan
	Keys        []NodeID
	Aggs        []NodeID
	Arena       *Arena
	SchemaCache *datatypes.Schema
}

func (p *AggregatePlan) Kind() PlanKind { return PlanAggregate }

func (p *AggregatePlan) Schema() (*datatypes.Schema, error) {
	if p.SchemaCache != nil {
		return p.SchemaCache, nil
	}
	if p.Arena == nil {
		return nil, errMissingArena
	}
	if p.Input == nil {
		return nil, errMissingInput
	}
	inputSchema, err := p.Input.Schema()
	if err != nil {
		return nil, err
	}
	schema, err := InferAggregateSchema(p.Arena, p.Keys, p.Aggs, inputSchema)
	if err != nil {
		return nil, err
	}
	p.SchemaCache = schema
	return schema, nil
}

func (p *AggregatePlan) Children() []LogicalPlan { return []LogicalPlan{p.Input} }

func (p *AggregatePlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	if len(children) != 1 {
		return nil, errInvalidChildren
	}
	cp := *p
	cp.Input = children[0]
	return &cp, nil
}

// JoinType describes the join mode.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
	JoinSemi
	JoinAnti
)

func (t JoinType) String() string {
	switch t {
	case JoinInner:
		return "Inner"
	case JoinLeft:
		return "Left"
	case JoinRight:
		return "Right"
	case JoinFull:
		return "Full"
	case JoinSemi:
		return "Semi"
	case JoinAnti:
		return "Anti"
	default:
		return "Unknown"
	}
}

// JoinPlan combines two inputs.
type JoinPlan struct {
	Left        LogicalPlan
	Right       LogicalPlan
	LeftOn      []NodeID
	RightOn     []NodeID
	Type        JoinType
	Arena       *Arena
	SchemaCache *datatypes.Schema
}

func (p *JoinPlan) Kind() PlanKind { return PlanJoin }

func (p *JoinPlan) Schema() (*datatypes.Schema, error) {
	if p.SchemaCache != nil {
		return p.SchemaCache, nil
	}
	if p.Left == nil || p.Right == nil {
		return nil, errMissingInput
	}
	leftSchema, err := p.Left.Schema()
	if err != nil {
		return nil, err
	}
	rightSchema, err := p.Right.Schema()
	if err != nil {
		return nil, err
	}
	schema := MergeJoinSchema(leftSchema, rightSchema)
	p.SchemaCache = schema
	return schema, nil
}

func (p *JoinPlan) Children() []LogicalPlan {
	return []LogicalPlan{p.Left, p.Right}
}

func (p *JoinPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	if len(children) != 2 {
		return nil, errInvalidChildren
	}
	cp := *p
	cp.Left = children[0]
	cp.Right = children[1]
	return &cp, nil
}
