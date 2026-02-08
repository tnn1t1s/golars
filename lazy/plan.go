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
	panic("not implemented")

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

func (p *ScanPlan) Kind() PlanKind { panic("not implemented") }

func (p *ScanPlan) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (p *ScanPlan) Children() []LogicalPlan { panic("not implemented") }

func (p *ScanPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

// ProjectionPlan selects or computes columns.
type ProjectionPlan struct {
	Input       LogicalPlan
	Exprs       []NodeID
	Arena       *Arena
	SchemaCache *datatypes.Schema
}

func (p *ProjectionPlan) Kind() PlanKind { panic("not implemented") }

func (p *ProjectionPlan) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (p *ProjectionPlan) Children() []LogicalPlan { panic("not implemented") }

func (p *ProjectionPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

// FilterPlan applies a predicate.
type FilterPlan struct {
	Input     LogicalPlan
	Predicate NodeID
	Arena     *Arena
}

func (p *FilterPlan) Kind() PlanKind { panic("not implemented") }

func (p *FilterPlan) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (p *FilterPlan) Children() []LogicalPlan { panic("not implemented") }

func (p *FilterPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

// AggregatePlan groups and aggregates.
type AggregatePlan struct {
	Input       LogicalPlan
	Keys        []NodeID
	Aggs        []NodeID
	Arena       *Arena
	SchemaCache *datatypes.Schema
}

func (p *AggregatePlan) Kind() PlanKind { panic("not implemented") }

func (p *AggregatePlan) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (p *AggregatePlan) Children() []LogicalPlan { panic("not implemented") }

func (p *AggregatePlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

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
	panic("not implemented")

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

func (p *JoinPlan) Kind() PlanKind { panic("not implemented") }

func (p *JoinPlan) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (p *JoinPlan) Children() []LogicalPlan {
	panic("not implemented")

}

func (p *JoinPlan) WithChildren(children []LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}
