package lazy

// Optimizer rewrites a logical plan.
type Optimizer interface {
	Name() string
	Optimize(plan LogicalPlan) (LogicalPlan, error)
}

// Pipeline runs optimizers in order.
type Pipeline struct {
	Optimizers []Optimizer
	MaxPasses  int
}

// Optimize applies optimizers until no changes or max passes reached.
func (p *Pipeline) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	panic("not implemented")

}

// DefaultPipeline returns the default optimizer pipeline.
func DefaultPipeline(arena *Arena) *Pipeline {
	panic("not implemented")

}
