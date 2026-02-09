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
	maxPasses := p.MaxPasses
	if maxPasses <= 0 {
		maxPasses = 5
	}

	current := plan
	for pass := 0; pass < maxPasses; pass++ {
		changed := false
		for _, opt := range p.Optimizers {
			next, err := opt.Optimize(current)
			if err != nil {
				return nil, err
			}
			if next != current {
				changed = true
				current = next
			}
		}
		if !changed {
			break
		}
	}
	return current, nil
}

// DefaultPipeline returns the default optimizer pipeline.
func DefaultPipeline(arena *Arena) *Pipeline {
	return &Pipeline{
		Optimizers: []Optimizer{
			&ConstantFolding{Arena: arena},
			&BooleanSimplify{Arena: arena},
			&ColumnExpansion{Arena: arena},
			&TypeCoercion{Arena: arena},
			&CommonSubexpressionElimination{Arena: arena},
			&PredicatePushdown{},
			&ProjectionPushdown{},
		},
		MaxPasses: 5,
	}
}
