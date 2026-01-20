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
	if p.MaxPasses <= 0 {
		p.MaxPasses = 1
	}

	current := plan
	for pass := 0; pass < p.MaxPasses; pass++ {
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
		MaxPasses: 2,
	}
}
