package lazy

import "fmt"

// Compile turns a logical plan into a physical plan.
func Compile(plan LogicalPlan) (PhysicalPlan, error) {
	return compilePlan(plan)
}

func compilePlan(plan LogicalPlan) (PhysicalPlan, error) {
	switch p := plan.(type) {
	case *ScanPlan:
		src, ok := p.Source.(ExecutableSource)
		if !ok {
			return nil, fmt.Errorf("data source %q is not executable", p.Source.Name())
		}
		var result PhysicalPlan = &PhysicalScan{Source: src}
		// If predicates were pushed down, wrap with physical filters
		for _, pred := range p.Predicates {
			result = &PhysicalFilter{
				Input:     result,
				Predicate: pred,
				Arena:     p.Arena,
			}
		}
		// If projections were pushed down, wrap with physical projection
		if len(p.Projections) > 0 {
			result = &PhysicalProjection{
				Input: result,
				Exprs: p.Projections,
				Arena: p.Arena,
			}
		}
		return result, nil

	case *FilterPlan:
		input, err := compilePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return &PhysicalFilter{
			Input:     input,
			Predicate: p.Predicate,
			Arena:     p.Arena,
		}, nil

	case *ProjectionPlan:
		input, err := compilePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return &PhysicalProjection{
			Input: input,
			Exprs: p.Exprs,
			Arena: p.Arena,
		}, nil

	case *AggregatePlan:
		input, err := compilePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return &PhysicalAggregate{
			Input: input,
			Keys:  p.Keys,
			Aggs:  p.Aggs,
			Arena: p.Arena,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported plan type: %T", plan)
	}
}
