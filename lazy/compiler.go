package lazy

import "fmt"

// Compile turns a logical plan into a physical plan.
func Compile(plan LogicalPlan) (PhysicalPlan, error) {
	switch node := plan.(type) {
	case *ScanPlan:
		source, ok := node.Source.(ExecutableSource)
		if !ok {
			return nil, fmt.Errorf("scan source is not executable")
		}
		var physical PhysicalPlan = &PhysicalScan{Source: source}
		if len(node.Predicates) > 0 && node.Arena == nil {
			return nil, errMissingArena
		}
		for _, predicate := range node.Predicates {
			physical = &PhysicalFilter{
				Input:     physical,
				Predicate: predicate,
				Arena:     node.Arena,
			}
		}
		if len(node.Projections) > 0 {
			if node.Arena == nil {
				return nil, errMissingArena
			}
			physical = &PhysicalProjection{
				Input: physical,
				Exprs: node.Projections,
				Arena: node.Arena,
			}
		}
		return physical, nil
	case *FilterPlan:
		input, err := Compile(node.Input)
		if err != nil {
			return nil, err
		}
		return &PhysicalFilter{
			Input:     input,
			Predicate: node.Predicate,
			Arena:     node.Arena,
		}, nil
	case *ProjectionPlan:
		input, err := Compile(node.Input)
		if err != nil {
			return nil, err
		}
		return &PhysicalProjection{
			Input: input,
			Exprs: node.Exprs,
			Arena: node.Arena,
		}, nil
	case *AggregatePlan:
		input, err := Compile(node.Input)
		if err != nil {
			return nil, err
		}
		return &PhysicalAggregate{
			Input: input,
			Keys:  node.Keys,
			Aggs:  node.Aggs,
			Arena: node.Arena,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported plan node")
	}
}
