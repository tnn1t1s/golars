package lazy

import (
	"fmt"
	"strings"
)

// ExplainPlan returns a simple tree representation of a plan.
func ExplainPlan(plan LogicalPlan) string {
	var sb strings.Builder
	explainNode(plan, 0, &sb)
	return sb.String()
}

func explainNode(plan LogicalPlan, depth int, sb *strings.Builder) {
	indent := strings.Repeat("  ", depth)
	sb.WriteString(indent)
	sb.WriteString(plan.Kind().String())

	switch p := plan.(type) {
	case *ScanPlan:
		if p.Source != nil {
			sb.WriteString(fmt.Sprintf(" [%s]", p.Source.Name()))
		}
	case *FilterPlan:
		sb.WriteString(fmt.Sprintf(" [predicate=%d]", p.Predicate))
	case *ProjectionPlan:
		sb.WriteString(fmt.Sprintf(" [exprs=%d]", len(p.Exprs)))
	case *AggregatePlan:
		sb.WriteString(fmt.Sprintf(" [keys=%d, aggs=%d]", len(p.Keys), len(p.Aggs)))
	case *JoinPlan:
		sb.WriteString(fmt.Sprintf(" [%s]", p.Type.String()))
	}

	sb.WriteString("\n")

	for _, child := range plan.Children() {
		explainNode(child, depth+1, sb)
	}
}
