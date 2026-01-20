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
	if plan == nil {
		return
	}
	sb.WriteString(strings.Repeat("  ", depth))
	switch node := plan.(type) {
	case *ScanPlan:
		fmt.Fprintf(sb, "Scan[%s]\n", node.Source.Name())
	case *FilterPlan:
		fmt.Fprintf(sb, "Filter\n")
	case *ProjectionPlan:
		fmt.Fprintf(sb, "Projection[%d]", len(node.Exprs))
		if node.Arena != nil {
			names := make([]string, len(node.Exprs))
			for i, exprID := range node.Exprs {
				names[i] = OutputName(node.Arena, exprID)
			}
			fmt.Fprintf(sb, " %v", names)
		}
		sb.WriteString("\n")
	case *AggregatePlan:
		fmt.Fprintf(sb, "Aggregate[%d keys, %d aggs]", len(node.Keys), len(node.Aggs))
		sb.WriteString("\n")
	case *JoinPlan:
		fmt.Fprintf(sb, "Join[%s]\n", node.Type)
	default:
		fmt.Fprintf(sb, "%s\n", plan.Kind().String())
	}
	for _, child := range plan.Children() {
		explainNode(child, depth+1, sb)
	}
}
