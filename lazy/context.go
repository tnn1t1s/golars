package lazy

// EvalContext describes how an expression is evaluated.
type EvalContext int

const (
	ContextUnknown EvalContext = iota
	ContextFilter
	ContextProjection
	ContextAggregation
	ContextJoin
)
