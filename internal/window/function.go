package window

import (
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Function represents a window function that can be applied over a partition
type Function interface {
	// Compute calculates the window function for each row in the partition
	Compute(partition Partition) (series.Series, error)
	
	// DataType returns the expected output data type given the input type
	DataType(inputType datatypes.DataType) datatypes.DataType
	
	// Name returns the function name for display purposes
	Name() string
	
	// Validate checks if the window specification is valid for this function
	Validate(spec *Spec) error
	
	// SetSpec sets the window specification (for functions that need access to ordering info)
	SetSpec(spec *Spec)
}

// AggregateFunction represents a window function that performs aggregation
type AggregateFunction interface {
	Function
	
	// RequiresOrder returns true if the function requires an ORDER BY clause
	RequiresOrder() bool
	
	// SupportsFrame returns true if the function supports custom frame specifications
	SupportsFrame() bool
}

// RankingFunction represents a window function that assigns ranks
type RankingFunction interface {
	Function
	
	// RequiresOrder returns true (ranking functions always require ORDER BY)
	RequiresOrder() bool
}

// ValueFunction represents a window function that accesses specific values
type ValueFunction interface {
	Function
	
	// Offset returns the row offset for functions like LAG/LEAD
	Offset() int
	
	// DefaultValue returns the default value when offset is out of bounds
	DefaultValue() interface{}
}