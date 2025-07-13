package window

import (
	"github.com/tnn1t1s/golars/expr"
)

// FrameType represents the type of window frame (ROWS, RANGE, or GROUPS)
type FrameType int

const (
	RowsFrame FrameType = iota
	RangeFrame
	GroupsFrame
)

// BoundType represents the type of frame boundary
type BoundType int

const (
	UnboundedPreceding BoundType = iota
	Preceding
	CurrentRow
	Following
	UnboundedFollowing
)

// FrameBound represents a window frame boundary
type FrameBound struct {
	Type   BoundType
	Offset interface{} // int for ROWS, comparable value for RANGE
}

// FrameSpec defines the window frame boundaries
type FrameSpec struct {
	Type  FrameType
	Start FrameBound
	End   FrameBound
}

// OrderClause represents an ORDER BY clause in a window specification
type OrderClause struct {
	Column    string
	Expr      expr.Expr
	Ascending bool
}

// Spec represents a window specification
type Spec struct {
	partitionBy []string
	orderBy     []OrderClause
	frame       *FrameSpec
}

// NewSpec creates a new window specification
func NewSpec() *Spec {
	return &Spec{
		partitionBy: make([]string, 0),
		orderBy:     make([]OrderClause, 0),
	}
}

// PartitionBy adds partition columns to the window specification
func (s *Spec) PartitionBy(cols ...string) *Spec {
	s.partitionBy = append(s.partitionBy, cols...)
	return s
}

// OrderBy adds an ordering clause to the window specification
func (s *Spec) OrderBy(col string, ascending ...bool) *Spec {
	asc := true
	if len(ascending) > 0 {
		asc = ascending[0]
	}
	
	s.orderBy = append(s.orderBy, OrderClause{
		Column:    col,
		Expr:      expr.Col(col),
		Ascending: asc,
	})
	return s
}

// OrderByExpr adds an expression-based ordering clause
func (s *Spec) OrderByExpr(expression expr.Expr, ascending ...bool) *Spec {
	asc := true
	if len(ascending) > 0 {
		asc = ascending[0]
	}
	
	s.orderBy = append(s.orderBy, OrderClause{
		Expr:      expression,
		Ascending: asc,
	})
	return s
}

// RowsBetween defines a ROWS frame with specific boundaries
func (s *Spec) RowsBetween(start, end int) *Spec {
	s.frame = &FrameSpec{
		Type:  RowsFrame,
		Start: s.createRowBound(start),
		End:   s.createRowBound(end),
	}
	return s
}

// RangeBetween defines a RANGE frame with specific boundaries
func (s *Spec) RangeBetween(start, end interface{}) *Spec {
	s.frame = &FrameSpec{
		Type:  RangeFrame,
		Start: s.createRangeBound(start),
		End:   s.createRangeBound(end),
	}
	return s
}

// GroupsBetween defines a GROUPS frame with specific boundaries
func (s *Spec) GroupsBetween(start, end int) *Spec {
	s.frame = &FrameSpec{
		Type:  GroupsFrame,
		Start: s.createGroupBound(start),
		End:   s.createGroupBound(end),
	}
	return s
}

// UnboundedPreceding sets the frame to start from the beginning of the partition
func (s *Spec) UnboundedPreceding() *Spec {
	if s.frame == nil {
		s.frame = &FrameSpec{Type: RowsFrame}
	}
	s.frame.Start = FrameBound{Type: UnboundedPreceding}
	return s
}

// UnboundedFollowing sets the frame to end at the end of the partition
func (s *Spec) UnboundedFollowing() *Spec {
	if s.frame == nil {
		s.frame = &FrameSpec{Type: RowsFrame}
	}
	s.frame.End = FrameBound{Type: UnboundedFollowing}
	return s
}

// CurrentRow sets the frame boundary to the current row
func (s *Spec) CurrentRow() *Spec {
	if s.frame == nil {
		s.frame = &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: CurrentRow},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	return s
}

// GetPartitionBy returns the partition columns
func (s *Spec) GetPartitionBy() []string {
	return s.partitionBy
}

// GetOrderBy returns the order clauses
func (s *Spec) GetOrderBy() []OrderClause {
	return s.orderBy
}

// GetFrame returns the frame specification
func (s *Spec) GetFrame() *FrameSpec {
	return s.frame
}

// HasOrderBy returns true if the window has an ORDER BY clause
func (s *Spec) HasOrderBy() bool {
	return len(s.orderBy) > 0
}

// IsPartitioned returns true if the window has a PARTITION BY clause
func (s *Spec) IsPartitioned() bool {
	return len(s.partitionBy) > 0
}

// createRowBound creates a frame bound for ROWS frames
func (s *Spec) createRowBound(offset int) FrameBound {
	if offset == 0 {
		return FrameBound{Type: CurrentRow}
	} else if offset < 0 {
		return FrameBound{
			Type:   Preceding,
			Offset: -offset,
		}
	} else {
		return FrameBound{
			Type:   Following,
			Offset: offset,
		}
	}
}

// createRangeBound creates a frame bound for RANGE frames
func (s *Spec) createRangeBound(value interface{}) FrameBound {
	// Special handling for unbounded
	if value == nil {
		return FrameBound{Type: UnboundedPreceding}
	}
	
	// Check if value is a special string indicating unbounded
	if str, ok := value.(string); ok {
		switch str {
		case "UNBOUNDED PRECEDING":
			return FrameBound{Type: UnboundedPreceding}
		case "UNBOUNDED FOLLOWING":
			return FrameBound{Type: UnboundedFollowing}
		case "CURRENT ROW":
			return FrameBound{Type: CurrentRow}
		}
	}
	
	// Numeric values indicate PRECEDING or FOLLOWING with offset
	switch v := value.(type) {
	case int:
		if v < 0 {
			return FrameBound{Type: Preceding, Offset: -v}
		} else if v > 0 {
			return FrameBound{Type: Following, Offset: v}
		} else {
			return FrameBound{Type: CurrentRow}
		}
	case int64:
		if v < 0 {
			return FrameBound{Type: Preceding, Offset: -v}
		} else if v > 0 {
			return FrameBound{Type: Following, Offset: v}
		} else {
			return FrameBound{Type: CurrentRow}
		}
	case float64:
		if v < 0 {
			return FrameBound{Type: Preceding, Offset: -v}
		} else if v > 0 {
			return FrameBound{Type: Following, Offset: v}
		} else {
			return FrameBound{Type: CurrentRow}
		}
	default:
		// For other types, treat as current row
		return FrameBound{Type: CurrentRow, Offset: value}
	}
}

// createGroupBound creates a frame bound for GROUPS frames
func (s *Spec) createGroupBound(offset int) FrameBound {
	// GROUPS frames work similar to ROWS but count peer groups
	if offset == 0 {
		return FrameBound{Type: CurrentRow}
	} else if offset < 0 {
		return FrameBound{
			Type:   Preceding,
			Offset: -offset,
		}
	} else {
		return FrameBound{
			Type:   Following,
			Offset: offset,
		}
	}
}

// DefaultFrame returns the default frame specification
// For aggregate functions with ORDER BY: RANGE UNBOUNDED PRECEDING TO CURRENT ROW
// For aggregate functions without ORDER BY: ROWS UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
func (s *Spec) DefaultFrame() *FrameSpec {
	if s.HasOrderBy() {
		return &FrameSpec{
			Type:  RangeFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	
	return &FrameSpec{
		Type:  RowsFrame,
		Start: FrameBound{Type: UnboundedPreceding},
		End:   FrameBound{Type: UnboundedFollowing},
	}
}