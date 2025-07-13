package window

import (
	"fmt"
	"sort"

	"github.com/davidpalaitis/golars/series"
)

// Partition represents a window partition with ordered data
type Partition interface {
	// Series returns all series in the partition
	Series() map[string]series.Series
	
	// Column returns a specific series by name
	Column(name string) (series.Series, error)
	
	// Indices returns the row indices for this partition
	Indices() []int
	
	// Size returns the number of rows in the partition
	Size() int
	
	// IsOrdered returns true if the partition has been ordered
	IsOrdered() bool
	
	// OrderIndices returns the indices in sorted order (if ordered)
	OrderIndices() []int
	
	// FrameBounds calculates the frame boundaries for a specific row
	FrameBounds(row int, frame *FrameSpec) (start, end int)
}

// WindowPartition is the concrete implementation of Partition
type WindowPartition struct {
	series       map[string]series.Series
	indices      []int
	orderIndices []int // indices in sorted order
	isOrdered    bool
	size         int
}

// NewPartition creates a new partition from series and indices
func NewPartition(seriesMap map[string]series.Series, indices []int) Partition {
	return &WindowPartition{
		series:    seriesMap,
		indices:   indices,
		size:      len(indices),
		isOrdered: false,
	}
}

// Series returns all series in the partition
func (p *WindowPartition) Series() map[string]series.Series {
	return p.series
}

// Column returns a specific series by name
func (p *WindowPartition) Column(name string) (series.Series, error) {
	if s, ok := p.series[name]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("column %s not found in partition", name)
}

// Indices returns the row indices for this partition
func (p *WindowPartition) Indices() []int {
	return p.indices
}

// Size returns the number of rows in the partition
func (p *WindowPartition) Size() int {
	return p.size
}

// IsOrdered returns true if the partition has been ordered
func (p *WindowPartition) IsOrdered() bool {
	return p.isOrdered
}

// OrderIndices returns the indices in sorted order
func (p *WindowPartition) OrderIndices() []int {
	return p.orderIndices
}

// ApplyOrder sorts the partition according to the given order clauses
func (p *WindowPartition) ApplyOrder(orderBy []OrderClause) error {
	if len(orderBy) == 0 {
		return nil
	}
	
	// Create a copy of indices to sort
	p.orderIndices = make([]int, len(p.indices))
	copy(p.orderIndices, p.indices)
	
	// Multi-column stable sort
	sort.SliceStable(p.orderIndices, func(i, j int) bool {
		idx1, idx2 := p.orderIndices[i], p.orderIndices[j]
		
		for _, clause := range orderBy {
			var val1, val2 interface{}
			
			if clause.Column != "" {
				// Column-based ordering
				if s, ok := p.series[clause.Column]; ok {
					val1 = s.Get(idx1)
					val2 = s.Get(idx2)
				}
			} else {
				// Expression-based ordering (future enhancement)
				// For now, we'll skip expression-based ordering
				continue
			}
			
			cmp := compareValues(val1, val2)
			if cmp != 0 {
				if clause.Ascending {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		
		return false
	})
	
	p.isOrdered = true
	return nil
}

// FrameBounds calculates the frame boundaries for a specific row
func (p *WindowPartition) FrameBounds(row int, frame *FrameSpec) (start, end int) {
	if frame == nil {
		// Default frame: entire partition
		return 0, p.size
	}
	
	switch frame.Type {
	case RowsFrame:
		return p.calculateRowBounds(row, frame)
	case RangeFrame:
		return p.calculateRangeBounds(row, frame)
	default:
		return 0, p.size
	}
}

// calculateRowBounds calculates frame boundaries for ROWS frame type
func (p *WindowPartition) calculateRowBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// Calculate start boundary
	switch frame.Start.Type {
	case UnboundedPreceding:
		start = 0
	case Preceding:
		offset := frame.Start.Offset.(int)
		start = max(0, currentRow-offset)
	case CurrentRow:
		start = currentRow
	case Following:
		offset := frame.Start.Offset.(int)
		start = min(p.size, currentRow+offset)
	case UnboundedFollowing:
		start = p.size
	}
	
	// Calculate end boundary
	switch frame.End.Type {
	case UnboundedPreceding:
		end = 0
	case Preceding:
		offset := frame.End.Offset.(int)
		end = max(0, currentRow-offset+1)
	case CurrentRow:
		end = currentRow + 1
	case Following:
		offset := frame.End.Offset.(int)
		end = min(p.size, currentRow+offset+1)
	case UnboundedFollowing:
		end = p.size
	}
	
	// Ensure valid bounds
	if start > end {
		start, end = end, start
	}
	
	return start, end
}

// calculateRangeBounds calculates frame boundaries for RANGE frame type
func (p *WindowPartition) calculateRangeBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// For now, implement basic RANGE support
	// Full implementation would compare actual values
	
	// Default behavior: same as ROWS for now
	return p.calculateRowBounds(currentRow, frame)
}

// compareValues compares two values for ordering
func compareValues(a, b interface{}) int {
	// Handle nulls
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	
	// Type-specific comparison
	switch va := a.(type) {
	case int8:
		vb := b.(int8)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case int16:
		vb := b.(int16)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case int32:
		vb := b.(int32)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float32:
		vb := b.(float32)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float64:
		vb := b.(float64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case bool:
		vb := b.(bool)
		if !va && vb {
			return -1
		} else if va && !vb {
			return 1
		}
		return 0
	default:
		// For other types, consider them equal
		return 0
	}
}

// Helper functions
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}