package window

import (
	"fmt"
	"sort"

	"github.com/tnn1t1s/golars/series"
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

	// ApplyOrder sorts the partition according to the given order clauses
	ApplyOrder(orderBy []OrderClause) error

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
	orderBy      []OrderClause // stores the order by clauses
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

	// Store the order by clauses
	p.orderBy = orderBy

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

// GetOrderValue returns the value used for ordering at the given row
func (p *WindowPartition) GetOrderValue(row int) interface{} {
	if !p.isOrdered || len(p.orderBy) == 0 {
		return nil
	}

	// Get the actual row index
	var idx int
	if row < len(p.orderIndices) {
		idx = p.orderIndices[row]
	} else {
		return nil
	}

	// Return the value from the first order column
	// For multi-column ordering, RANGE frames only use the first column
	firstOrderCol := p.orderBy[0]
	if firstOrderCol.Column != "" {
		if s, ok := p.series[firstOrderCol.Column]; ok {
			return s.Get(idx)
		}
	}

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
	case GroupsFrame:
		return p.calculateGroupsBounds(row, frame)
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
	// RANGE frames require an ORDER BY clause
	if !p.IsOrdered() {
		// Without ordering, treat as entire partition
		return 0, p.size
	}

	// Get the current row's order value
	currentValue := p.GetOrderValue(currentRow)
	if currentValue == nil {
		// Null values - handle separately
		return p.calculateNullRangeBounds(currentRow, frame)
	}

	// Calculate start boundary
	start = p.findRangeStart(currentRow, currentValue, frame.Start)

	// Calculate end boundary
	end = p.findRangeEnd(currentRow, currentValue, frame.End)

	return start, end
}

// findRangeStart finds the start position for a RANGE frame
func (p *WindowPartition) findRangeStart(currentRow int, currentValue interface{}, bound FrameBound) int {
	switch bound.Type {
	case UnboundedPreceding:
		return 0

	case Preceding:
		// Find first row where value >= (currentValue - offset)
		targetValue := subtractOffset(currentValue, bound.Offset)
		for i := 0; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val != nil && compareValues(val, targetValue) >= 0 {
				return i
			}
		}
		return p.size

	case CurrentRow:
		// Find first row with same value (peer row)
		for i := 0; i < currentRow; i++ {
			val := p.GetOrderValue(i)
			if val != nil && compareValues(val, currentValue) == 0 {
				return i
			}
		}
		return currentRow

	case Following:
		// Find first row where value >= (currentValue + offset)
		targetValue := addOffset(currentValue, bound.Offset)
		for i := currentRow; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val != nil && compareValues(val, targetValue) >= 0 {
				return i
			}
		}
		return p.size

	case UnboundedFollowing:
		return p.size
	}

	return 0
}

// findRangeEnd finds the end position for a RANGE frame
func (p *WindowPartition) findRangeEnd(currentRow int, currentValue interface{}, bound FrameBound) int {
	switch bound.Type {
	case UnboundedPreceding:
		return 0

	case Preceding:
		// Find last row where value <= (currentValue - offset) + 1
		targetValue := subtractOffset(currentValue, bound.Offset)
		end := 0
		for i := 0; i < currentRow; i++ {
			val := p.GetOrderValue(i)
			if val != nil && compareValues(val, targetValue) <= 0 {
				end = i + 1
			} else {
				break
			}
		}
		return end

	case CurrentRow:
		// Find last row with same value (peer row) + 1
		end := currentRow + 1
		for i := currentRow + 1; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val != nil && compareValues(val, currentValue) == 0 {
				end = i + 1
			} else {
				break
			}
		}
		return end

	case Following:
		// Find last row where value <= (currentValue + offset) + 1
		targetValue := addOffset(currentValue, bound.Offset)
		end := currentRow + 1
		for i := currentRow + 1; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val != nil && compareValues(val, targetValue) <= 0 {
				end = i + 1
			} else {
				break
			}
		}
		return end

	case UnboundedFollowing:
		return p.size
	}

	return p.size
}

// calculateNullRangeBounds handles RANGE bounds for null values
func (p *WindowPartition) calculateNullRangeBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// For null values in RANGE frames, include all nulls as peers
	start = currentRow
	end = currentRow + 1

	// Find all null peers before current row
	for i := currentRow - 1; i >= 0; i-- {
		if p.GetOrderValue(i) == nil {
			start = i
		} else {
			break
		}
	}

	// Find all null peers after current row
	for i := currentRow + 1; i < p.size; i++ {
		if p.GetOrderValue(i) == nil {
			end = i + 1
		} else {
			break
		}
	}

	return start, end
}

// addOffset adds an offset to a value for RANGE calculations
func addOffset(value interface{}, offset interface{}) interface{} {
	if offset == nil {
		return value
	}

	switch v := value.(type) {
	case int8:
		return v + offset.(int8)
	case int16:
		return v + offset.(int16)
	case int32:
		return v + offset.(int32)
	case int64:
		return v + offset.(int64)
	case int:
		return v + offset.(int)
	case float32:
		return v + offset.(float32)
	case float64:
		return v + offset.(float64)
	default:
		// For non-numeric types, RANGE doesn't make sense with offsets
		return value
	}
}

// subtractOffset subtracts an offset from a value for RANGE calculations
func subtractOffset(value interface{}, offset interface{}) interface{} {
	if offset == nil {
		return value
	}

	switch v := value.(type) {
	case int8:
		return v - offset.(int8)
	case int16:
		return v - offset.(int16)
	case int32:
		return v - offset.(int32)
	case int64:
		return v - offset.(int64)
	case int:
		return v - offset.(int)
	case float32:
		return v - offset.(float32)
	case float64:
		return v - offset.(float64)
	default:
		// For non-numeric types, RANGE doesn't make sense with offsets
		return value
	}
}

// calculateGroupsBounds calculates frame boundaries for GROUPS frame type
func (p *WindowPartition) calculateGroupsBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// GROUPS frames require an ORDER BY clause
	if !p.IsOrdered() {
		// Without ordering, treat as entire partition
		return 0, p.size
	}

	// Find peer groups
	groups := p.findPeerGroups()

	// Find which group the current row belongs to
	currentGroup := -1
	for i, group := range groups {
		for _, rowIdx := range group {
			if rowIdx == currentRow {
				currentGroup = i
				break
			}
		}
		if currentGroup >= 0 {
			break
		}
	}

	if currentGroup < 0 {
		// Current row not found in groups
		return currentRow, currentRow + 1
	}

	// Calculate start group
	startGroup := p.calculateGroupStartBoundary(currentGroup, frame.Start, len(groups))

	// Calculate end group
	endGroup := p.calculateGroupEndBoundary(currentGroup, frame.End, len(groups))

	// Ensure valid bounds
	if startGroup > endGroup {
		startGroup, endGroup = endGroup, startGroup
	}

	// Convert group indices to row indices
	if startGroup < len(groups) {
		start = groups[startGroup][0]
	} else {
		start = p.size
	}

	if endGroup < len(groups) {
		// End is the first row of the next group (exclusive)
		end = groups[endGroup][0]
	} else {
		// Include all remaining rows
		end = p.size
	}

	return start, end
}

// findPeerGroups identifies groups of rows with the same order value
func (p *WindowPartition) findPeerGroups() [][]int {
	groups := make([][]int, 0)

	if p.size == 0 {
		return groups
	}

	currentGroup := []int{0}
	currentValue := p.GetOrderValue(0)

	for i := 1; i < p.size; i++ {
		val := p.GetOrderValue(i)

		// Check if this row is a peer of the current group
		if (currentValue == nil && val == nil) ||
			(currentValue != nil && val != nil && compareValues(currentValue, val) == 0) {
			// Same value, add to current group
			currentGroup = append(currentGroup, i)
		} else {
			// Different value, start new group
			groups = append(groups, currentGroup)
			currentGroup = []int{i}
			currentValue = val
		}
	}

	// Add the last group
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

// calculateGroupStartBoundary calculates the start group index for a boundary
func (p *WindowPartition) calculateGroupStartBoundary(currentGroup int, bound FrameBound, numGroups int) int {
	switch bound.Type {
	case UnboundedPreceding:
		return 0
	case Preceding:
		offset := bound.Offset.(int)
		return max(0, currentGroup-offset)
	case CurrentRow:
		return currentGroup
	case Following:
		offset := bound.Offset.(int)
		return min(numGroups-1, currentGroup+offset)
	case UnboundedFollowing:
		return numGroups - 1
	}
	return currentGroup
}

// calculateGroupEndBoundary calculates the end group index for a boundary
func (p *WindowPartition) calculateGroupEndBoundary(currentGroup int, bound FrameBound, numGroups int) int {
	switch bound.Type {
	case UnboundedPreceding:
		return 0
	case Preceding:
		offset := bound.Offset.(int)
		return max(0, currentGroup-offset+1)
	case CurrentRow:
		return currentGroup + 1
	case Following:
		offset := bound.Offset.(int)
		return min(numGroups, currentGroup+offset+1)
	case UnboundedFollowing:
		return numGroups
	}
	return currentGroup + 1
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
