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
	s, ok := p.series[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found in partition", name)
	}
	return s, nil
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
	// Store the order by clauses
	p.orderBy = orderBy

	// Create a copy of indices to sort
	p.orderIndices = make([]int, len(p.indices))
	copy(p.orderIndices, p.indices)

	// Multi-column stable sort
	sort.SliceStable(p.orderIndices, func(i, j int) bool {
		idxI := p.orderIndices[i]
		idxJ := p.orderIndices[j]

		for _, clause := range orderBy {
			// Column-based ordering
			s, ok := p.series[clause.Column]
			if !ok {
				continue
			}

			valI := s.Get(idxI)
			valJ := s.Get(idxJ)

			cmp := compareValues(valI, valJ)
			if cmp == 0 {
				continue
			}

			if clause.Ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})

	p.isOrdered = true
	return nil
}

// GetOrderValue returns the value used for ordering at the given row
func (p *WindowPartition) GetOrderValue(row int) interface{} {
	if len(p.orderBy) == 0 {
		return nil
	}

	// Get the actual row index
	idx := p.orderIndices[row]

	// Return the value from the first order column
	// For multi-column ordering, RANGE frames only use the first column
	s, ok := p.series[p.orderBy[0].Column]
	if !ok {
		return nil
	}
	return s.Get(idx)
}

// FrameBounds calculates the frame boundaries for a specific row
func (p *WindowPartition) FrameBounds(row int, frame *FrameSpec) (start, end int) {
	// Default frame: entire partition
	if frame == nil {
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
		offset, _ := toInt(frame.Start.Offset)
		start = currentRow - offset
	case CurrentRow:
		start = currentRow
	case Following:
		offset, _ := toInt(frame.Start.Offset)
		start = currentRow + offset
	case UnboundedFollowing:
		start = p.size
	}

	// Calculate end boundary
	switch frame.End.Type {
	case UnboundedPreceding:
		end = 0
	case Preceding:
		offset, _ := toInt(frame.End.Offset)
		end = currentRow - offset + 1
	case CurrentRow:
		end = currentRow + 1
	case Following:
		offset, _ := toInt(frame.End.Offset)
		end = currentRow + offset + 1
	case UnboundedFollowing:
		end = p.size
	}

	// Ensure valid bounds
	start = max(0, start)
	end = min(p.size, end)
	if start > end {
		start = end
	}

	return start, end
}

// calculateRangeBounds calculates frame boundaries for RANGE frame type
func (p *WindowPartition) calculateRangeBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// Without ordering, treat as entire partition
	if !p.isOrdered || len(p.orderBy) == 0 {
		return 0, p.size
	}

	// Get the current row's order value
	currentValue := p.GetOrderValue(currentRow)

	// Null values - handle separately
	if currentValue == nil {
		return p.calculateNullRangeBounds(currentRow, frame)
	}

	// Check if the value at the index is null using the series
	s, ok := p.series[p.orderBy[0].Column]
	if ok {
		idx := p.orderIndices[currentRow]
		if s.IsNull(idx) {
			return p.calculateNullRangeBounds(currentRow, frame)
		}
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
	case CurrentRow:
		// Find first row with same value (peer row)
		for i := 0; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if compareValues(val, currentValue) == 0 {
				return i
			}
		}
		return currentRow
	case Preceding:
		// Find first row where value >= (currentValue - offset)
		target := subtractOffset(currentValue, bound.Offset)
		for i := 0; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val == nil {
				continue
			}
			if compareValues(val, target) >= 0 {
				return i
			}
		}
		return p.size
	case Following:
		// Find first row where value >= (currentValue + offset)
		target := addOffset(currentValue, bound.Offset)
		for i := 0; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val == nil {
				continue
			}
			if compareValues(val, target) >= 0 {
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
	case UnboundedFollowing:
		return p.size
	case CurrentRow:
		// Find last row with same value (peer row) + 1
		lastPeer := currentRow
		for i := p.size - 1; i >= 0; i-- {
			val := p.GetOrderValue(i)
			if compareValues(val, currentValue) == 0 {
				lastPeer = i
				break
			}
		}
		return lastPeer + 1
	case Preceding:
		// Find last row where value <= (currentValue - offset) + 1
		target := subtractOffset(currentValue, bound.Offset)
		lastMatch := -1
		for i := 0; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val == nil {
				continue
			}
			if compareValues(val, target) <= 0 {
				lastMatch = i
			}
		}
		return lastMatch + 1
	case Following:
		// Find last row where value <= (currentValue + offset) + 1
		target := addOffset(currentValue, bound.Offset)
		lastMatch := -1
		for i := 0; i < p.size; i++ {
			val := p.GetOrderValue(i)
			if val == nil {
				continue
			}
			if compareValues(val, target) <= 0 {
				lastMatch = i
			}
		}
		return lastMatch + 1
	case UnboundedPreceding:
		return 0
	}
	return p.size
}

// calculateNullRangeBounds handles RANGE bounds for null values
func (p *WindowPartition) calculateNullRangeBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// For null values in RANGE frames, include all nulls as peers
	s, ok := p.series[p.orderBy[0].Column]
	if !ok {
		return 0, p.size
	}

	// Find all null peers
	nullStart := p.size
	nullEnd := 0
	for i := 0; i < p.size; i++ {
		idx := p.orderIndices[i]
		if s.IsNull(idx) {
			if i < nullStart {
				nullStart = i
			}
			if i > nullEnd {
				nullEnd = i
			}
		}
	}

	if nullStart > nullEnd {
		return currentRow, currentRow + 1
	}

	// For CURRENT ROW bounds, return just the null peer group
	if frame.Start.Type == CurrentRow && frame.End.Type == CurrentRow {
		return nullStart, nullEnd + 1
	}

	// For unbounded, expand accordingly
	if frame.Start.Type == UnboundedPreceding {
		start = 0
	} else {
		start = nullStart
	}

	if frame.End.Type == UnboundedFollowing {
		end = p.size
	} else {
		end = nullEnd + 1
	}

	return start, end
}

// addOffset adds an offset to a value for RANGE calculations
func addOffset(value interface{}, offset interface{}) interface{} {
	switch v := value.(type) {
	case int32:
		switch o := offset.(type) {
		case int:
			return v + int32(o)
		case int32:
			return v + o
		case int64:
			return v + int32(o)
		case float64:
			return float64(v) + o
		}
	case int64:
		switch o := offset.(type) {
		case int:
			return v + int64(o)
		case int32:
			return v + int64(o)
		case int64:
			return v + o
		case float64:
			return float64(v) + o
		}
	case float64:
		switch o := offset.(type) {
		case int:
			return v + float64(o)
		case int32:
			return v + float64(o)
		case int64:
			return v + float64(o)
		case float64:
			return v + o
		}
	}
	// For non-numeric types, RANGE doesn't make sense with offsets
	return value
}

// subtractOffset subtracts an offset from a value for RANGE calculations
func subtractOffset(value interface{}, offset interface{}) interface{} {
	switch v := value.(type) {
	case int32:
		switch o := offset.(type) {
		case int:
			return v - int32(o)
		case int32:
			return v - o
		case int64:
			return v - int32(o)
		case float64:
			return float64(v) - o
		}
	case int64:
		switch o := offset.(type) {
		case int:
			return v - int64(o)
		case int32:
			return v - int64(o)
		case int64:
			return v - o
		case float64:
			return float64(v) - o
		}
	case float64:
		switch o := offset.(type) {
		case int:
			return v - float64(o)
		case int32:
			return v - float64(o)
		case int64:
			return v - float64(o)
		case float64:
			return v - o
		}
	}
	// For non-numeric types, RANGE doesn't make sense with offsets
	return value
}

// calculateGroupsBounds calculates frame boundaries for GROUPS frame type
func (p *WindowPartition) calculateGroupsBounds(currentRow int, frame *FrameSpec) (start, end int) {
	// Without ordering, treat as entire partition
	if !p.isOrdered || len(p.orderBy) == 0 {
		return 0, p.size
	}

	// Find peer groups
	groups := p.findPeerGroups()
	numGroups := len(groups)

	// Find which group the current row belongs to
	currentGroup := -1
	for g, group := range groups {
		for _, idx := range group {
			if idx == currentRow {
				currentGroup = g
				break
			}
		}
		if currentGroup >= 0 {
			break
		}
	}

	// Current row not found in groups
	if currentGroup < 0 {
		return 0, p.size
	}

	// Calculate start group
	startGroup := p.calculateGroupStartBoundary(currentGroup, frame.Start, numGroups)

	// Calculate end group
	endGroup := p.calculateGroupEndBoundary(currentGroup, frame.End, numGroups)

	// Ensure valid bounds
	startGroup = max(0, startGroup)
	endGroup = min(numGroups-1, endGroup)

	// Convert group indices to row indices
	start = groups[startGroup][0]

	// End is the first row of the next group (exclusive)
	if endGroup+1 < numGroups {
		end = groups[endGroup+1][0]
	} else {
		// Include all remaining rows
		end = p.size
	}

	return start, end
}

// findPeerGroups identifies groups of rows with the same order value
func (p *WindowPartition) findPeerGroups() [][]int {
	if p.size == 0 {
		return nil
	}

	groups := make([][]int, 0)
	currentGroup := []int{0}

	for i := 1; i < p.size; i++ {
		prevVal := p.GetOrderValue(i - 1)
		currVal := p.GetOrderValue(i)

		// Check if this row is a peer of the current group
		if compareValues(prevVal, currVal) == 0 {
			// Same value, add to current group
			currentGroup = append(currentGroup, i)
		} else {
			// Different value, start new group
			groups = append(groups, currentGroup)
			currentGroup = []int{i}
		}
	}

	// Add the last group
	groups = append(groups, currentGroup)
	return groups
}

// calculateGroupStartBoundary calculates the start group index for a boundary
func (p *WindowPartition) calculateGroupStartBoundary(currentGroup int, bound FrameBound, numGroups int) int {
	switch bound.Type {
	case UnboundedPreceding:
		return 0
	case Preceding:
		offset, _ := toInt(bound.Offset)
		return currentGroup - offset
	case CurrentRow:
		return currentGroup
	case Following:
		offset, _ := toInt(bound.Offset)
		return currentGroup + offset
	case UnboundedFollowing:
		return numGroups - 1
	}
	return 0
}

// calculateGroupEndBoundary calculates the end group index for a boundary
func (p *WindowPartition) calculateGroupEndBoundary(currentGroup int, bound FrameBound, numGroups int) int {
	switch bound.Type {
	case UnboundedFollowing:
		return numGroups - 1
	case Following:
		offset, _ := toInt(bound.Offset)
		return currentGroup + offset
	case CurrentRow:
		return currentGroup
	case Preceding:
		offset, _ := toInt(bound.Offset)
		return currentGroup - offset
	case UnboundedPreceding:
		return 0
	}
	return numGroups - 1
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
	case int32:
		vb, ok := b.(int32)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case int64:
		vb, ok := b.(int64)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case float64:
		vb, ok := b.(float64)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case string:
		vb, ok := b.(string)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	}

	// For other types, consider them equal
	return 0
}

// toInt converts an interface{} offset to an int
func toInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int32:
		return int(val), true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	}
	return 0, false
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
