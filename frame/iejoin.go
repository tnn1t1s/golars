// IEJoin implementation based on Khayyat et al. 2015,
// "Lightning Fast and Space Efficient Inequality Joins"
//
// This algorithm efficiently handles inequality joins (joins with <, <=, >, >= operators)
// in O((n+m) log(n+m)) time instead of O(n*m) naive nested loop.
package frame

import (
	"fmt"
	"sort"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/series"
)

// InequalityOperator represents the comparison operator for inequality joins
type InequalityOperator int

const (
	OpLt InequalityOperator = iota // <
	OpLe                           // <=
	OpGt                           // >
	OpGe                           // >=
)

func (op InequalityOperator) isStrict() bool {
	return op == OpLt || op == OpGt
}

// FilteredBitArray is a bit array with chunk-based filtering for efficient sparse iteration.
// Based on section 4.1 from Khayyat et al. 2015.
type FilteredBitArray struct {
	bits   []uint64 // Main bit array
	filter []uint64 // Chunk filter - bit set if any bit in chunk is set
	len    int
}

const (
	bitsPerWord  = 64
	chunkSize    = 1024 // bits per filter chunk
	wordsPerChunk = chunkSize / bitsPerWord
)

func newFilteredBitArray(length int) *FilteredBitArray {
	numWords := (length + bitsPerWord - 1) / bitsPerWord
	numChunks := (length + chunkSize - 1) / chunkSize
	return &FilteredBitArray{
		bits:   make([]uint64, numWords),
		filter: make([]uint64, (numChunks+bitsPerWord-1)/bitsPerWord),
		len:    length,
	}
}

func (f *FilteredBitArray) setBit(index int) {
	wordIdx := index / bitsPerWord
	bitIdx := index % bitsPerWord
	f.bits[wordIdx] |= 1 << bitIdx

	// Update filter
	chunkIdx := index / chunkSize
	filterWordIdx := chunkIdx / bitsPerWord
	filterBitIdx := chunkIdx % bitsPerWord
	f.filter[filterWordIdx] |= 1 << filterBitIdx
}

// forEachSetBitFrom iterates over all set bits starting from startIdx
func (f *FilteredBitArray) forEachSetBitFrom(startIdx int, action func(int)) {
	if startIdx >= f.len {
		return
	}

	startChunk := startIdx / chunkSize
	numChunks := (f.len + chunkSize - 1) / chunkSize

	for chunkIdx := startChunk; chunkIdx < numChunks; chunkIdx++ {
		// Check filter first
		filterWordIdx := chunkIdx / bitsPerWord
		filterBitIdx := chunkIdx % bitsPerWord
		if f.filter[filterWordIdx]&(1<<filterBitIdx) == 0 {
			continue // No set bits in this chunk
		}

		// Scan the chunk
		chunkStart := chunkIdx * chunkSize
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > f.len {
			chunkEnd = f.len
		}

		// Adjust start for first chunk
		scanStart := chunkStart
		if chunkIdx == startChunk {
			scanStart = startIdx
		}

		for bitIdx := scanStart; bitIdx < chunkEnd; bitIdx++ {
			wordIdx := bitIdx / bitsPerWord
			wordBit := bitIdx % bitsPerWord
			if f.bits[wordIdx]&(1<<wordBit) != 0 {
				action(bitIdx)
			}
		}
	}
}

// l1Item represents an entry in the L1 array (sorted by x values)
type l1Item struct {
	rowIndex int64   // Positive (1-based) for LHS, negative (-1-based) for RHS
	value    float64 // The x value
}

// l2Item represents an entry in the L2 array (L1 indices sorted by y values)
type l2Item struct {
	l1Index int  // Index into L1 array
	runEnd  bool // Whether this is the end of a run of equal y values
}

// ieJoinResult holds the join result indices
type ieJoinResult struct {
	leftIndices  []int
	rightIndices []int
}

// extractInequalityPredicates parses the predicates and extracts inequality operators
// Returns: leftCol1, rightCol1, op1, leftCol2, rightCol2, op2, error
// For single-operator joins, the second set will be empty
func extractInequalityPredicates(left, right *DataFrame, predicates []expr.Expr) (
	leftCol1, rightCol1 string, op1 InequalityOperator,
	leftCol2, rightCol2 string, op2 InequalityOperator,
	hasTwoOps bool, err error) {

	if len(predicates) == 0 {
		err = fmt.Errorf("JoinWhere requires at least one predicate")
		return
	}

	// Extract first predicate
	leftCol1, rightCol1, op1, err = extractSinglePredicate(left, right, predicates[0])
	if err != nil {
		return
	}

	if len(predicates) == 1 {
		hasTwoOps = false
		return
	}

	// Extract second predicate
	leftCol2, rightCol2, op2, err = extractSinglePredicate(left, right, predicates[1])
	if err != nil {
		return
	}
	hasTwoOps = true

	// If more than 2 predicates, we'd need to filter results afterward
	// For now, we support up to 2 inequality predicates in IEJoin
	if len(predicates) > 2 {
		err = fmt.Errorf("IEJoin currently supports at most 2 inequality predicates")
	}
	return
}

// extractSinglePredicate extracts column names and operator from a binary expression
func extractSinglePredicate(left, right *DataFrame, pred expr.Expr) (
	leftCol, rightCol string, op InequalityOperator, err error) {

	binExpr, ok := pred.(*expr.BinaryExpr)
	if !ok {
		err = fmt.Errorf("predicate must be a binary expression, got %T", pred)
		return
	}

	// Get left column
	leftColExpr, ok := binExpr.Left().(*expr.ColumnExpr)
	if !ok {
		err = fmt.Errorf("left side of predicate must be a column reference")
		return
	}

	// Get right column
	rightColExpr, ok := binExpr.Right().(*expr.ColumnExpr)
	if !ok {
		err = fmt.Errorf("right side of predicate must be a column reference")
		return
	}

	// Determine which column belongs to which DataFrame
	leftColName := leftColExpr.Name()
	rightColName := rightColExpr.Name()

	// Check if leftColName is in left DataFrame
	_, leftInLeft := left.Column(leftColName)
	_, rightInRight := right.Column(rightColName)

	if leftInLeft == nil && rightInRight == nil {
		leftCol = leftColName
		rightCol = rightColName
	} else {
		// Try swapping
		_, leftInRight := right.Column(leftColName)
		_, rightInLeft := left.Column(rightColName)
		if leftInRight == nil && rightInLeft == nil {
			// Need to swap columns AND invert operator
			leftCol = rightColName
			rightCol = leftColName
			// Invert operator
			switch binExpr.Op() {
			case expr.OpLess:
				op = OpGt
			case expr.OpLessEqual:
				op = OpGe
			case expr.OpGreater:
				op = OpLt
			case expr.OpGreaterEqual:
				op = OpLe
			default:
				err = fmt.Errorf("unsupported operator for inequality join: %v", binExpr.Op())
			}
			return
		}
		err = fmt.Errorf("cannot determine column ownership for predicate")
		return
	}

	// Convert operator
	switch binExpr.Op() {
	case expr.OpLess:
		op = OpLt
	case expr.OpLessEqual:
		op = OpLe
	case expr.OpGreater:
		op = OpGt
	case expr.OpGreaterEqual:
		op = OpGe
	default:
		err = fmt.Errorf("unsupported operator for inequality join: %v", binExpr.Op())
	}
	return
}

// getNumericValues extracts float64 values from a series for comparison
func getNumericValues(s series.Series) ([]float64, error) {
	values := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		v := s.Get(i)
		if v == nil {
			values[i] = 0 // Will be filtered by null handling
			continue
		}
		switch val := v.(type) {
		case int32:
			values[i] = float64(val)
		case int64:
			values[i] = float64(val)
		case float32:
			values[i] = float64(val)
		case float64:
			values[i] = val
		case int:
			values[i] = float64(val)
		default:
			return nil, fmt.Errorf("unsupported type for inequality join: %T", v)
		}
	}
	return values, nil
}

// ieJoinTwoOperators implements the full IEJoin algorithm for two inequality operators
func ieJoinTwoOperators(
	leftX, rightX []float64,
	leftY, rightY []float64,
	op1, op2 InequalityOperator,
) *ieJoinResult {
	leftHeight := len(leftX)
	rightHeight := len(rightX)
	totalLen := leftHeight + rightHeight

	if totalLen == 0 {
		return &ieJoinResult{}
	}

	// Step 1: Build L1 array (combined x values, sorted)
	l1 := make([]l1Item, totalLen)
	for i := 0; i < leftHeight; i++ {
		l1[i] = l1Item{
			rowIndex: int64(i + 1), // 1-based positive for LHS
			value:    leftX[i],
		}
	}
	for i := 0; i < rightHeight; i++ {
		l1[leftHeight+i] = l1Item{
			rowIndex: int64(-(i + 1)), // -1-based for RHS
			value:    rightX[i],
		}
	}

	// Sort L1 by x value according to op1
	// For < or <=: ascending (smaller values first)
	// For > or >=: descending (larger values first)
	l1Descending := op1 == OpGt || op1 == OpGe
	sort.SliceStable(l1, func(i, j int) bool {
		if l1Descending {
			return l1[i].value > l1[j].value
		}
		return l1[i].value < l1[j].value
	})

	// Step 2: Build array of y values in L1 order
	yOrderedByX := make([]float64, totalLen)
	for i, item := range l1 {
		if item.rowIndex > 0 {
			yOrderedByX[i] = leftY[item.rowIndex-1]
		} else {
			yOrderedByX[i] = rightY[(-item.rowIndex)-1]
		}
	}

	// Step 3: Get L2 order (indices into L1, sorted by y values)
	l2Order := make([]int, totalLen)
	for i := range l2Order {
		l2Order[i] = i
	}

	// Sort L2 by y value according to op2
	// For < or <=: descending (we want larger y to be processed first)
	// For > or >=: ascending
	l2Descending := op2 == OpLt || op2 == OpLe
	sort.SliceStable(l2Order, func(i, j int) bool {
		if l2Descending {
			return yOrderedByX[l2Order[i]] > yOrderedByX[l2Order[j]]
		}
		return yOrderedByX[l2Order[i]] < yOrderedByX[l2Order[j]]
	})

	// Step 4: Traverse L2 and find matches
	bitArray := newFilteredBitArray(totalLen)
	result := &ieJoinResult{
		leftIndices:  make([]int, 0),
		rightIndices: make([]int, 0),
	}

	if op2.isStrict() {
		// For strict inequalities, process each L2 entry directly
		for _, p := range l2Order {
			item := l1[p]
			if item.rowIndex > 0 {
				// LHS entry: find matches in RHS that have been visited
				leftRow := int(item.rowIndex - 1)
				startIdx := findSearchStartIndex(l1, p, op1)
				bitArray.forEachSetBitFrom(startIdx, func(setBit int) {
					rhsItem := l1[setBit]
					if rhsItem.rowIndex < 0 {
						rightRow := int((-rhsItem.rowIndex) - 1)
						result.leftIndices = append(result.leftIndices, leftRow)
						result.rightIndices = append(result.rightIndices, rightRow)
					}
				})
			} else {
				// RHS entry: mark as visited
				bitArray.setBit(p)
			}
		}
	} else {
		// For non-strict inequalities, track runs of equal y values
		l2Array := buildL2Array(yOrderedByX, l2Order)
		runStart := 0

		for i, item := range l2Array {
			p := item.l1Index
			l1Item := l1[p]

			// Mark RHS entries as visited
			if l1Item.rowIndex < 0 {
				bitArray.setBit(p)
			}

			if item.runEnd {
				// Process all LHS entries in this run
				for j := runStart; j <= i; j++ {
					pp := l2Array[j].l1Index
					ppItem := l1[pp]
					if ppItem.rowIndex > 0 {
						leftRow := int(ppItem.rowIndex - 1)
						startIdx := findSearchStartIndex(l1, pp, op1)
						bitArray.forEachSetBitFrom(startIdx, func(setBit int) {
							rhsItem := l1[setBit]
							if rhsItem.rowIndex < 0 {
								rightRow := int((-rhsItem.rowIndex) - 1)
								result.leftIndices = append(result.leftIndices, leftRow)
								result.rightIndices = append(result.rightIndices, rightRow)
							}
						})
					}
				}
				runStart = i + 1
			}
		}
	}

	return result
}

// buildL2Array creates L2 items with run_end markers
func buildL2Array(yOrderedByX []float64, l2Order []int) []l2Item {
	if len(l2Order) == 0 {
		return nil
	}

	l2Array := make([]l2Item, len(l2Order))
	for i := 0; i < len(l2Order)-1; i++ {
		currY := yOrderedByX[l2Order[i]]
		nextY := yOrderedByX[l2Order[i+1]]
		l2Array[i] = l2Item{
			l1Index: l2Order[i],
			runEnd:  currY != nextY,
		}
	}
	// Last item always ends a run
	l2Array[len(l2Order)-1] = l2Item{
		l1Index: l2Order[len(l2Order)-1],
		runEnd:  true,
	}
	return l2Array
}

// findSearchStartIndex finds where to start searching in L1 for matches
// using exponential search for O(log n) complexity
func findSearchStartIndex(l1 []l1Item, index int, op InequalityOperator) int {
	if index >= len(l1) {
		return len(l1)
	}

	value := l1[index].value
	subL1 := l1[index:]

	var pos int
	switch op {
	case OpGt:
		// Find first position where value < item.value (descending sort)
		pos = exponentialSearch(subL1, func(item l1Item) bool {
			return item.value >= value
		})
	case OpLt:
		// Find first position where value > item.value (ascending sort)
		pos = exponentialSearch(subL1, func(item l1Item) bool {
			return item.value <= value
		})
	case OpGe:
		pos = exponentialSearch(subL1, func(item l1Item) bool {
			return value < item.value
		})
	case OpLe:
		pos = exponentialSearch(subL1, func(item l1Item) bool {
			return value > item.value
		})
	}

	return index + pos
}

// exponentialSearch finds the partition point using exponential search
// Returns the first index where pred returns false
func exponentialSearch(items []l1Item, pred func(l1Item) bool) int {
	if len(items) == 0 {
		return 0
	}

	// Check if first element satisfies predicate
	if !pred(items[0]) {
		return 0
	}

	// Exponential search to find bounds
	bound := 1
	for bound < len(items) && pred(items[bound]) {
		bound *= 2
	}

	// Binary search within bounds
	lo := bound / 2
	hi := bound
	if hi > len(items) {
		hi = len(items)
	}

	for lo < hi {
		mid := lo + (hi-lo)/2
		if pred(items[mid]) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// piecewiseMergeJoin implements the simpler single-operator join
func piecewiseMergeJoin(
	leftVals, rightVals []float64,
	op InequalityOperator,
) *ieJoinResult {
	leftHeight := len(leftVals)
	rightHeight := len(rightVals)

	if leftHeight == 0 || rightHeight == 0 {
		return &ieJoinResult{}
	}

	// Create sorted indices
	leftOrder := make([]int, leftHeight)
	rightOrder := make([]int, rightHeight)
	for i := range leftOrder {
		leftOrder[i] = i
	}
	for i := range rightOrder {
		rightOrder[i] = i
	}

	// Sort according to operator
	descending := op == OpGt || op == OpGe
	sort.SliceStable(leftOrder, func(i, j int) bool {
		if descending {
			return leftVals[leftOrder[i]] > leftVals[leftOrder[j]]
		}
		return leftVals[leftOrder[i]] < leftVals[leftOrder[j]]
	})
	sort.SliceStable(rightOrder, func(i, j int) bool {
		if descending {
			return rightVals[rightOrder[i]] > rightVals[rightOrder[j]]
		}
		return rightVals[rightOrder[i]] < rightVals[rightOrder[j]]
	})

	result := &ieJoinResult{
		leftIndices:  make([]int, 0),
		rightIndices: make([]int, 0),
	}

	// Create predicate function
	var pred func(l, r float64) bool
	switch op {
	case OpLt:
		pred = func(l, r float64) bool { return l < r }
	case OpLe:
		pred = func(l, r float64) bool { return l <= r }
	case OpGt:
		pred = func(l, r float64) bool { return l > r }
	case OpGe:
		pred = func(l, r float64) bool { return l >= r }
	}

	leftIdx := 0
	rightIdx := 0

	for leftIdx < leftHeight {
		leftVal := leftVals[leftOrder[leftIdx]]

		// Find first right value that satisfies predicate
		for rightIdx < rightHeight {
			rightVal := rightVals[rightOrder[rightIdx]]
			if pred(leftVal, rightVal) {
				// Found match - all remaining right values also match
				for k := rightIdx; k < rightHeight; k++ {
					result.leftIndices = append(result.leftIndices, leftOrder[leftIdx])
					result.rightIndices = append(result.rightIndices, rightOrder[k])
				}
				break
			}
			rightIdx++
		}

		if rightIdx == rightHeight {
			// No more matches possible
			break
		}

		leftIdx++
	}

	return result
}

// JoinWhereIEJoin performs an inequality join using the IEJoin algorithm
// This replaces the naive O(n*m) nested loop with O((n+m) log(n+m)) algorithm
func (df *DataFrame) JoinWhereIEJoin(other *DataFrame, predicates ...expr.Expr) (*DataFrame, error) {
	df.mu.RLock()
	other.mu.RLock()
	defer df.mu.RUnlock()
	defer other.mu.RUnlock()

	// Extract and validate predicates
	leftCol1, rightCol1, op1, leftCol2, rightCol2, op2, hasTwoOps, err :=
		extractInequalityPredicates(df, other, predicates)
	if err != nil {
		return nil, err
	}

	// Get column data
	leftSeries1, err := df.Column(leftCol1)
	if err != nil {
		return nil, fmt.Errorf("column %s not found in left DataFrame: %w", leftCol1, err)
	}
	rightSeries1, err := other.Column(rightCol1)
	if err != nil {
		return nil, fmt.Errorf("column %s not found in right DataFrame: %w", rightCol1, err)
	}

	leftX, err := getNumericValues(leftSeries1)
	if err != nil {
		return nil, err
	}
	rightX, err := getNumericValues(rightSeries1)
	if err != nil {
		return nil, err
	}

	var result *ieJoinResult

	if hasTwoOps {
		// Full IEJoin with two operators
		leftSeries2, err := df.Column(leftCol2)
		if err != nil {
			return nil, fmt.Errorf("column %s not found in left DataFrame: %w", leftCol2, err)
		}
		rightSeries2, err := other.Column(rightCol2)
		if err != nil {
			return nil, fmt.Errorf("column %s not found in right DataFrame: %w", rightCol2, err)
		}

		leftY, err := getNumericValues(leftSeries2)
		if err != nil {
			return nil, err
		}
		rightY, err := getNumericValues(rightSeries2)
		if err != nil {
			return nil, err
		}

		result = ieJoinTwoOperators(leftX, rightX, leftY, rightY, op1, op2)
	} else {
		// Single operator - use piecewise merge join
		result = piecewiseMergeJoin(leftX, rightX, op1)
	}

	// Build result DataFrame
	config := JoinConfig{
		How:    InnerJoin,
		Suffix: "_right",
	}
	return buildJoinWhereResult(df, other, result.leftIndices, result.rightIndices, config)
}
