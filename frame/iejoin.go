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
	bitsPerWord   = 64
	chunkSize     = 1024 // bits per filter chunk
	wordsPerChunk = chunkSize / bitsPerWord
)

func newFilteredBitArray(length int) *FilteredBitArray {
	numWords := (length + bitsPerWord - 1) / bitsPerWord
	numFilterWords := (numWords + wordsPerChunk - 1) / wordsPerChunk
	return &FilteredBitArray{
		bits:   make([]uint64, numWords),
		filter: make([]uint64, numFilterWords),
		len:    length,
	}
}

func (f *FilteredBitArray) setBit(index int) {
	if index < 0 || index >= f.len {
		return
	}
	wordIdx := index / bitsPerWord
	bitIdx := uint(index % bitsPerWord)
	f.bits[wordIdx] |= 1 << bitIdx

	// Update filter
	filterWord := wordIdx / wordsPerChunk
	filterBit := uint(wordIdx % wordsPerChunk)
	f.filter[filterWord] |= 1 << filterBit
}

// forEachSetBitFrom iterates over all set bits starting from startIdx
func (f *FilteredBitArray) forEachSetBitFrom(startIdx int, action func(int)) {
	if startIdx >= f.len {
		return
	}
	if startIdx < 0 {
		startIdx = 0
	}

	startWord := startIdx / bitsPerWord
	startBit := uint(startIdx % bitsPerWord)

	for wi := startWord; wi < len(f.bits); wi++ {
		// Check filter first
		chunkIdx := wi / wordsPerChunk
		chunkBit := uint(wi % wordsPerChunk)
		if f.filter[chunkIdx]&(1<<chunkBit) == 0 {
			continue // No set bits in this word
		}

		word := f.bits[wi]
		if word == 0 {
			continue
		}

		// Mask off bits before startBit in the first word
		if wi == startWord && startBit > 0 {
			word &= ^((1 << startBit) - 1)
		}

		for word != 0 {
			// Find lowest set bit
			tz := trailingZeros64(word)
			bitIndex := wi*bitsPerWord + tz
			if bitIndex >= f.len {
				return
			}
			action(bitIndex)
			word &= word - 1 // Clear lowest set bit
		}
	}
}

// trailingZeros64 counts trailing zero bits
func trailingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	for x&1 == 0 {
		n++
		x >>= 1
	}
	return n
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
func extractInequalityPredicates(left, right *DataFrame, predicates []expr.Expr) (
	leftCol1, rightCol1 string, op1 InequalityOperator,
	leftCol2, rightCol2 string, op2 InequalityOperator,
	hasTwoOps bool, err error) {

	if len(predicates) == 0 {
		err = fmt.Errorf("at least one predicate required")
		return
	}

	// Extract first predicate
	leftCol1, rightCol1, op1, err = extractSinglePredicate(left, right, predicates[0])
	if err != nil {
		return
	}

	if len(predicates) >= 2 {
		// Extract second predicate
		leftCol2, rightCol2, op2, err = extractSinglePredicate(left, right, predicates[1])
		if err != nil {
			return
		}
		hasTwoOps = true
	}

	return
}

// extractSinglePredicate extracts column names and operator from a binary expression
func extractSinglePredicate(left, right *DataFrame, pred expr.Expr) (
	leftCol, rightCol string, op InequalityOperator, err error) {

	binExpr, ok := pred.(*expr.BinaryExpr)
	if !ok {
		err = fmt.Errorf("predicate must be a binary expression")
		return
	}

	// Get left and right column names
	leftColExpr, leftIsCol := binExpr.Left().(*expr.ColumnExpr)
	rightColExpr, rightIsCol := binExpr.Right().(*expr.ColumnExpr)

	if !leftIsCol || !rightIsCol {
		err = fmt.Errorf("both sides of predicate must be column references")
		return
	}

	leftColName := leftColExpr.Name()
	rightColName := rightColExpr.Name()

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
		err = fmt.Errorf("unsupported operator for inequality join: must be <, <=, >, >=")
		return
	}

	// Determine which column belongs to which DataFrame
	leftHasLeft := left.HasColumn(leftColName)
	rightHasRight := right.HasColumn(rightColName)

	if leftHasLeft && rightHasRight {
		leftCol = leftColName
		rightCol = rightColName
		return
	}

	// Try swapping
	leftHasRight := left.HasColumn(rightColName)
	rightHasLeft := right.HasColumn(leftColName)

	if leftHasRight && rightHasLeft {
		leftCol = rightColName
		rightCol = leftColName
		// Invert operator
		switch op {
		case OpLt:
			op = OpGt
		case OpLe:
			op = OpGe
		case OpGt:
			op = OpLt
		case OpGe:
			op = OpLe
		}
		return
	}

	err = fmt.Errorf("cannot determine column assignment for predicate: %s", pred.String())
	return
}

// getNumericValues extracts float64 values from a series for comparison
func getNumericValues(s series.Series) ([]float64, error) {
	values := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			return nil, fmt.Errorf("null values not supported in IEJoin; filter nulls first")
		}
		values[i] = toFloat64Value(s.Get(i))
	}
	return values, nil
}

// ieJoinTwoOperators implements the full IEJoin algorithm for two inequality operators
func ieJoinTwoOperators(
	leftX, rightX []float64,
	leftY, rightY []float64,
	op1, op2 InequalityOperator,
) *ieJoinResult {
	result := &ieJoinResult{}

	nLeft := len(leftX)
	nRight := len(rightX)
	total := nLeft + nRight

	// Step 1: Build L1 array (combined x values, sorted)
	l1 := make([]l1Item, total)
	for i := 0; i < nLeft; i++ {
		l1[i] = l1Item{
			rowIndex: int64(i + 1), // 1-based positive for LHS
			value:    leftX[i],
		}
	}
	for i := 0; i < nRight; i++ {
		l1[nLeft+i] = l1Item{
			rowIndex: -(int64(i) + 1), // -1-based for RHS
			value:    rightX[i],
		}
	}

	// Sort L1 by x value according to op1
	ascending := op1 == OpLt || op1 == OpLe
	sort.SliceStable(l1, func(i, j int) bool {
		if l1[i].value != l1[j].value {
			if ascending {
				return l1[i].value < l1[j].value
			}
			return l1[i].value > l1[j].value
		}
		// For strict inequalities, RHS entries should come before LHS at same value
		if op1.isStrict() {
			return l1[i].rowIndex < 0 && l1[j].rowIndex > 0
		}
		return false
	})

	// Step 2: Build array of y values in L1 order
	yOrderedByX := make([]float64, total)
	for i, item := range l1 {
		if item.rowIndex > 0 {
			yOrderedByX[i] = leftY[item.rowIndex-1]
		} else {
			yOrderedByX[i] = rightY[-(item.rowIndex) - 1]
		}
	}

	// Step 3: Get L2 order (indices into L1, sorted by y values)
	l2Order := make([]int, total)
	for i := range l2Order {
		l2Order[i] = i
	}

	ascending2 := op2 == OpGt || op2 == OpGe
	sort.SliceStable(l2Order, func(i, j int) bool {
		if yOrderedByX[l2Order[i]] != yOrderedByX[l2Order[j]] {
			if ascending2 {
				return yOrderedByX[l2Order[i]] < yOrderedByX[l2Order[j]]
			}
			return yOrderedByX[l2Order[i]] > yOrderedByX[l2Order[j]]
		}
		return false
	})

	// Build L2 with run_end markers
	l2 := buildL2Array(yOrderedByX, l2Order)

	// Step 4: Traverse L2 and find matches using bit array
	bitArray := newFilteredBitArray(total)

	i := 0
	for i < len(l2) {
		if op2.isStrict() {
			// For strict inequalities, process each L2 entry directly
			item := l2[i]
			l1Idx := item.l1Index

			if l1[l1Idx].rowIndex > 0 {
				// LHS entry: find matches in RHS that have been visited
				leftRow := int(l1[l1Idx].rowIndex - 1)
				startSearch := 0
				if op1.isStrict() {
					startSearch = l1Idx + 1
				} else {
					startSearch = l1Idx
				}
				bitArray.forEachSetBitFrom(startSearch, func(bitIdx int) {
					if l1[bitIdx].rowIndex < 0 {
						rightRow := int(-(l1[bitIdx].rowIndex) - 1)
						result.leftIndices = append(result.leftIndices, leftRow)
						result.rightIndices = append(result.rightIndices, rightRow)
					}
				})
			} else {
				// RHS entry: mark as visited
				bitArray.setBit(l1Idx)
			}
			i++
		} else {
			// For non-strict inequalities, track runs of equal y values
			runStart := i
			for i < len(l2) && !l2[i].runEnd {
				i++
			}
			if i < len(l2) {
				i++ // include the run-end item
			}
			runEnd := i

			// Mark RHS entries as visited first
			for j := runStart; j < runEnd; j++ {
				l1Idx := l2[j].l1Index
				if l1[l1Idx].rowIndex < 0 {
					bitArray.setBit(l1Idx)
				}
			}

			// Process all LHS entries in this run
			for j := runStart; j < runEnd; j++ {
				l1Idx := l2[j].l1Index
				if l1[l1Idx].rowIndex > 0 {
					leftRow := int(l1[l1Idx].rowIndex - 1)
					startSearch := l1Idx
					bitArray.forEachSetBitFrom(startSearch, func(bitIdx int) {
						if l1[bitIdx].rowIndex < 0 {
							rightRow := int(-(l1[bitIdx].rowIndex) - 1)
							result.leftIndices = append(result.leftIndices, leftRow)
							result.rightIndices = append(result.rightIndices, rightRow)
						}
					})
				}
			}
		}
	}

	return result
}

// buildL2Array creates L2 items with run_end markers
func buildL2Array(yOrderedByX []float64, l2Order []int) []l2Item {
	n := len(l2Order)
	l2 := make([]l2Item, n)

	for i := 0; i < n; i++ {
		l2[i].l1Index = l2Order[i]
		// Check if this is end of a run (next item has different y value, or is the last)
		if i == n-1 {
			l2[i].runEnd = true
		} else {
			l2[i].runEnd = yOrderedByX[l2Order[i]] != yOrderedByX[l2Order[i+1]]
		}
	}

	return l2
}

// findSearchStartIndex finds where to start searching in L1 for matches
func findSearchStartIndex(l1 []l1Item, index int, op InequalityOperator) int {
	if op.isStrict() {
		return index + 1
	}
	return index
}

// exponentialSearch finds the partition point using exponential search
func exponentialSearch(items []l1Item, pred func(l1Item) bool) int {
	n := len(items)
	if n == 0 || !pred(items[0]) {
		return 0
	}

	// Exponential search to find bounds
	bound := 1
	for bound < n && pred(items[bound]) {
		bound *= 2
	}

	// Binary search within bounds
	lo := bound / 2
	hi := bound
	if hi > n {
		hi = n
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
	result := &ieJoinResult{}

	// Create sorted indices
	leftIdx := make([]int, len(leftVals))
	for i := range leftIdx {
		leftIdx[i] = i
	}
	rightIdx := make([]int, len(rightVals))
	for i := range rightIdx {
		rightIdx[i] = i
	}

	// Sort according to operator direction
	sort.Slice(leftIdx, func(i, j int) bool {
		return leftVals[leftIdx[i]] < leftVals[leftIdx[j]]
	})
	sort.Slice(rightIdx, func(i, j int) bool {
		return rightVals[rightIdx[i]] < rightVals[rightIdx[j]]
	})

	// Create predicate function
	pred := func(lv, rv float64) bool {
		switch op {
		case OpLt:
			return lv < rv
		case OpLe:
			return lv <= rv
		case OpGt:
			return lv > rv
		case OpGe:
			return lv >= rv
		}
		return false
	}

	// For < and <= operators: for each left value, find matching right values
	switch op {
	case OpLt, OpLe:
		// Left values should be smaller than right values
		ri := 0
		for _, li := range leftIdx {
			lv := leftVals[li]
			// Find first right value that satisfies predicate
			for ri < len(rightIdx) && rightVals[rightIdx[ri]] <= lv {
				if op == OpLe && rightVals[rightIdx[ri]] == lv {
					break
				}
				ri++
			}
			startRI := ri
			if op == OpLe {
				// Find first right >= lv
				startRI = sort.Search(len(rightIdx), func(k int) bool {
					return rightVals[rightIdx[k]] >= lv
				})
				if op == OpLt {
					startRI = sort.Search(len(rightIdx), func(k int) bool {
						return rightVals[rightIdx[k]] > lv
					})
				}
			}
			// All right values from startRI onward match
			for rj := startRI; rj < len(rightIdx); rj++ {
				if pred(lv, rightVals[rightIdx[rj]]) {
					result.leftIndices = append(result.leftIndices, li)
					result.rightIndices = append(result.rightIndices, rightIdx[rj])
				}
			}
		}
	case OpGt, OpGe:
		// Left values should be greater than right values
		for _, li := range leftIdx {
			lv := leftVals[li]
			for _, rj := range rightIdx {
				rv := rightVals[rj]
				if pred(lv, rv) {
					result.leftIndices = append(result.leftIndices, li)
					result.rightIndices = append(result.rightIndices, rj)
				}
			}
		}
	}

	return result
}

// JoinWhereIEJoin performs an inequality join using the IEJoin algorithm
func (df *DataFrame) JoinWhereIEJoin(other *DataFrame, predicates ...expr.Expr) (*DataFrame, error) {
	if len(predicates) == 0 {
		return nil, fmt.Errorf("at least one predicate required")
	}

	// Extract and validate predicates
	leftCol1, rightCol1, op1, leftCol2, rightCol2, op2, hasTwoOps, err :=
		extractInequalityPredicates(df, other, predicates)
	if err != nil {
		return nil, fmt.Errorf("extracting predicates: %w", err)
	}

	// Get column data
	lc1, err := df.Column(leftCol1)
	if err != nil {
		return nil, err
	}
	rc1, err := other.Column(rightCol1)
	if err != nil {
		return nil, err
	}

	leftX, err := getNumericValues(lc1)
	if err != nil {
		return nil, fmt.Errorf("left column %q: %w", leftCol1, err)
	}
	rightX, err := getNumericValues(rc1)
	if err != nil {
		return nil, fmt.Errorf("right column %q: %w", rightCol1, err)
	}

	var joinResult *ieJoinResult

	if hasTwoOps {
		// Full IEJoin with two operators
		lc2, err := df.Column(leftCol2)
		if err != nil {
			return nil, err
		}
		rc2, err := other.Column(rightCol2)
		if err != nil {
			return nil, err
		}

		leftY, err := getNumericValues(lc2)
		if err != nil {
			return nil, fmt.Errorf("left column %q: %w", leftCol2, err)
		}
		rightY, err := getNumericValues(rc2)
		if err != nil {
			return nil, fmt.Errorf("right column %q: %w", rightCol2, err)
		}

		joinResult = ieJoinTwoOperators(leftX, rightX, leftY, rightY, op1, op2)
	} else {
		// Single operator - use piecewise merge join
		joinResult = piecewiseMergeJoin(leftX, rightX, op1)
	}

	// Build result DataFrame
	var resultCols []series.Series

	// Add left columns
	for _, col := range df.columns {
		if len(joinResult.leftIndices) == 0 {
			resultCols = append(resultCols, createNullSeries(col.Name(), col.DataType(), 0))
		} else {
			if taken, ok := series.TakeFast(col, joinResult.leftIndices); ok {
				resultCols = append(resultCols, taken)
			} else {
				resultCols = append(resultCols, col.Take(joinResult.leftIndices))
			}
		}
	}

	// Add right columns with suffix for duplicates
	for _, col := range other.columns {
		name := col.Name()
		if df.HasColumn(name) {
			name = name + "_right"
		}
		if len(joinResult.rightIndices) == 0 {
			resultCols = append(resultCols, createNullSeries(name, col.DataType(), 0))
		} else {
			var s series.Series
			if taken, ok := series.TakeFast(col, joinResult.rightIndices); ok {
				s = taken
			} else {
				s = col.Take(joinResult.rightIndices)
			}
			if s.Name() != name {
				s = s.Rename(name)
			}
			resultCols = append(resultCols, s)
		}
	}

	return NewDataFrame(resultCols...)
}
