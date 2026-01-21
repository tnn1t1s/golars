package compute

import (
	"github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/series"
)

type stringKey struct {
	value string
	valid bool
}

type stringPairKey struct {
	a      string
	b      string
	aValid bool
	bValid bool
}

type stringHashTable struct {
	buckets map[stringKey][]int
}

type stringPairHashTable struct {
	buckets map[stringPairKey][]int
}

func stringValuesFromSeries(s series.Series) ([]string, []bool) {
	if values, validity, ok := series.StringValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]string, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case string:
			values[i] = val
			validity[i] = true
		case []byte:
			values[i] = string(val)
			validity[i] = true
		}
	}
	return values, validity
}

func buildStringHashTable(values []string, validity []bool) *stringHashTable {
	buckets := make(map[stringKey][]int, len(values))
	for i, v := range values {
		key := stringKey{value: v, valid: validity[i]}
		buckets[key] = append(buckets[key], i)
	}
	return &stringHashTable{buckets: buckets}
}

func buildStringPairHashTable(a []string, aValid []bool, b []string, bValid []bool) *stringPairHashTable {
	buckets := make(map[stringPairKey][]int, len(a))
	for i := range a {
		key := stringPairKey{
			a:      a[i],
			b:      b[i],
			aValid: aValid[i],
			bValid: bValid[i],
		}
		buckets[key] = append(buckets[key], i)
	}
	return &stringPairHashTable{buckets: buckets}
}

func probeManyString(ht *stringHashTable, keys []string, validity []bool) ([]int, []int) {
	leftIndices := make([]int, 0, len(keys))
	rightIndices := make([]int, 0, len(keys))
	for i, v := range keys {
		key := stringKey{value: v, valid: validity[i]}
		matches := ht.buckets[key]
		for _, match := range matches {
			leftIndices = append(leftIndices, match)
			rightIndices = append(rightIndices, i)
		}
	}
	return leftIndices, rightIndices
}

func probeManyStringParallel(ht *stringHashTable, keys []string, validity []bool) ([]int, []int) {
	n := len(keys)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return probeManyString(ht, keys, validity)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			left, right := probeManyString(ht, keys[offset:limit], validity[offset:limit])
			if offset != 0 {
				for i := range right {
					right[i] += offset
				}
			}
			parts[idx] = part{left: left, right: right}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	buildIndices := make([]int, 0, total)
	probeIndices := make([]int, 0, total)
	for _, p := range parts {
		buildIndices = append(buildIndices, p.left...)
		probeIndices = append(probeIndices, p.right...)
	}
	return buildIndices, probeIndices
}

func probeManyStringPair(ht *stringPairHashTable, a []string, aValid []bool, b []string, bValid []bool) ([]int, []int) {
	leftIndices := make([]int, 0, len(a))
	rightIndices := make([]int, 0, len(a))
	for i := range a {
		key := stringPairKey{
			a:      a[i],
			b:      b[i],
			aValid: aValid[i],
			bValid: bValid[i],
		}
		matches := ht.buckets[key]
		for _, match := range matches {
			leftIndices = append(leftIndices, match)
			rightIndices = append(rightIndices, i)
		}
	}
	return leftIndices, rightIndices
}

func probeManyStringPairParallel(ht *stringPairHashTable, a []string, aValid []bool, b []string, bValid []bool) ([]int, []int) {
	n := len(a)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return probeManyStringPair(ht, a, aValid, b, bValid)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			left, right := probeManyStringPair(
				ht,
				a[offset:limit],
				aValid[offset:limit],
				b[offset:limit],
				bValid[offset:limit],
			)
			if offset != 0 {
				for i := range right {
					right[i] += offset
				}
			}
			parts[idx] = part{left: left, right: right}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	buildIndices := make([]int, 0, total)
	probeIndices := make([]int, 0, total)
	for _, p := range parts {
		buildIndices = append(buildIndices, p.left...)
		probeIndices = append(probeIndices, p.right...)
	}
	return buildIndices, probeIndices
}

func leftJoinSequentialString(ht *stringHashTable, keys []string, validity []bool) ([]int, []int) {
	leftIndices := make([]int, 0, len(keys))
	rightIndices := make([]int, 0, len(keys))

	for i, v := range keys {
		key := stringKey{value: v, valid: validity[i]}
		matches := ht.buckets[key]
		if len(matches) == 0 {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
			continue
		}
		for _, idx := range matches {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, idx)
		}
	}
	return leftIndices, rightIndices
}

func leftJoinParallelString(ht *stringHashTable, keys []string, validity []bool) ([]int, []int) {
	n := len(keys)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return leftJoinSequentialString(ht, keys, validity)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			localLeft := make([]int, 0, limit-offset)
			localRight := make([]int, 0, limit-offset)
			for i := offset; i < limit; i++ {
				key := stringKey{value: keys[i], valid: validity[i]}
				matches := ht.buckets[key]
				if len(matches) == 0 {
					localLeft = append(localLeft, i)
					localRight = append(localRight, -1)
					continue
				}
				for _, idx := range matches {
					localLeft = append(localLeft, i)
					localRight = append(localRight, idx)
				}
			}
			parts[idx] = part{left: localLeft, right: localRight}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	leftIndices := make([]int, 0, total)
	rightIndices := make([]int, 0, total)
	for _, p := range parts {
		leftIndices = append(leftIndices, p.left...)
		rightIndices = append(rightIndices, p.right...)
	}
	return leftIndices, rightIndices
}

func leftJoinSequentialStringPair(ht *stringPairHashTable, a []string, aValid []bool, b []string, bValid []bool) ([]int, []int) {
	leftIndices := make([]int, 0, len(a))
	rightIndices := make([]int, 0, len(a))

	for i := range a {
		key := stringPairKey{
			a:      a[i],
			b:      b[i],
			aValid: aValid[i],
			bValid: bValid[i],
		}
		matches := ht.buckets[key]
		if len(matches) == 0 {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
			continue
		}
		for _, idx := range matches {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, idx)
		}
	}
	return leftIndices, rightIndices
}

func leftJoinParallelStringPair(ht *stringPairHashTable, a []string, aValid []bool, b []string, bValid []bool) ([]int, []int) {
	n := len(a)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return leftJoinSequentialStringPair(ht, a, aValid, b, bValid)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			localLeft := make([]int, 0, limit-offset)
			localRight := make([]int, 0, limit-offset)
			for i := offset; i < limit; i++ {
				key := stringPairKey{
					a:      a[i],
					b:      b[i],
					aValid: aValid[i],
					bValid: bValid[i],
				}
				matches := ht.buckets[key]
				if len(matches) == 0 {
					localLeft = append(localLeft, i)
					localRight = append(localRight, -1)
					continue
				}
				for _, idx := range matches {
					localLeft = append(localLeft, i)
					localRight = append(localRight, idx)
				}
			}
			parts[idx] = part{left: localLeft, right: localRight}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	leftIndices := make([]int, 0, total)
	rightIndices := make([]int, 0, total)
	for _, p := range parts {
		leftIndices = append(leftIndices, p.left...)
		rightIndices = append(rightIndices, p.right...)
	}
	return leftIndices, rightIndices
}

// StringJoinIndices performs a hash join on string columns and returns indices.
func StringJoinIndices(left, right series.Series) ([]int, []int, error) {
	leftKeys, leftValid := stringValuesFromSeries(left)
	rightKeys, rightValid := stringValuesFromSeries(right)

	var buildKeys, probeKeys []string
	var buildValid, probeValid []bool
	var swapped bool

	if len(leftKeys) <= len(rightKeys) {
		buildKeys, buildValid = leftKeys, leftValid
		probeKeys, probeValid = rightKeys, rightValid
		swapped = false
	} else {
		buildKeys, buildValid = rightKeys, rightValid
		probeKeys, probeValid = leftKeys, leftValid
		swapped = true
	}

	ht := buildStringHashTable(buildKeys, buildValid)

	var buildIndices, probeIndices []int
	if shouldParallelProbe(len(probeKeys)) {
		buildIndices, probeIndices = probeManyStringParallel(ht, probeKeys, probeValid)
	} else {
		buildIndices, probeIndices = probeManyString(ht, probeKeys, probeValid)
	}

	if swapped {
		return probeIndices, buildIndices, nil
	}
	return buildIndices, probeIndices, nil
}

// StringLeftJoinIndices performs a left join on string columns and returns indices.
func StringLeftJoinIndices(left, right series.Series) ([]int, []int, error) {
	leftKeys, leftValid := stringValuesFromSeries(left)
	rightKeys, rightValid := stringValuesFromSeries(right)

	ht := buildStringHashTable(rightKeys, rightValid)
	if shouldParallelProbe(len(leftKeys)) {
		leftIndices, rightIndices := leftJoinParallelString(ht, leftKeys, leftValid)
		return leftIndices, rightIndices, nil
	}
	leftIndices, rightIndices := leftJoinSequentialString(ht, leftKeys, leftValid)
	return leftIndices, rightIndices, nil
}

// StringPairJoinIndices performs a hash join on two string columns and returns indices.
func StringPairJoinIndices(leftA, leftB, rightA, rightB series.Series) ([]int, []int, error) {
	leftAKeys, leftAValid := stringValuesFromSeries(leftA)
	leftBKeys, leftBValid := stringValuesFromSeries(leftB)
	rightAKeys, rightAValid := stringValuesFromSeries(rightA)
	rightBKeys, rightBValid := stringValuesFromSeries(rightB)

	var buildA, buildB []string
	var buildAValid, buildBValid []bool
	var probeA, probeB []string
	var probeAValid, probeBValid []bool
	var swapped bool

	if len(leftAKeys) <= len(rightAKeys) {
		buildA, buildAValid = leftAKeys, leftAValid
		buildB, buildBValid = leftBKeys, leftBValid
		probeA, probeAValid = rightAKeys, rightAValid
		probeB, probeBValid = rightBKeys, rightBValid
		swapped = false
	} else {
		buildA, buildAValid = rightAKeys, rightAValid
		buildB, buildBValid = rightBKeys, rightBValid
		probeA, probeAValid = leftAKeys, leftAValid
		probeB, probeBValid = leftBKeys, leftBValid
		swapped = true
	}

	ht := buildStringPairHashTable(buildA, buildAValid, buildB, buildBValid)

	var buildIndices, probeIndices []int
	if shouldParallelProbe(len(probeA)) {
		buildIndices, probeIndices = probeManyStringPairParallel(
			ht,
			probeA,
			probeAValid,
			probeB,
			probeBValid,
		)
	} else {
		buildIndices, probeIndices = probeManyStringPair(
			ht,
			probeA,
			probeAValid,
			probeB,
			probeBValid,
		)
	}

	if swapped {
		return probeIndices, buildIndices, nil
	}
	return buildIndices, probeIndices, nil
}

// StringPairLeftJoinIndices performs a left join on two string columns and returns indices.
func StringPairLeftJoinIndices(leftA, leftB, rightA, rightB series.Series) ([]int, []int, error) {
	leftAKeys, leftAValid := stringValuesFromSeries(leftA)
	leftBKeys, leftBValid := stringValuesFromSeries(leftB)
	rightAKeys, rightAValid := stringValuesFromSeries(rightA)
	rightBKeys, rightBValid := stringValuesFromSeries(rightB)

	ht := buildStringPairHashTable(rightAKeys, rightAValid, rightBKeys, rightBValid)

	if shouldParallelProbe(len(leftAKeys)) {
		leftIndices, rightIndices := leftJoinParallelStringPair(
			ht,
			leftAKeys,
			leftAValid,
			leftBKeys,
			leftBValid,
		)
		return leftIndices, rightIndices, nil
	}

	leftIndices, rightIndices := leftJoinSequentialStringPair(
		ht,
		leftAKeys,
		leftAValid,
		leftBKeys,
		leftBValid,
	)
	return leftIndices, rightIndices, nil
}
