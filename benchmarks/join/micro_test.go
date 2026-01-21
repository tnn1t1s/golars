package join

import (
	"fmt"
	"sync"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

const (
	microLeftRows  = 50_000
	microRightRows = 5_000
	microGroups    = 1_000
)

type lcg struct {
	state uint32
}

func (l *lcg) next() uint32 {
	l.state = l.state*1664525 + 1013904223
	return l.state
}

type microJoinData struct {
	innerIntLeft  *frame.DataFrame
	innerIntRight *frame.DataFrame

	innerStrLeft  *frame.DataFrame
	innerStrRight *frame.DataFrame

	innerStr2Left  *frame.DataFrame
	innerStr2Right *frame.DataFrame
}

var (
	microData     microJoinData
	microDataOnce sync.Once
)

func buildMicroData() {
	// Int64 join data.
	intKeysLeft, intValsLeft := buildIntKeys(microLeftRows, microGroups, 1)
	intKeysRight, intValsRight := buildIntKeys(microRightRows, microGroups, 2)

	microData.innerIntLeft = mustFrame(
		series.NewInt64Series("id", intKeysLeft),
		series.NewInt64Series("v", intValsLeft),
	)
	microData.innerIntRight = mustFrame(
		series.NewInt64Series("id", intKeysRight),
		series.NewInt64Series("w", intValsRight),
	)

	// String join data.
	strKeysLeft, strValsLeft := buildStringKeys(microLeftRows, microGroups, 3)
	strKeysRight, strValsRight := buildStringKeys(microRightRows, microGroups, 4)

	microData.innerStrLeft = mustFrame(
		series.NewStringSeries("id", strKeysLeft),
		series.NewInt64Series("v", strValsLeft),
	)
	microData.innerStrRight = mustFrame(
		series.NewStringSeries("id", strKeysRight),
		series.NewInt64Series("w", strValsRight),
	)

	// Two-key string join data.
	strAleft, strBleft, valsLeft := buildStringPairs(microLeftRows, microGroups, 5, 6)
	strAright, strBright, valsRight := buildStringPairs(microRightRows, microGroups, 7, 8)

	microData.innerStr2Left = mustFrame(
		series.NewStringSeries("id1", strAleft),
		series.NewStringSeries("id2", strBleft),
		series.NewInt64Series("v", valsLeft),
	)
	microData.innerStr2Right = mustFrame(
		series.NewStringSeries("id1", strAright),
		series.NewStringSeries("id2", strBright),
		series.NewInt64Series("w", valsRight),
	)
}

func mustFrame(cols ...series.Series) *frame.DataFrame {
	df, err := frame.NewDataFrame(cols...)
	if err != nil {
		panic(err)
	}
	return df
}

func buildIntKeys(rows, groups int, seed uint32) ([]int64, []int64) {
	rng := &lcg{state: seed}
	keys := make([]int64, rows)
	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		keys[i] = int64(rng.next() % uint32(groups))
		values[i] = int64(rng.next())
	}
	return keys, values
}

func buildStringKeys(rows, groups int, seed uint32) ([]string, []int64) {
	rng := &lcg{state: seed}
	keys := make([]string, rows)
	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		keys[i] = fmt.Sprintf("k%04d", rng.next()%uint32(groups))
		values[i] = int64(rng.next())
	}
	return keys, values
}

func buildStringPairs(rows, groups int, seedA, seedB uint32) ([]string, []string, []int64) {
	rngA := &lcg{state: seedA}
	rngB := &lcg{state: seedB}
	keysA := make([]string, rows)
	keysB := make([]string, rows)
	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		keysA[i] = fmt.Sprintf("k%04d", rngA.next()%uint32(groups))
		keysB[i] = fmt.Sprintf("k%04d", rngB.next()%uint32(groups))
		values[i] = int64(rngA.next())
	}
	return keysA, keysB, values
}

func BenchmarkMicroInnerJoinInt64(b *testing.B) {
	microDataOnce.Do(buildMicroData)
	left := microData.innerIntLeft
	right := microData.innerIntRight

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := left.Join(right, "id", golars.InnerJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkMicroLeftJoinInt64(b *testing.B) {
	microDataOnce.Do(buildMicroData)
	left := microData.innerIntLeft
	right := microData.innerIntRight

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := left.Join(right, "id", golars.LeftJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkMicroInnerJoinString(b *testing.B) {
	microDataOnce.Do(buildMicroData)
	left := microData.innerStrLeft
	right := microData.innerStrRight

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := left.Join(right, "id", golars.InnerJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkMicroLeftJoinString(b *testing.B) {
	microDataOnce.Do(buildMicroData)
	left := microData.innerStrLeft
	right := microData.innerStrRight

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := left.Join(right, "id", golars.LeftJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkMicroInnerJoinStringTwoCol(b *testing.B) {
	microDataOnce.Do(buildMicroData)
	left := microData.innerStr2Left
	right := microData.innerStr2Right

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := left.JoinOn(right, []string{"id1", "id2"}, []string{"id1", "id2"}, golars.InnerJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}
