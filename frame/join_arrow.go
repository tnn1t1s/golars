package frame

import (
	_ "fmt"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/compute"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func arrowJoinIndices(leftCols, rightCols []series.Series, joinType JoinType) ([]int, []int, error) {
	panic("not implemented")

}

func arrowJoinIndicesSingle(leftCol, rightCol series.Series, joinType JoinType) ([]int, []int, error) {
	panic("not implemented")

}

func arrowJoinIndicesMulti(leftCols, rightCols []series.Series, joinType JoinType) ([]int, []int, error) {
	panic("not implemented")

}

func arrowIndexArrayToInts(arr arrow.Array, allowNulls bool) ([]int, error) {
	panic("not implemented")

}

func isArrowJoinTypeSupported(col series.Series) bool {
	panic("not implemented")

}
