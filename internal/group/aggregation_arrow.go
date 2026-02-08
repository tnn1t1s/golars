package group

import (
	_ "fmt"
	_ "github.com/apache/arrow-go/v18/arrow"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/tnn1t1s/golars/expr"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (gb *GroupBy) tryAggArrow(aggregations map[string]expr.Expr) (*AggResult, bool, error) {
	panic("not implemented")

}

func (gb *GroupBy) tryCountArrow() (*AggResult, bool, error) {
	panic("not implemented")

}

func arrowAggOp(op expr.AggOp) (arrowcompute.GroupByAggOp, bool) {
	panic("not implemented")

}

func isArrowGroupByKeySupported(col series.Series) bool {
	panic("not implemented")

}

func isArrowGroupByValueSupported(col series.Series, op expr.AggOp) bool {
	panic("not implemented")

}
