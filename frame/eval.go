package frame

import (
	"fmt"
	"math"
	"strings"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	_ "github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/internal/window"
	"github.com/tnn1t1s/golars/series"
)

func (df *DataFrame) evaluateExpr(e expr.Expr) (series.Series, error) {
	switch ex := e.(type) {
	case *expr.ColumnExpr:
		return df.Column(ex.Name())
	case *expr.LiteralExpr:
		return df.createLiteralSeries(ex.Value())
	case *window.Expr:
		return df.evaluateWindowExpr(ex)
	case *expr.BinaryExpr:
		return df.evaluateBinaryOpExpr(ex)
	case *expr.UnaryExpr:
		return df.evaluateUnaryOpExpr(ex)
	case *expr.AliasExpr:
		result, err := df.evaluateExpr(ex.Expr())
		if err != nil {
			return nil, err
		}
		return result.Rename(ex.Name()), nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

func (df *DataFrame) evaluateWindowExpr(we *window.Expr) (series.Series, error) {
	return nil, fmt.Errorf("window expressions not yet supported")
}

type partitionResult struct {
	indices []int
	series  series.Series
}

func (df *DataFrame) mergePartitionResults(results []partitionResult, name string) (series.Series, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("no partition results to merge")
	}
	if len(results) == 1 {
		return results[0].series.Rename(name), nil
	}
	return df.mergeFloat64Results(results, name), nil
}

func (df *DataFrame) mergeInt32Results(results []partitionResult, name string) series.Series {
	values := make([]int32, df.height)
	for _, pr := range results {
		for j, idx := range pr.indices {
			values[idx] = pr.series.Get(j).(int32)
		}
	}
	return series.NewInt32Series(name, values)
}

func (df *DataFrame) mergeInt64Results(results []partitionResult, name string) series.Series {
	values := make([]int64, df.height)
	for _, pr := range results {
		for j, idx := range pr.indices {
			values[idx] = pr.series.Get(j).(int64)
		}
	}
	return series.NewInt64Series(name, values)
}

func (df *DataFrame) mergeFloat64Results(results []partitionResult, name string) series.Series {
	values := make([]float64, df.height)
	for _, pr := range results {
		for j, idx := range pr.indices {
			values[idx] = pr.series.Get(j).(float64)
		}
	}
	return series.NewFloat64Series(name, values)
}

func (df *DataFrame) mergeStringResults(results []partitionResult, name string) series.Series {
	values := make([]string, df.height)
	for _, pr := range results {
		for j, idx := range pr.indices {
			values[idx] = pr.series.Get(j).(string)
		}
	}
	return series.NewStringSeries(name, values)
}

func (df *DataFrame) createPartitions(spec *window.Spec) ([]window.Partition, error) {
	seriesMap := make(map[string]series.Series)
	for _, col := range df.columns {
		seriesMap[col.Name()] = col
	}

	partBy := spec.GetPartitionBy()
	if len(partBy) == 0 {
		indices := make([]int, df.height)
		for i := range indices {
			indices[i] = i
		}
		return []window.Partition{window.NewPartition(seriesMap, indices)}, nil
	}

	groups, err := df.partitionByColumns(partBy)
	if err != nil {
		return nil, err
	}

	partitions := make([]window.Partition, 0, len(groups))
	for _, indices := range groups {
		partitions = append(partitions, window.NewPartition(seriesMap, indices))
	}
	return partitions, nil
}

func (df *DataFrame) partitionByColumns(columns []string) (map[string][]int, error) {
	cols := make([]series.Series, len(columns))
	for i, name := range columns {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}

	groups := make(map[string][]int)
	for row := 0; row < df.height; row++ {
		var keyParts []string
		for _, col := range cols {
			keyParts = append(keyParts, col.GetAsString(row))
		}
		key := strings.Join(keyParts, "\x00")
		groups[key] = append(groups[key], row)
	}
	return groups, nil
}

func (df *DataFrame) createLiteralSeries(value interface{}) (series.Series, error) {
	n := df.height
	if n == 0 {
		n = 1
	}
	switch v := value.(type) {
	case bool:
		vals := make([]bool, n)
		for i := range vals {
			vals[i] = v
		}
		return series.NewBooleanSeries("literal", vals), nil
	case int:
		vals := make([]int64, n)
		for i := range vals {
			vals[i] = int64(v)
		}
		return series.NewInt64Series("literal", vals), nil
	case int32:
		vals := make([]int32, n)
		for i := range vals {
			vals[i] = v
		}
		return series.NewInt32Series("literal", vals), nil
	case int64:
		vals := make([]int64, n)
		for i := range vals {
			vals[i] = v
		}
		return series.NewInt64Series("literal", vals), nil
	case float64:
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = v
		}
		return series.NewFloat64Series("literal", vals), nil
	case float32:
		vals := make([]float32, n)
		for i := range vals {
			vals[i] = v
		}
		return series.NewFloat32Series("literal", vals), nil
	case string:
		vals := make([]string, n)
		for i := range vals {
			vals[i] = v
		}
		return series.NewStringSeries("literal", vals), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", value)
	}
}

func (df *DataFrame) evaluateBinaryOpExpr(e *expr.BinaryExpr) (series.Series, error) {
	left, err := df.evaluateExpr(e.Left())
	if err != nil {
		return nil, err
	}
	right, err := df.evaluateExpr(e.Right())
	if err != nil {
		return nil, err
	}
	op := e.Op()
	n := left.Len()

	switch op {
	case expr.OpEqual, expr.OpNotEqual, expr.OpLess, expr.OpLessEqual, expr.OpGreater, expr.OpGreaterEqual:
		results := make([]bool, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				continue
			}
			validity[i] = true
			results[i] = compareValues(left.Get(i), right.Get(i), op)
		}
		return series.NewSeriesWithValidity("result", results, validity, datatypes.Boolean{}), nil
	case expr.OpAnd:
		results := make([]bool, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				continue
			}
			validity[i] = true
			lv, lok := left.Get(i).(bool)
			rv, rok := right.Get(i).(bool)
			if lok && rok {
				results[i] = lv && rv
			}
		}
		return series.NewSeriesWithValidity("result", results, validity, datatypes.Boolean{}), nil
	case expr.OpOr:
		results := make([]bool, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				continue
			}
			validity[i] = true
			lv, lok := left.Get(i).(bool)
			rv, rok := right.Get(i).(bool)
			if lok && rok {
				results[i] = lv || rv
			}
		}
		return series.NewSeriesWithValidity("result", results, validity, datatypes.Boolean{}), nil
	case expr.OpAdd, expr.OpSubtract, expr.OpMultiply, expr.OpDivide, expr.OpModulo:
		results := make([]float64, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if left.IsNull(i) || right.IsNull(i) {
				continue
			}
			validity[i] = true
			lv := toFloat64Value(left.Get(i))
			rv := toFloat64Value(right.Get(i))
			switch op {
			case expr.OpAdd:
				results[i] = lv + rv
			case expr.OpSubtract:
				results[i] = lv - rv
			case expr.OpMultiply:
				results[i] = lv * rv
			case expr.OpDivide:
				if rv == 0 {
					results[i] = math.NaN()
				} else {
					results[i] = lv / rv
				}
			case expr.OpModulo:
				results[i] = math.Mod(lv, rv)
			}
		}
		return series.NewSeriesWithValidity("result", results, validity, datatypes.Float64{}), nil
	default:
		return nil, fmt.Errorf("unsupported binary operation: %d", op)
	}
}

func (df *DataFrame) evaluateUnaryOpExpr(e *expr.UnaryExpr) (series.Series, error) {
	inner, err := df.evaluateExpr(e.Expr())
	if err != nil {
		return nil, err
	}
	op := e.Op()
	n := inner.Len()

	switch op {
	case expr.OpNot:
		results := make([]bool, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if inner.IsNull(i) {
				continue
			}
			validity[i] = true
			if bv, ok := inner.Get(i).(bool); ok {
				results[i] = !bv
			}
		}
		return series.NewSeriesWithValidity("result", results, validity, datatypes.Boolean{}), nil
	case expr.OpIsNull:
		results := make([]bool, n)
		for i := 0; i < n; i++ {
			results[i] = inner.IsNull(i)
		}
		return series.NewBooleanSeries("result", results), nil
	case expr.OpIsNotNull:
		results := make([]bool, n)
		for i := 0; i < n; i++ {
			results[i] = !inner.IsNull(i)
		}
		return series.NewBooleanSeries("result", results), nil
	case expr.OpNegate:
		results := make([]float64, n)
		validity := make([]bool, n)
		for i := 0; i < n; i++ {
			if inner.IsNull(i) {
				continue
			}
			validity[i] = true
			results[i] = -toFloat64Value(inner.Get(i))
		}
		return series.NewSeriesWithValidity("result", results, validity, datatypes.Float64{}), nil
	default:
		return nil, fmt.Errorf("unsupported unary operation: %d", op)
	}
}
