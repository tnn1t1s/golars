package filter

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

const (
	pocRows  = 1_000_000
	pocMatch = int64(42)
)

var (
	pocOnce   sync.Once
	pocDF     *frame.DataFrame
	pocArrow  arrow.Array
	pocDatum  *compute.ArrayDatum
	pocScalar *compute.ScalarDatum
	pocErr    error
)

func loadPOCData(b *testing.B) (*frame.DataFrame, arrow.Array, *compute.ArrayDatum, *compute.ScalarDatum) {
	b.Helper()
	pocOnce.Do(func() {
		values := make([]int64, pocRows)
		validity := make([]bool, pocRows)
		for i := 0; i < pocRows; i++ {
			values[i] = int64(i % 1024)
			validity[i] = i%20 != 0
		}

		col := series.NewSeriesWithValidity("x", values, validity, datatypes.Int64{})
		pocDF, pocErr = frame.NewDataFrame(col)
		if pocErr != nil {
			return
		}

		mem := memory.NewGoAllocator()
		builder := array.NewInt64Builder(mem)
		builder.Reserve(pocRows)
		for i, v := range values {
			if validity[i] {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		}
		pocArrow = builder.NewArray()
		builder.Release()

		pocDatum = &compute.ArrayDatum{Value: pocArrow.Data()}
		pocScalar = &compute.ScalarDatum{Value: scalar.NewInt64Scalar(pocMatch)}
	})
	if pocErr != nil {
		b.Fatal(pocErr)
	}
	return pocDF, pocArrow, pocDatum, pocScalar
}

func BenchmarkFilterPOCGolarsInt64Eq(b *testing.B) {
	df, _, _, _ := loadPOCData(b)
	filterExpr := expr.Col("x").Eq(expr.Lit(pocMatch))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := df.Filter(filterExpr)
		if err != nil {
			b.Fatal(err)
		}
		_ = out.Height()
	}
}

func BenchmarkFilterPOCArrowComputeInt64Eq(b *testing.B) {
	_, _, datum, scalarDatum := loadPOCData(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mask, err := compute.CallFunction(ctx, "equal", nil, datum, scalarDatum)
		if err != nil {
			b.Fatal(err)
		}
		out, err := compute.Filter(ctx, datum, mask, *compute.DefaultFilterOptions())
		if err != nil {
			mask.Release()
			b.Fatal(err)
		}
		mask.Release()
		out.Release()
	}
}
