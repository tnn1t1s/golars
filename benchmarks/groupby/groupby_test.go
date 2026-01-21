// Package groupby contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_group_by.py
//
// These tests are based on the H2O.ai database benchmark.
// See: https://h2oai.github.io/db-benchmark/
//
// Data configuration matches Polars conftest.py:
//
//	groupby_data = generate_group_by_data(10_000, 100, null_ratio=0.05)
package groupby

import (
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// testData matches Polars' groupby_data fixture from conftest.py
// Default: 10,000 rows, 100 groups, 5% null ratio
var testData *frame.DataFrame

func init() {
	var err error
	testData, err = data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}
}

// =============================================================================
// Polars test_group_by.py - Exact Translations
// =============================================================================

// BenchmarkGroupByH2OAI_Q1 matches test_groupby_h2oai_q1
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id1")
//	.agg(pl.sum("v1").alias("v1_sum"))
//	.collect()
func BenchmarkGroupByH2OAI_Q1(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id1")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v1_sum": expr.Col("v1").Sum(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q2 matches test_groupby_h2oai_q2
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id1", "id2")
//	.agg(pl.sum("v1").alias("v1_sum"))
//	.collect()
func BenchmarkGroupByH2OAI_Q2(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id1", "id2")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v1_sum": expr.Col("v1").Sum(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q3 matches test_groupby_h2oai_q3
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id3")
//	.agg(pl.sum("v1").alias("v1_sum"), pl.mean("v3").alias("v3_mean"))
//	.collect()
func BenchmarkGroupByH2OAI_Q3(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id3")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v1_sum":  expr.Col("v1").Sum(),
			"v3_mean": expr.Col("v3").Mean(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q4 matches test_groupby_h2oai_q4
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id4")
//	.agg(pl.mean("v1").alias("v1_mean"), pl.mean("v2").alias("v2_mean"), pl.mean("v3").alias("v3_mean"))
//	.collect()
func BenchmarkGroupByH2OAI_Q4(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id4")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v1_mean": expr.Col("v1").Mean(),
			"v2_mean": expr.Col("v2").Mean(),
			"v3_mean": expr.Col("v3").Mean(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q5 matches test_groupby_h2oai_q5
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id6")
//	.agg(pl.sum("v1").alias("v1_sum"), pl.sum("v2").alias("v2_sum"), pl.sum("v3").alias("v3_sum"))
//	.collect()
func BenchmarkGroupByH2OAI_Q5(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id6")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v1_sum": expr.Col("v1").Sum(),
			"v2_sum": expr.Col("v2").Sum(),
			"v3_sum": expr.Col("v3").Sum(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q6 matches test_groupby_h2oai_q6
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id4", "id5")
//	.agg(pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std"))
//	.collect()
func BenchmarkGroupByH2OAI_Q6(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id4", "id5")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v3_median": expr.Col("v3").Median(),
			"v3_std":    expr.Col("v3").Std(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q7 matches test_groupby_h2oai_q7
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id3")
//	.agg((pl.max("v1") - pl.min("v2")).alias("range_v1_v2"))
//	.collect()
func BenchmarkGroupByH2OAI_Q7(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id3")
		if err != nil {
			b.Fatal(err)
		}
		// Expression arithmetic: max(v1) - min(v2)
		result, err := groupBy.Agg(map[string]expr.Expr{
			"range_v1_v2": expr.Col("v1").Max().Sub(expr.Col("v2").Min()),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q8 matches test_groupby_h2oai_q8
// Polars:
//
//	groupby_data.lazy()
//	.drop_nulls("v3")
//	.group_by("id6")
//	.agg(pl.col("v3").top_k(2).alias("largest2_v3"))
//	.explode("largest2_v3")
//	.collect()
func BenchmarkGroupByH2OAI_Q8(b *testing.B) {
	b.Skip("Explode not implemented yet")
	// TODO: Implement when Explode is available
	// filtered, _ := testData.Filter(expr.Col("v3").IsNotNull())
	// groupBy, _ := filtered.GroupBy("id6")
	// aggDF, _ := groupBy.Agg(map[string]expr.Expr{"largest2_v3": expr.Col("v3").TopK(2)})
	// result, _ := aggDF.Explode("largest2_v3")
}

// BenchmarkGroupByH2OAI_Q9 matches test_groupby_h2oai_q9
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id2", "id4")
//	.agg((pl.corr("v1", "v2") ** 2).alias("r2"))
//	.collect()
//
// Note: We compute correlation but don't square it since golars doesn't
// support ** operator on scalar results yet. The benchmark is still meaningful
// as correlation is the computationally expensive part.
func BenchmarkGroupByH2OAI_Q9(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id2", "id4")
		if err != nil {
			b.Fatal(err)
		}
		// Correlation aggregation (corr squared would require additional op)
		result, err := groupBy.Agg(map[string]expr.Expr{
			"r2": expr.Corr("v1", "v2"),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByH2OAI_Q10 matches test_groupby_h2oai_q10
// Polars:
//
//	groupby_data.lazy()
//	.group_by("id1", "id2", "id3", "id4", "id5", "id6")
//	.agg(pl.sum("v3").alias("v3_sum"), pl.count("v1").alias("v1_count"))
//	.collect()
func BenchmarkGroupByH2OAI_Q10(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := testData.GroupBy("id1", "id2", "id3", "id4", "id5", "id6")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v3_sum":   expr.Col("v3").Sum(),
			"v1_count": expr.Col("v1").Count(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}
