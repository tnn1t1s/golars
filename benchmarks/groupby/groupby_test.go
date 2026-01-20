package groupby

import (
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// Global variable to store test data
var testData struct {
	small  *frame.DataFrame
	medium *frame.DataFrame
	large  *frame.DataFrame
}

// init loads the test data once
func init() {
	// Load small dataset
	small, err := data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}
	testData.small = small

	// Load medium dataset
	medium, err := data.GenerateH2OAIData(data.H2OAIMedium)
	if err != nil {
		panic(err)
	}
	testData.medium = medium

	// Note: Large dataset is commented out by default to avoid memory issues
	// Uncomment when needed for large-scale benchmarks
	// large, err := data.GenerateH2OAIData(data.H2OAILarge)
	// if err != nil {
	//     panic(err)
	// }
	// testData.large = large
}

// BenchmarkGroupByQ1 - Simple group by single column with sum
// Polars: df.group_by("id1").agg(pl.sum("v1").alias("v1_sum"))
func BenchmarkGroupByQ1_Small(b *testing.B) {
	benchmarkGroupByQ1(b, testData.small)
}

func BenchmarkGroupByQ1_Medium(b *testing.B) {
	benchmarkGroupByQ1(b, testData.medium)
}

func benchmarkGroupByQ1(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id1")
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

// BenchmarkGroupByQ2 - Group by two columns with sum
// Polars: df.group_by("id1", "id2").agg(pl.sum("v1").alias("v1_sum"))
func BenchmarkGroupByQ2_Small(b *testing.B) {
	benchmarkGroupByQ2(b, testData.small)
}

func BenchmarkGroupByQ2_Medium(b *testing.B) {
	benchmarkGroupByQ2(b, testData.medium)
}

func benchmarkGroupByQ2(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id1", "id2")
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

// BenchmarkGroupByQ3 - Group by with sum and mean
// Polars: df.group_by("id3").agg([pl.sum("v1").alias("v1_sum"), pl.mean("v3").alias("v3_mean")])
func BenchmarkGroupByQ3_Small(b *testing.B) {
	benchmarkGroupByQ3(b, testData.small)
}

func BenchmarkGroupByQ3_Medium(b *testing.B) {
	benchmarkGroupByQ3(b, testData.medium)
}

func benchmarkGroupByQ3(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id3")
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

// BenchmarkGroupByQ4 - Group by with multiple means
// Polars: df.group_by("id4").agg([pl.mean("v1").alias("v1_mean"), pl.mean("v2").alias("v2_mean"), pl.mean("v3").alias("v3_mean")])
func BenchmarkGroupByQ4_Small(b *testing.B) {
	benchmarkGroupByQ4(b, testData.small)
}

func BenchmarkGroupByQ4_Medium(b *testing.B) {
	benchmarkGroupByQ4(b, testData.medium)
}

func benchmarkGroupByQ4(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id4")
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

// BenchmarkGroupByQ5 - Group by with multiple sums
// Polars: df.group_by("id6").agg([pl.sum("v1").alias("v1_sum"), pl.sum("v2").alias("v2_sum"), pl.sum("v3").alias("v3_sum")])
func BenchmarkGroupByQ5_Small(b *testing.B) {
	benchmarkGroupByQ5(b, testData.small)
}

func BenchmarkGroupByQ5_Medium(b *testing.B) {
	benchmarkGroupByQ5(b, testData.medium)
}

func benchmarkGroupByQ5(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id6")
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

// BenchmarkGroupByQ6 - Group by two columns with median and std
// Polars: df.group_by("id4", "id5").agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")])
func BenchmarkGroupByQ6_Small(b *testing.B) {
	benchmarkGroupByQ6(b, testData.small)
}

func BenchmarkGroupByQ6_Medium(b *testing.B) {
	benchmarkGroupByQ6(b, testData.medium)
}

func benchmarkGroupByQ6(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id4", "id5")
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

// =============================================================================
// Q7, Q8, Q9 are SKIPPED - NOT COMPARABLE with Polars
// =============================================================================
// These benchmarks require operations not yet implemented in golars:
// - Q7: Expression arithmetic in aggregations (max - min)
// - Q8: top_k, drop_nulls, explode operations
// - Q9: correlation function
//
// Including simplified versions would produce misleading benchmark comparisons.
// See: https://github.com/tnn1t1s/golars/issues/1 (Advanced Data Types)
// =============================================================================

// BenchmarkGroupByQ10 - Group by all ID columns with sum and count
// Polars: df.group_by("id1", "id2", "id3", "id4", "id5", "id6").agg([pl.sum("v3").alias("v3_sum"), pl.count("v1").alias("v1_count")])
func BenchmarkGroupByQ10_Small(b *testing.B) {
	benchmarkGroupByQ10(b, testData.small)
}

func BenchmarkGroupByQ10_Medium(b *testing.B) {
	benchmarkGroupByQ10(b, testData.medium)
}

func benchmarkGroupByQ10(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id1", "id2", "id3", "id4", "id5", "id6")
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
