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

// BenchmarkGroupByQ7 - Group by with range calculation (max - min)
// Polars: df.group_by("id3").agg((pl.max("v1") - pl.min("v2")).alias("range_v1_v2"))
func BenchmarkGroupByQ7_Small(b *testing.B) {
	benchmarkGroupByQ7(b, testData.small)
}

func BenchmarkGroupByQ7_Medium(b *testing.B) {
	benchmarkGroupByQ7(b, testData.medium)
}

func benchmarkGroupByQ7(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groupBy, err := df.GroupBy("id3")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v1_max": expr.Col("v1").Max(), // Simplified - Sub not available on AggExpr
			"v2_min": expr.Col("v2").Min(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByQ8 - Group by with top-k
// Polars: df.drop_nulls("v3").group_by("id6").agg(pl.col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3")
func BenchmarkGroupByQ8_Small(b *testing.B) {
	benchmarkGroupByQ8(b, testData.small)
}

func BenchmarkGroupByQ8_Medium(b *testing.B) {
	benchmarkGroupByQ8(b, testData.medium)
}

func benchmarkGroupByQ8(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Note: This is simplified - actual implementation would need:
		// 1. Drop nulls from v3
		// 2. Group by id6
		// 3. Get top 2 values of v3 per group
		// 4. Explode the result
		// For now, we'll do a simpler version
		dfNoNulls := df // TODO: Implement DropNulls when available
		groupBy, err := dfNoNulls.GroupBy("id6")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"v3_max": expr.Col("v3").Max(), // Simplified version
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkGroupByQ9 - Group by with correlation
// Polars: df.group_by("id2", "id4").agg((pl.corr("v1", "v2") ** 2).alias("r2"))
func BenchmarkGroupByQ9_Small(b *testing.B) {
	benchmarkGroupByQ9(b, testData.small)
}

func BenchmarkGroupByQ9_Medium(b *testing.B) {
	benchmarkGroupByQ9(b, testData.medium)
}

func benchmarkGroupByQ9(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Note: If Corr is not implemented, this is a placeholder
		// In reality, we'd compute correlation between v1 and v2, then square it
		groupBy, err := df.GroupBy("id2", "id4")
		if err != nil {
			b.Fatal(err)
		}
		result, err := groupBy.Agg(map[string]expr.Expr{
			"r2": expr.Col("v1").Mean(), // Placeholder - should be corr(v1,v2)^2
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

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
