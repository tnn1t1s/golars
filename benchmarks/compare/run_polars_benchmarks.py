#!/usr/bin/env python3
"""
Run Polars benchmarks using the local Polars installation.
This script runs the benchmarks directly without pytest to have more control.
"""

import sys
import time
import os
import polars as pl

def load_data(size):
    """Load H2O.ai dataset from golars-generated parquet file"""
    data_path = f"../data/h2oai_{size}.parquet"
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}. Run 'just generate-{size}-data' first.")
    return pl.read_parquet(data_path)

def run_benchmark(name, func, df, iterations=3):
    """Run a benchmark function multiple times and return average time"""
    # Warmup
    func(df)
    
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func(df)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    avg_time = sum(times) / len(times)
    return avg_time, result

# H2O.ai benchmark queries
def q1(df):
    return (
        df.lazy()
        .group_by("id1")
        .agg(pl.sum("v1").alias("v1_sum"))
        .collect()
    )

def q2(df):
    return (
        df.lazy()
        .group_by("id1", "id2")
        .agg(pl.sum("v1").alias("v1_sum"))
        .collect()
    )

def q3(df):
    return (
        df.lazy()
        .group_by("id3")
        .agg([
            pl.sum("v1").alias("v1_sum"),
            pl.mean("v3").alias("v3_mean"),
        ])
        .collect()
    )

def q4(df):
    return (
        df.lazy()
        .group_by("id4")
        .agg([
            pl.mean("v1").alias("v1_mean"),
            pl.mean("v2").alias("v2_mean"),
            pl.mean("v3").alias("v3_mean"),
        ])
        .collect()
    )

def q5(df):
    return (
        df.lazy()
        .group_by("id6")
        .agg([
            pl.sum("v1").alias("v1_sum"),
            pl.sum("v2").alias("v2_sum"),
            pl.sum("v3").alias("v3_sum"),
        ])
        .collect()
    )

def q6(df):
    return (
        df.lazy()
        .group_by("id4", "id5")
        .agg([
            pl.median("v3").alias("v3_median"),
            pl.std("v3").alias("v3_std"),
        ])
        .collect()
    )

def q7(df):
    return (
        df.lazy()
        .group_by("id3")
        .agg((pl.max("v1") - pl.min("v2")).alias("range_v1_v2"))
        .collect()
    )

def q8(df):
    return (
        df.drop_nulls("v3")
        .lazy()
        .group_by("id6")
        .agg(pl.col("v3").top_k(2).alias("largest2_v3"))
        .explode("largest2_v3")
        .collect()
    )

def q9(df):
    return (
        df.lazy()
        .group_by("id2", "id4")
        .agg((pl.corr("v1", "v2") ** 2).alias("r2"))
        .collect()
    )

def q10(df):
    return (
        df.lazy()
        .group_by("id1", "id2", "id3", "id4", "id5", "id6")
        .agg([
            pl.sum("v3").alias("v3_sum"),
            pl.count("v1").alias("v1_count"),
        ])
        .collect()
    )

def main():
    size = sys.argv[1] if len(sys.argv) > 1 else "small"
    queries = sys.argv[2] if len(sys.argv) > 2 else "1-6"
    
    # Parse query range
    if "-" in queries:
        start, end = map(int, queries.split("-"))
        query_nums = range(start, end + 1)
    else:
        query_nums = [int(queries)]
    
    # Load data
    print(f"Loading {size} dataset...")
    df = load_data(size)
    print(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
    print(f"Memory usage: {df.estimated_size('mb'):.2f} MB")
    print()
    
    # Define benchmarks
    benchmarks = {
        1: ("Q1: Simple sum", q1),
        2: ("Q2: Sum with 2 group-by", q2),
        3: ("Q3: Sum and mean", q3),
        4: ("Q4: Multiple means", q4),
        5: ("Q5: Multiple sums", q5),
        6: ("Q6: Median and std", q6),
        7: ("Q7: Range calculation", q7),
        8: ("Q8: Top-k selection", q8),
        9: ("Q9: Correlation", q9),
        10: ("Q10: All groups aggregation", q10),
    }
    
    # Run selected benchmarks
    print("Running benchmarks...")
    results = []
    
    for num in query_nums:
        if num not in benchmarks:
            continue
            
        name, func = benchmarks[num]
        try:
            elapsed, result = run_benchmark(f"Q{num}", func, df)
            print(f"{name}: {elapsed*1000:.2f}ms ({len(result):,} rows)")
            results.append((f"Q{num}", elapsed))
        except Exception as e:
            print(f"{name}: ERROR - {e}")
            results.append((f"Q{num}", None))
    
    # Print summary
    print("\nSummary:")
    print("-" * 40)
    for query, elapsed in results:
        if elapsed is not None:
            print(f"{query}: {elapsed*1000:.2f}ms")
        else:
            print(f"{query}: ERROR")

if __name__ == "__main__":
    main()