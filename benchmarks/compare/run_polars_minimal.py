#!/usr/bin/env python3
"""Run minimal Polars benchmarks (Q1-Q6 only)"""

import sys
import time
import polars as pl

def load_data(size):
    """Load H2O.ai dataset"""
    return pl.read_parquet(f"../data/h2oai_{size}.parquet")

def benchmark_q1(df):
    """Q1: Simple sum aggregation"""
    start = time.time()
    result = df.group_by("id1").agg(pl.sum("v1").alias("v1_sum"))
    elapsed = time.time() - start
    return elapsed, len(result)

def benchmark_q2(df):
    """Q2: Group by two columns with sum"""
    start = time.time()
    result = df.group_by("id1", "id2").agg(pl.sum("v1").alias("v1_sum"))
    elapsed = time.time() - start
    return elapsed, len(result)

def benchmark_q3(df):
    """Q3: Sum and mean"""
    start = time.time()
    result = df.group_by("id3").agg([
        pl.sum("v1").alias("v1_sum"),
        pl.mean("v3").alias("v3_mean")
    ])
    elapsed = time.time() - start
    return elapsed, len(result)

def benchmark_q4(df):
    """Q4: Multiple means"""
    start = time.time()
    result = df.group_by("id4").agg([
        pl.mean("v1").alias("v1_mean"),
        pl.mean("v2").alias("v2_mean"),
        pl.mean("v3").alias("v3_mean")
    ])
    elapsed = time.time() - start
    return elapsed, len(result)

def benchmark_q5(df):
    """Q5: Multiple sums"""
    start = time.time()
    result = df.group_by("id6").agg([
        pl.sum("v1").alias("v1_sum"),
        pl.sum("v2").alias("v2_sum"),
        pl.sum("v3").alias("v3_sum")
    ])
    elapsed = time.time() - start
    return elapsed, len(result)

def benchmark_q6(df):
    """Q6: Median and std"""
    start = time.time()
    result = df.group_by("id4", "id5").agg([
        pl.median("v3").alias("v3_median"),
        pl.std("v3").alias("v3_std")
    ])
    elapsed = time.time() - start
    return elapsed, len(result)

def main():
    size = sys.argv[1] if len(sys.argv) > 1 else "small"
    
    # Load data
    print(f"Loading {size} dataset...")
    df = load_data(size)
    print(f"Loaded {len(df):,} rows")
    
    # Run benchmarks
    benchmarks = [
        ("Q1", benchmark_q1),
        ("Q2", benchmark_q2),
        ("Q3", benchmark_q3),
        ("Q4", benchmark_q4),
        ("Q5", benchmark_q5),
        ("Q6", benchmark_q6),
    ]
    
    # Warmup
    print("Warming up...")
    for name, func in benchmarks:
        func(df)
    
    # Run benchmarks
    print("\nRunning benchmarks...")
    results = []
    
    for name, func in benchmarks:
        times = []
        for i in range(3):  # Run 3 times
            elapsed, count = func(df)
            times.append(elapsed)
            print(f"{name}: {elapsed:.6f}s ({count:,} groups)")
        
        avg_time = sum(times) / len(times)
        results.append((name, avg_time))
        print(f"{name} average: {avg_time:.6f}s")
    
    # Print summary
    print("\nSummary:")
    for name, avg_time in results:
        print(f"{name}: {avg_time*1000:.2f}ms")

if __name__ == "__main__":
    main()