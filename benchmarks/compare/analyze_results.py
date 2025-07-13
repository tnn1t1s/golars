#!/usr/bin/env python3
"""Analyze benchmark results comparing golars vs Polars"""

import sys
import re

def parse_golars_results(filename):
    """Parse golars benchmark results"""
    results = {}
    with open(filename, 'r') as f:
        for line in f:
            # Look for lines like: BenchmarkGroupByQ1_Small-8   	       3	   2570959 ns/op
            match = re.match(r'BenchmarkGroupBy(Q\d+)_\w+-\d+\s+\d+\s+(\d+)\s+ns/op', line)
            if match:
                query = match.group(1)
                ns_per_op = int(match.group(2))
                ms_per_op = ns_per_op / 1_000_000
                results[query] = ms_per_op
    return results

def parse_polars_results(filename):
    """Parse Polars benchmark results"""
    results = {}
    with open(filename, 'r') as f:
        content = f.read()
        # Look for summary section
        if "Summary:" in content:
            summary_section = content.split("Summary:")[1]
            for line in summary_section.strip().split('\n'):
                if line.strip() == "-" * 40:
                    continue
                # Look for lines like: Q1: 2.45ms
                match = re.match(r'(Q\d+):\s+([\d.]+)ms', line)
                if match:
                    query = match.group(1)
                    ms = float(match.group(2))
                    results[query] = ms
    return results

def main():
    size = sys.argv[1] if len(sys.argv) > 1 else "light"
    
    # Map size names
    size_map = {
        "light": "light",
        "small": "light",
        "full": "full", 
        "medium": "full",
        "heavy": "heavy",
        "large": "heavy"
    }
    
    mapped_size = size_map.get(size, size)
    
    # Parse results
    golars_file = f"results/golars_{mapped_size}.txt"
    polars_file = f"results/polars_{mapped_size}.txt"
    
    try:
        golars_results = parse_golars_results(golars_file)
        polars_results = parse_polars_results(polars_file)
    except FileNotFoundError as e:
        print(f"Error: Could not find results file: {e}")
        return
    
    if not golars_results or not polars_results:
        print("Error: No results found in one or both files")
        return
    
    # Compare results
    print(f"\nH2O.ai Benchmark Comparison ({size} dataset)")
    print("=" * 70)
    print(f"{'Query':<10} {'Golars (ms)':<15} {'Polars (ms)':<15} {'Ratio':<15} {'Status'}")
    print("-" * 70)
    
    total_golars = 0
    total_polars = 0
    
    for query in sorted(set(golars_results.keys()) & set(polars_results.keys())):
        golars_ms = golars_results[query]
        polars_ms = polars_results[query]
        ratio = golars_ms / polars_ms if polars_ms > 0 else 0
        
        # Determine status
        if ratio < 2:
            status = "🟢 Good"
        elif ratio < 5:
            status = "🟡 OK"
        else:
            status = "🔴 Slow"
        
        print(f"{query:<10} {golars_ms:<15.2f} {polars_ms:<15.2f} {ratio:<15.2f}x {status}")
        
        total_golars += golars_ms
        total_polars += polars_ms
    
    print("-" * 70)
    
    if total_polars > 0:
        total_ratio = total_golars / total_polars
        print(f"{'Total':<10} {total_golars:<15.2f} {total_polars:<15.2f} {total_ratio:<15.2f}x")
    
    print("\nNotes:")
    print("- Ratio shows golars time / polars time (lower is better)")
    print("- 🟢 Good: < 2x slower | 🟡 OK: 2-5x slower | 🔴 Slow: > 5x slower")
    print("- Polars is written in Rust and highly optimized")
    print("- Golars is written in Go and under active development")

if __name__ == "__main__":
    main()