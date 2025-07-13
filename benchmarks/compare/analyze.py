#!/usr/bin/env python3
"""
Analyze and compare benchmark results between golars and Polars.
"""

import json
import re
import argparse
from pathlib import Path
from typing import Dict, List, Tuple


def parse_polars_results(json_path: Path) -> Dict[str, float]:
    """Parse pytest-benchmark JSON output."""
    with open(json_path) as f:
        data = json.load(f)
    
    results = {}
    for benchmark in data.get('benchmarks', []):
        name = benchmark['name']
        # Extract query number from test name (e.g., test_groupby_h2oai_q1 -> q1)
        match = re.search(r'test_groupby_h2oai_(q\d+)', name)
        if match:
            query = match.group(1)
            # Use median time in seconds
            results[query] = benchmark['stats']['median']
    
    return results


def parse_golars_results(txt_path: Path) -> Dict[str, Tuple[float, int]]:
    """Parse Go benchmark output."""
    results = {}
    
    with open(txt_path) as f:
        for line in f:
            # Parse lines like: BenchmarkGroupByQ1_Medium-8    1234    987654 ns/op    12345 B/op    67 allocs/op
            match = re.search(r'BenchmarkGroupBy(Q\d+)_\w+.*?\s+\d+\s+(\d+)\s+ns/op\s+(\d+)\s+B/op', line)
            if match:
                query = match.group(1).lower()
                time_ns = int(match.group(2))
                memory_bytes = int(match.group(3))
                # Convert nanoseconds to seconds
                results[query] = (time_ns / 1e9, memory_bytes)
    
    return results


def compare_results(polars_results: Dict[str, float], golars_results: Dict[str, Tuple[float, int]]):
    """Compare and display results."""
    print("\nBenchmark Comparison: golars vs Polars")
    print("=" * 80)
    print(f"{'Query':<10} {'Polars (s)':<15} {'golars (s)':<15} {'Ratio':<10} {'Memory (MB)':<12}")
    print("-" * 80)
    
    total_polars = 0
    total_golars = 0
    
    for query in sorted(set(polars_results.keys()) | set(golars_results.keys())):
        polars_time = polars_results.get(query, 0)
        golars_time, golars_memory = golars_results.get(query, (0, 0))
        
        if polars_time > 0 and golars_time > 0:
            ratio = golars_time / polars_time
            total_polars += polars_time
            total_golars += golars_time
            
            print(f"{query:<10} {polars_time:<15.6f} {golars_time:<15.6f} {ratio:<10.2f} {golars_memory/1024/1024:<12.2f}")
        elif polars_time > 0:
            print(f"{query:<10} {polars_time:<15.6f} {'N/A':<15} {'N/A':<10} {'N/A':<12}")
        elif golars_time > 0:
            print(f"{query:<10} {'N/A':<15} {golars_time:<15.6f} {'N/A':<10} {golars_memory/1024/1024:<12.2f}")
    
    if total_polars > 0 and total_golars > 0:
        print("-" * 80)
        total_ratio = total_golars / total_polars
        print(f"{'TOTAL':<10} {total_polars:<15.6f} {total_golars:<15.6f} {total_ratio:<10.2f}")
    
    print("\nNotes:")
    print("- Ratio: golars time / Polars time (lower is better for golars)")
    print("- Polars times include Python overhead")
    print("- Memory shown is golars memory allocation per operation")


def main():
    parser = argparse.ArgumentParser(description='Compare golars and Polars benchmark results')
    parser.add_argument('--polars', type=Path, help='Path to Polars JSON results')
    parser.add_argument('--golars', type=Path, help='Path to golars text results')
    
    args = parser.parse_args()
    
    if not args.polars and not args.golars:
        # Try to find the most recent results
        results_dir = Path(__file__).parent.parent / 'results'
        polars_files = list(results_dir.glob('polars_*.json'))
        golars_files = list(results_dir.glob('golars_*.txt'))
        
        if polars_files and golars_files:
            args.polars = max(polars_files, key=lambda p: p.stat().st_mtime)
            args.golars = max(golars_files, key=lambda p: p.stat().st_mtime)
            print(f"Using most recent results:")
            print(f"  Polars: {args.polars}")
            print(f"  golars: {args.golars}")
        else:
            print("No results found. Please run benchmarks first.")
            return
    
    polars_results = {}
    golars_results = {}
    
    if args.polars and args.polars.exists():
        polars_results = parse_polars_results(args.polars)
    else:
        print(f"Warning: Polars results not found at {args.polars}")
    
    if args.golars and args.golars.exists():
        golars_results = parse_golars_results(args.golars)
    else:
        print(f"Warning: golars results not found at {args.golars}")
    
    if polars_results or golars_results:
        compare_results(polars_results, golars_results)
    else:
        print("No results to compare.")


if __name__ == '__main__':
    main()