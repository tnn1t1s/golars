#!/usr/bin/env python3
"""
Polars IO benchmark comparison script.

Run golars benchmarks first, then run this script to compare:
    go test -bench=. ./benchmarks/io/size -benchmem > /tmp/golars_io.txt
    python benchmarks/io/polars_comparison.py
"""

import argparse
import os
import random
import tempfile
import time
from pathlib import Path

import polars as pl


PARQUET_WRITE_KWARGS = {
    "compression": "snappy",
}

EXTERNAL_DIR_ENV = "GOLARS_IO_BENCH_DATA_DIR"


def resolve_external_dir(cli_value: str | None) -> Path | None:
    if cli_value:
        return Path(cli_value)
    env_value = os.getenv(EXTERNAL_DIR_ENV)
    if env_value:
        return Path(env_value)
    return None


def external_path(base_dir: Path, category: str, name: str, ext: str) -> Path:
    return base_dir / category / f"{name}.{ext}"


def require_external_path(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing external benchmark file: {path}")


def generate_h2o_style(n_rows: int, n_groups: int, seed: int = 42) -> pl.DataFrame:
    """Generate H2O.ai style benchmark data (9 columns)."""
    random.seed(seed)

    group_str_small = [f"id{i+1:03d}" for i in range(n_groups)]
    group_str_large = [f"id{i+1:010d}" for i in range(n_rows // n_groups)]

    return pl.DataFrame({
        "id1": [group_str_small[random.randint(0, n_groups-1)] for _ in range(n_rows)],
        "id2": [group_str_small[random.randint(0, n_groups-1)] for _ in range(n_rows)],
        "id3": [group_str_large[random.randint(0, n_rows//n_groups-1)] for _ in range(n_rows)],
        "id4": [random.randint(1, n_groups) for _ in range(n_rows)],
        "id5": [random.randint(1, n_groups) for _ in range(n_rows)],
        "id6": [random.randint(1, n_rows//n_groups) for _ in range(n_rows)],
        "v1": [random.randint(1, 5) for _ in range(n_rows)],
        "v2": [random.randint(1, 15) for _ in range(n_rows)],
        "v3": [round(random.random() * 100, 6) for _ in range(n_rows)],
    }).cast({
        "id4": pl.Int32, "id5": pl.Int32, "id6": pl.Int32,
        "v1": pl.Int32, "v2": pl.Int32
    })


def generate_wide_data(n_rows: int, n_str: int, n_int: int, n_flt: int,
                       n_groups: int = 1000, seed: int = 42) -> pl.DataFrame:
    """Generate wide benchmark data with configurable column counts."""
    random.seed(seed)

    group_strs = [f"id{i+1:06d}" for i in range(n_groups)]

    data = {}
    for i in range(n_str):
        data[f"str_{i}"] = [group_strs[random.randint(0, n_groups-1)] for _ in range(n_rows)]
    for i in range(n_int):
        data[f"int_{i}"] = [random.randint(0, 1000) for _ in range(n_rows)]
    for i in range(n_flt):
        data[f"flt_{i}"] = [round(random.random() * 1000, 6) for _ in range(n_rows)]

    return pl.DataFrame(data)


def bench(name: str, fn, iterations: int = 10, warmup: int = 3) -> float:
    """Run benchmark and return average time in microseconds."""
    for _ in range(warmup):
        fn()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)

    avg_us = sum(times) / len(times) * 1e6
    print(f"{name}: {avg_us:,.0f} µs/op")
    return avg_us


def run_size_ladder(external_dir: Path | None) -> None:
    """Benchmark across different data sizes."""
    print("\n" + "="*60)
    print("SIZE LADDER BENCHMARKS")
    print("="*60)

    sizes = [
        ("Small", 10_000, 100),
        ("Medium", 100_000, 1_000),
        ("Large", 1_000_000, 10_000),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        for name, rows, groups in sizes:
            print(f"\n--- {name} ({rows:,} rows) ---")
            label = name.lower()

            if external_dir:
                parquet_path = external_path(external_dir, "size", label, "parquet")
                csv_path = external_path(external_dir, "size", label, "csv")
                require_external_path(parquet_path)
                require_external_path(csv_path)
                df = pl.read_parquet(parquet_path)
            else:
                df = generate_h2o_style(rows, groups)
                parquet_path = tmpdir / f"{label}.parquet"
                csv_path = tmpdir / f"{label}.csv"
                df.write_parquet(parquet_path, **PARQUET_WRITE_KWARGS)
                df.write_csv(csv_path)

            iters = max(1, 100_000 // rows)
            write_parquet_path = tmpdir / f"write_{label}.parquet"
            write_csv_path = tmpdir / f"write_{label}.csv"

            bench(
                f"ParquetWrite_{name}",
                lambda p=write_parquet_path: df.write_parquet(p, **PARQUET_WRITE_KWARGS),
                iterations=iters,
            )
            bench(
                f"CSVWrite_{name}",
                lambda p=write_csv_path: df.write_csv(p),
                iterations=iters,
            )
            bench(
                f"ParquetRead_{name}",
                lambda p=parquet_path: pl.read_parquet(p),
                iterations=iters,
            )
            bench(
                f"CSVRead_{name}",
                lambda p=csv_path: pl.read_csv(p),
                iterations=iters,
            )


def run_width_ladder(external_dir: Path | None) -> None:
    """Benchmark across different column widths."""
    print("\n" + "="*60)
    print("WIDTH LADDER BENCHMARKS (100K rows)")
    print("="*60)

    widths = [
        ("Narrow", "narrow", 1, 1, 1),
        ("Medium", "medium", 3, 4, 2),
        ("Wide", "wide", 10, 20, 20),
        ("VeryWide", "very_wide", 50, 100, 50),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        for label, name, n_str, n_int, n_flt in widths:
            total_cols = n_str + n_int + n_flt
            print(f"\n--- {label} ({total_cols} columns) ---")

            if external_dir:
                parquet_path = external_path(external_dir, "width", name, "parquet")
                require_external_path(parquet_path)
                df = pl.read_parquet(parquet_path)
            else:
                df = generate_wide_data(100_000, n_str, n_int, n_flt)
                parquet_path = tmpdir / f"{name}.parquet"
                df.write_parquet(parquet_path, **PARQUET_WRITE_KWARGS)

            write_parquet_path = tmpdir / f"write_{name}.parquet"
            bench(
                f"ParquetWrite_{label}",
                lambda p=write_parquet_path: df.write_parquet(p, **PARQUET_WRITE_KWARGS),
                iterations=10,
            )
            bench(
                f"ParquetRead_{label}",
                lambda p=parquet_path: pl.read_parquet(p),
                iterations=10,
            )


def run_projection_benchmarks(external_dir: Path | None) -> None:
    """Benchmark column projection."""
    print("\n" + "="*60)
    print("PROJECTION BENCHMARKS (100K rows, 50 columns)")
    print("="*60)

    if external_dir:
        parquet_path = external_path(external_dir, "projection", "projection_50cols", "parquet")
        require_external_path(parquet_path)
    else:
        df = generate_wide_data(100_000, 10, 20, 20)
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "projection.parquet"
            df.write_parquet(parquet_path, **PARQUET_WRITE_KWARGS)

    projections = [
        ("1of50", ["str_0"]),
        ("5of50", ["str_0", "int_0", "int_5", "flt_0", "flt_10"]),
        ("10of50", [f"str_{i}" for i in range(2)] + [f"int_{i}" for i in range(4)] + [f"flt_{i}" for i in range(4)]),
        ("AllCols", None),
    ]

    for name, cols in projections:
        if cols:
            bench(
                f"Projection_{name}",
                lambda c=cols, p=parquet_path: pl.read_parquet(p, columns=c),
                iterations=20,
            )
        else:
            bench(
                f"Projection_{name}",
                lambda p=parquet_path: pl.read_parquet(p),
                iterations=20,
            )


def run_format_comparison(external_dir: Path | None) -> None:
    """Compare Parquet vs CSV."""
    print("\n" + "="*60)
    print("FORMAT COMPARISON (Parquet vs CSV)")
    print("="*60)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        if external_dir:
            parquet_path = external_path(external_dir, "format", "medium", "parquet")
            csv_path = external_path(external_dir, "format", "medium", "csv")
            require_external_path(parquet_path)
            require_external_path(csv_path)
            df = pl.read_parquet(parquet_path)
        else:
            df = generate_h2o_style(100_000, 1_000)
            parquet_path = tmpdir / "format.parquet"
            csv_path = tmpdir / "format.csv"
            df.write_parquet(parquet_path, **PARQUET_WRITE_KWARGS)
            df.write_csv(csv_path)

        write_parquet_path = tmpdir / "format_write.parquet"
        write_csv_path = tmpdir / "format_write.csv"

        bench("ParquetWrite", lambda: df.write_parquet(write_parquet_path, **PARQUET_WRITE_KWARGS), iterations=20)
        bench("CSVWrite", lambda: df.write_csv(write_csv_path), iterations=20)

        print(f"\nFile sizes: Parquet={parquet_path.stat().st_size:,} bytes, CSV={csv_path.stat().st_size:,} bytes")

        bench("ParquetRead", lambda: pl.read_parquet(parquet_path), iterations=20)
        bench("CSVRead", lambda: pl.read_csv(csv_path), iterations=20)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Polars IO benchmarks for golars comparison.")
    parser.add_argument(
        "--external-dir",
        help=f"Directory containing golars-generated benchmark files (or set {EXTERNAL_DIR_ENV}).",
    )
    args = parser.parse_args()

    external_dir = resolve_external_dir(args.external_dir)
    if external_dir:
        print(f"Using external benchmark files from {external_dir}")

    print(f"Polars version: {pl.__version__}")
    print("Running IO benchmarks for comparison with golars")

    run_size_ladder(external_dir)
    run_width_ladder(external_dir)
    run_projection_benchmarks(external_dir)
    run_format_comparison(external_dir)

    print("\n" + "="*60)
    print("BENCHMARK COMPLETE")
    print("="*60)


if __name__ == "__main__":
    main()
