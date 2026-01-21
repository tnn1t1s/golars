import statistics
import time

import polars as pl

LEFT_ROWS = 50_000
RIGHT_ROWS = 5_000
GROUPS = 1_000
RUNS = 5


class LCG:
    def __init__(self, seed: int) -> None:
        self.state = seed & 0xFFFFFFFF

    def next(self) -> int:
        self.state = (self.state * 1664525 + 1013904223) & 0xFFFFFFFF
        return self.state


def build_int_keys(rows: int, groups: int, seed: int):
    rng = LCG(seed)
    keys = [0] * rows
    values = [0] * rows
    for i in range(rows):
        keys[i] = rng.next() % groups
        values[i] = rng.next()
    return keys, values


def build_string_keys(rows: int, groups: int, seed: int):
    rng = LCG(seed)
    keys = [""] * rows
    values = [0] * rows
    for i in range(rows):
        keys[i] = f"k{rng.next() % groups:04d}"
        values[i] = rng.next()
    return keys, values


def build_string_pairs(rows: int, groups: int, seed_a: int, seed_b: int):
    rng_a = LCG(seed_a)
    rng_b = LCG(seed_b)
    keys_a = [""] * rows
    keys_b = [""] * rows
    values = [0] * rows
    for i in range(rows):
        keys_a[i] = f"k{rng_a.next() % groups:04d}"
        keys_b[i] = f"k{rng_b.next() % groups:04d}"
        values[i] = rng_a.next()
    return keys_a, keys_b, values


def bench(fn):
    times = []
    for _ in range(RUNS):
        start = time.perf_counter()
        result = fn()
        _ = result.height
        times.append(time.perf_counter() - start)
    return {
        "mean_ms": statistics.mean(times) * 1000.0,
        "min_ms": min(times) * 1000.0,
        "max_ms": max(times) * 1000.0,
    }


def main() -> None:
    int_keys_left, int_vals_left = build_int_keys(LEFT_ROWS, GROUPS, 1)
    int_keys_right, int_vals_right = build_int_keys(RIGHT_ROWS, GROUPS, 2)

    str_keys_left, str_vals_left = build_string_keys(LEFT_ROWS, GROUPS, 3)
    str_keys_right, str_vals_right = build_string_keys(RIGHT_ROWS, GROUPS, 4)

    str_a_left, str_b_left, vals_left = build_string_pairs(LEFT_ROWS, GROUPS, 5, 6)
    str_a_right, str_b_right, vals_right = build_string_pairs(RIGHT_ROWS, GROUPS, 7, 8)

    df_int_left = pl.DataFrame({"id": int_keys_left, "v": int_vals_left})
    df_int_right = pl.DataFrame({"id": int_keys_right, "w": int_vals_right})

    df_str_left = pl.DataFrame({"id": str_keys_left, "v": str_vals_left})
    df_str_right = pl.DataFrame({"id": str_keys_right, "w": str_vals_right})

    df_str2_left = pl.DataFrame({"id1": str_a_left, "id2": str_b_left, "v": vals_left})
    df_str2_right = pl.DataFrame({"id1": str_a_right, "id2": str_b_right, "w": vals_right})

    # Warmup
    _ = df_int_left.join(df_int_right, on="id", how="inner")
    _ = df_int_left.join(df_int_right, on="id", how="left")
    _ = df_str_left.join(df_str_right, on="id", how="inner")
    _ = df_str_left.join(df_str_right, on="id", how="left")
    _ = df_str2_left.join(df_str2_right, on=["id1", "id2"], how="inner")

    results = {
        "inner_int64": bench(lambda: df_int_left.join(df_int_right, on="id", how="inner")),
        "left_int64": bench(lambda: df_int_left.join(df_int_right, on="id", how="left")),
        "inner_string": bench(lambda: df_str_left.join(df_str_right, on="id", how="inner")),
        "left_string": bench(lambda: df_str_left.join(df_str_right, on="id", how="left")),
        "inner_string_2col": bench(
            lambda: df_str2_left.join(df_str2_right, on=["id1", "id2"], how="inner")
        ),
    }

    print("polars", pl.__version__)
    for name, stats in results.items():
        print(name, stats)


if __name__ == "__main__":
    main()
