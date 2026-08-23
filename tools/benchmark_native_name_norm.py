"""Compare the opt-in PyO3 normaliser with the current Python oracle.

The benchmark must be run against a representative local Parquet snapshot and
does not change an extractor route. It fails if native output differs from the
current scalar contract.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from pathlib import Path

import services.runtime_env  # noqa: F401  # native thread caps must precede Polars
from shared.name_norm import name_norm_many


def _best_seconds(callback: Callable[[], object], repeats: int) -> float:
    durations: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        callback()
        durations.append(time.perf_counter() - start)
    return min(durations)


def main() -> None:
    import polars as pl

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("data/gold/parquet/corporate_notices.parquet"),
    )
    parser.add_argument("--limit", type=int, default=50_000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.limit < 1 or args.workers < 1 or args.repeats < 1:
        parser.error("limit, workers, and repeats must each be at least one")

    values = (
        pl.scan_parquet(args.source)
        .select("entity_name")
        .drop_nulls()
        .limit(args.limit)
        .collect()
        .get_column("entity_name")
        .to_list()
    )
    expected = name_norm_many(values, backend="python")
    native = name_norm_many(values, backend="native", workers=args.workers)
    if native != expected:
        raise AssertionError("native normalisation drifted from the Python oracle")

    python_seconds = _best_seconds(lambda: name_norm_many(values, backend="python"), args.repeats)
    native_seconds = _best_seconds(lambda: name_norm_many(values, backend="native", workers=args.workers), args.repeats)
    speedup = python_seconds / native_seconds if native_seconds else float("inf")
    print(f"rows={len(values)} repeats={args.repeats} workers={args.workers}")
    print(f"python_best_seconds={python_seconds:.6f}")
    print(f"native_best_seconds={native_seconds:.6f}")
    print(f"native_speedup={speedup:.2f}x")


if __name__ == "__main__":
    main()
