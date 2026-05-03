"""Empirical demo: how much does typing + zstd actually save on Dáil Tracker parquets?

Reads every parquet in data/gold/parquet/ (and optionally silver), writes four
variants of each to pipeline_sandbox/_zstd_demo_out/, and prints a side-by-side
size + read-time comparison.

Variants:
    baseline    — uncompressed, no dtype changes
    zstd_only   — zstd-3, no dtype changes
    typed_only  — uncompressed, Int32 years + Categorical low-cardinality cols
    typed_zstd  — zstd-3 + the above typing

Run from project root:
    python pipeline_sandbox/zstd_typing_demo.py
    python pipeline_sandbox/zstd_typing_demo.py --include-silver
    python pipeline_sandbox/zstd_typing_demo.py --file current_dail_vote_history.parquet
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = ROOT / "data" / "gold" / "parquet"
SILVER_DIR = ROOT / "data" / "silver" / "parquet"
OUT_DIR = Path(__file__).resolve().parent / "_zstd_demo_out"

LOW_CARDINALITY_COLUMNS = {
    "party", "party_name",
    "chamber",
    "constituency", "constituency_name",
    "taa_band_label",
    "court",
    "dail_term", "dail_number",
    "is_minister",
    "delivery_method",
    "policy_area",
    "vote_type", "vote_result",
    "Position",
}

YEAR_COLUMNS = {"year", "payment_year", "year_elected"}

CARDINALITY_THRESHOLD = 50


def apply_typing(df: pl.DataFrame, *, verbose: bool = False) -> pl.DataFrame:
    """Cast year columns to Int32 and low-cardinality string columns to Categorical.

    Two strategies for catching low-cardinality columns:
      1. The known-name set above (LOW_CARDINALITY_COLUMNS).
      2. An auto-detect pass: any Utf8 column whose distinct count <= CARDINALITY_THRESHOLD.
    """
    casts = []
    notes = []

    for col, dtype in df.schema.items():
        if col in YEAR_COLUMNS and dtype.is_integer():
            casts.append(pl.col(col).cast(pl.Int32))
            notes.append(f"  {col}: {dtype} -> Int32")
            continue

        if dtype == pl.Utf8:
            if col in LOW_CARDINALITY_COLUMNS:
                casts.append(pl.col(col).cast(pl.Categorical))
                notes.append(f"  {col}: Utf8 -> Categorical (known low-card)")
                continue

            try:
                n_unique = df[col].n_unique()
            except Exception:
                n_unique = None
            if n_unique is not None and n_unique <= CARDINALITY_THRESHOLD:
                casts.append(pl.col(col).cast(pl.Categorical))
                notes.append(f"  {col}: Utf8 -> Categorical (auto, {n_unique} distinct)")

    if verbose and notes:
        print("  Typing changes:")
        for n in notes:
            print(n)
    elif verbose:
        print("  Typing changes: none")

    return df.with_columns(casts) if casts else df


def write_variants(df: pl.DataFrame, stem: str) -> dict[str, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    paths = {
        "baseline":   OUT_DIR / f"{stem}__baseline.parquet",
        "zstd_only":  OUT_DIR / f"{stem}__zstd_only.parquet",
        "typed_only": OUT_DIR / f"{stem}__typed_only.parquet",
        "typed_zstd": OUT_DIR / f"{stem}__typed_zstd.parquet",
    }

    df.write_parquet(paths["baseline"], compression="uncompressed")
    df.write_parquet(paths["zstd_only"], compression="zstd", compression_level=3, statistics=True)

    typed = apply_typing(df)
    typed.write_parquet(paths["typed_only"], compression="uncompressed")
    typed.write_parquet(paths["typed_zstd"], compression="zstd", compression_level=3, statistics=True)

    return paths


def time_read(path: Path, n_iter: int = 3) -> float:
    times = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        _ = pl.read_parquet(path)
        times.append(time.perf_counter() - t0)
    return min(times)


def kib(n_bytes: int) -> str:
    return f"{n_bytes / 1024:>9.1f} KiB"


def pct(part: int, whole: int) -> str:
    if whole == 0:
        return "   n/a"
    return f"{(1 - part / whole) * 100:>5.1f}%"


def report_one(parquet_path: Path, *, verbose: bool) -> None:
    print(f"\n{'=' * 78}")
    print(f"FILE: {parquet_path.relative_to(ROOT)}")
    print(f"{'=' * 78}")

    df = pl.read_parquet(parquet_path)
    print(f"  rows: {df.height:>8,}    cols: {df.width:>3}")

    if verbose:
        print(f"  schema preview: {dict(list(df.schema.items())[:5])}")

    paths = write_variants(df, parquet_path.stem)
    sizes = {name: p.stat().st_size for name, p in paths.items()}

    print()
    print(f"  {'variant':<14} {'size':>14} {'vs baseline':>13}   {'read time (s)':>14}")
    print(f"  {'-' * 14} {'-' * 14} {'-' * 13}   {'-' * 14}")
    base = sizes["baseline"]
    for variant in ("baseline", "zstd_only", "typed_only", "typed_zstd"):
        size = sizes[variant]
        rt = time_read(paths[variant])
        print(f"  {variant:<14} {kib(size)} {pct(size, base):>13}   {rt:>14.4f}")

    if verbose:
        print()
        apply_typing(df, verbose=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-silver", action="store_true",
                        help="Also process data/silver/parquet/")
    parser.add_argument("--file", type=str, default=None,
                        help="Run on one specific parquet filename (looked up in gold then silver)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print per-column typing decisions")
    args = parser.parse_args()

    targets: list[Path] = []
    if args.file:
        for d in (GOLD_DIR, SILVER_DIR):
            candidate = d / args.file
            if candidate.exists():
                targets.append(candidate)
                break
        if not targets:
            raise SystemExit(f"File not found: {args.file}")
    else:
        targets = sorted(GOLD_DIR.glob("*.parquet"))
        if args.include_silver:
            targets += sorted(SILVER_DIR.glob("*.parquet"))

    print(f"Demo output dir: {OUT_DIR}")
    print(f"Processing {len(targets)} parquet file(s).")

    totals = {"baseline": 0, "zstd_only": 0, "typed_only": 0, "typed_zstd": 0}

    for path in targets:
        try:
            report_one(path, verbose=args.verbose)
            for variant, p in {
                "baseline":   OUT_DIR / f"{path.stem}__baseline.parquet",
                "zstd_only":  OUT_DIR / f"{path.stem}__zstd_only.parquet",
                "typed_only": OUT_DIR / f"{path.stem}__typed_only.parquet",
                "typed_zstd": OUT_DIR / f"{path.stem}__typed_zstd.parquet",
            }.items():
                totals[variant] += p.stat().st_size
        except Exception as e:
            print(f"  SKIPPED ({type(e).__name__}: {e})")

    print(f"\n{'=' * 78}")
    print(f"TOTALS across {len(targets)} files")
    print(f"{'=' * 78}")
    base = totals["baseline"]
    for variant in ("baseline", "zstd_only", "typed_only", "typed_zstd"):
        size = totals[variant]
        print(f"  {variant:<14} {kib(size)} {pct(size, base):>13}")

    if base > 0:
        final = totals["typed_zstd"]
        saved_pct = (1 - final / base) * 100
        print(f"\n  Combined typing + zstd-3 reduces total parquet footprint by "
              f"{saved_pct:.1f}% on this data set.")


if __name__ == "__main__":
    main()
