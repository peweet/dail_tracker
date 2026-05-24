"""One-shot rewrite of every existing data/{silver,gold}/parquet/*.parquet with
zstd-3 + statistics. Applies the same URI/Null column drops the writers in
questions.py and legislation.py now produce, so the on-disk artefacts match
what the next pipeline run would emit.

Idempotent: re-reading a zstd parquet and re-writing it with the same options
is a no-op. Re-running this script after the writers have been updated is safe.

Atomic per file: writes to <name>.parquet.bak-rewrite, then replaces the
original. If a single file fails the rest still proceed; the failure is
reported in the summary.

Usage:
    python pipeline_sandbox/parquet_backfill.py
    python pipeline_sandbox/parquet_backfill.py --dry-run
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
TARGET_DIRS = [
    ROOT / "data" / "silver" / "parquet",
    ROOT / "data" / "gold" / "parquet",
]

# Per-file column drops — must match what the writers in questions.py and
# legislation.py now produce. Scoped by full relative path (silver-only) to
# avoid clobbering derived gold parquets that share a stem (e.g.
# data/gold/parquet/questions.parquet is a different 10k-row table emitted by
# a SQL query in lobby_processing.py).
DROP_COLUMNS_BY_FILE: dict[str, list[str]] = {
    "data/silver/parquet/questions.parquet": [
        "debate_section_uri",
        "uri",
        "member_uri",
        "question.debateSection.formats.xml.uri",
        "question.house.uri",
        "question.debateSection.formats.pdf",
        "question.to.uri",
        "question.to.roleType",
        "ministry_role_code",
    ],
    "data/silver/parquet/debates.parquet": [
        "uri",
        "chamber.uri",
        "billSort.billShortTitleEnSort",
        "billSort.billYearSort",
    ],
}


def _kib(n: int) -> str:
    return f"{n / 1024:>9.1f} KiB"


def _rewrite_one(path: Path, *, dry_run: bool) -> tuple[int, int, int, str]:
    """Returns (bytes_before, bytes_after, rows, status)."""
    before = path.stat().st_size

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        return (before, before, 0, f"READ_FAIL: {type(e).__name__}: {e}")

    rows = df.height

    rel_key = str(path.relative_to(ROOT)).replace("\\", "/")
    drop_cols = DROP_COLUMNS_BY_FILE.get(rel_key, [])
    present = [c for c in drop_cols if c in df.columns]
    if present:
        df = df.drop(present)

    if dry_run:
        return (before, before, rows, f"DRY_RUN (would drop {len(present)} cols)")

    tmp = path.with_suffix(path.suffix + ".bak-rewrite")
    try:
        df.write_parquet(
            tmp,
            compression="zstd",
            compression_level=3,
            statistics=True,
        )
    except Exception as e:
        if tmp.exists():
            tmp.unlink()
        return (before, before, rows, f"WRITE_FAIL: {type(e).__name__}: {e}")

    shutil.move(str(tmp), str(path))
    after = path.stat().st_size
    return (before, after, rows, "OK" if not present else f"OK (dropped {len(present)} cols)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen without rewriting any file")
    args = parser.parse_args()

    targets: list[Path] = []
    for d in TARGET_DIRS:
        if not d.exists():
            print(f"SKIP missing dir: {d}")
            continue
        targets.extend(sorted(d.glob("*.parquet")))

    if not targets:
        print("No parquet files found.")
        return 0

    print(f"Backfill {'(DRY RUN)' if args.dry_run else ''} on {len(targets)} files:")
    print(f"  {'file':<58} {'before':>11} {'after':>11} {'delta':>8}  status")
    print(f"  {'-' * 58} {'-' * 11} {'-' * 11} {'-' * 8}  {'-' * 6}")

    total_before = 0
    total_after = 0
    failures: list[tuple[Path, str]] = []
    t0 = time.perf_counter()

    for path in targets:
        before, after, rows, status = _rewrite_one(path, dry_run=args.dry_run)
        total_before += before
        total_after += after
        rel = str(path.relative_to(ROOT)).replace("\\", "/")
        if len(rel) > 58:
            rel = "…" + rel[-57:]
        delta_pct = (1 - after / before) * 100 if before else 0
        print(f"  {rel:<58} {_kib(before)} {_kib(after)} {delta_pct:>6.1f}%  {status}")
        if not status.startswith("OK") and not status.startswith("DRY_RUN"):
            failures.append((path, status))

    elapsed = time.perf_counter() - t0
    saved = total_before - total_after
    saved_pct = (saved / total_before * 100) if total_before else 0
    print()
    print(f"Total: {_kib(total_before)} -> {_kib(total_after)}  "
          f"({saved_pct:.1f}% reduction, {saved/1024:.1f} KiB saved)  "
          f"in {elapsed:.1f}s")

    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for p, s in failures:
            print(f"  {p.relative_to(ROOT)}: {s}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
