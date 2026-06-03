"""tools/gold_rowcounts.py — row counts for every committed gold parquet.

Pure read. Two modes:

    python tools/gold_rowcounts.py                  # JSON {filename: rows} to stdout
    python tools/gold_rowcounts.py --diff A B        # compare two captured JSON files

Used by the pipeline-probe workflow to measure cold-start thinning (baseline
vs after a run), and reusable as the basis for a publish-time regression guard
(refuse to publish if a gold table loses a large fraction of its rows).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import GOLD_PARQUET_DIR  # noqa: E402


def emit_counts() -> dict[str, object]:
    """Map each gold parquet filename to its row count (or an ERROR marker)."""
    counts: dict[str, object] = {}
    for p in sorted(GOLD_PARQUET_DIR.glob("*.parquet")):
        try:
            counts[p.name] = int(pl.scan_parquet(p).select(pl.len()).collect().item())
        except Exception as e:  # noqa: BLE001 — a probe must report, not crash, on a bad file
            counts[p.name] = f"ERROR: {type(e).__name__}"
    return counts


def print_diff(before_path: str, after_path: str) -> None:
    """Print a before/after row-count table, flagging tables that lost >50%."""
    before = json.loads(Path(before_path).read_text())
    after = json.loads(Path(after_path).read_text())
    names = sorted(set(before) | set(after))
    print(f"{'gold table':50} {'before':>12} {'after':>12}  delta")
    print("-" * 92)
    for n in names:
        b = before.get(n, "-")
        a = after.get(n, "-")
        delta = ""
        if isinstance(b, int) and isinstance(a, int):
            d = a - b
            pct = (d / b * 100) if b else 0.0
            flag = "  <-- DROPPED >50%" if (b > 0 and a < b * 0.5) else ""
            delta = f"{d:+d} ({pct:+.0f}%){flag}"
        print(f"{n:50} {str(b):>12} {str(a):>12}  {delta}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"), help="compare two captured JSON files")
    args = parser.parse_args(argv)

    if args.diff:
        print_diff(*args.diff)
    else:
        json.dump(emit_counts(), sys.stdout, indent=2)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
