"""
Side-by-side comparison: refetch 2023/2024/2025 windows via API and compare
row counts to the existing manual exports. Diagnostic-only — does NOT
overwrite the manual files in data/bronze/lobbying_csv_data/.

Output goes to data/bronze/lobbying_csv_data/_api_refetch_compare/.
"""

from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from lobbying_fetch import (  # noqa: E402
    BRONZE_DIR,
    REPO_ROOT,
    fetch_window,
    filename_for,
    fmt_bytes,
    url_for,
)

COMPARE_DIR = BRONZE_DIR / "_api_refetch_compare"
WINDOWS = [
    (date(2023, 2, 1), date(2024, 2, 1)),
    (date(2024, 2, 1), date(2025, 2, 1)),
    (date(2025, 2, 1), date(2026, 2, 1)),
]


def main() -> int:
    COMPARE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Refetching {len(WINDOWS)} windows for comparison")
    print(f"Manual exports : {BRONZE_DIR.relative_to(REPO_ROOT)}")
    print(f"API refetch dir: {COMPARE_DIR.relative_to(REPO_ROOT)}")
    print("=" * 70)

    rows: list[dict] = []
    for ws, we in WINDOWS:
        name = filename_for(ws, we)
        manual_path = BRONZE_DIR / name
        api_path = COMPARE_DIR / name

        if api_path.exists():
            print(f"  [skip]  {name}  api copy exists")
        else:
            print(f"  [fetch] {ws} -> {we}  ...", flush=True)
            t0 = time.monotonic()
            ok, written, status = fetch_window(ws, we, api_path)
            elapsed = time.monotonic() - t0
            if ok:
                print(f"          OK  {fmt_bytes(written)} in {elapsed:.1f}s")
            else:
                print(f"          FAIL  {status}")
                continue

        manual_rows = (
            pl.read_csv(manual_path, infer_schema_length=0).height if manual_path.exists() else None
        )
        api_rows = pl.read_csv(api_path, infer_schema_length=0).height
        manual_size = manual_path.stat().st_size if manual_path.exists() else None
        api_size = api_path.stat().st_size

        rows.append({
            "window": f"{ws} -> {we}",
            "manual_rows": manual_rows,
            "api_rows": api_rows,
            "delta": (api_rows - manual_rows) if manual_rows is not None else None,
            "manual_size": fmt_bytes(manual_size) if manual_size else "—",
            "api_size": fmt_bytes(api_size),
        })

    print()
    print("=" * 70)
    print("COMPARISON")
    print("=" * 70)
    print(f"{'window':<28}  {'manual':>10}  {'api':>10}  {'delta':>10}  {'manual sz':>10}  {'api sz':>10}")
    for r in rows:
        m = f"{r['manual_rows']:,}" if r['manual_rows'] is not None else "—"
        a = f"{r['api_rows']:,}"
        d = f"{r['delta']:+,}" if r['delta'] is not None else "—"
        print(f"{r['window']:<28}  {m:>10}  {a:>10}  {d:>10}  {r['manual_size']:>10}  {r['api_size']:>10}")

    if rows and rows[0]["manual_rows"] is not None:
        for r in rows:
            if r["delta"] and abs(r["delta"]) > 100:
                pct = (r["delta"] / r["manual_rows"]) * 100 if r["manual_rows"] else 0
                print(f"\nNote: {r['window']} differs by {r['delta']:+,} rows ({pct:+.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
