"""One-shot data patch: repair the ``paid_flag`` column-misalignment leak in the
already-written payment facts (silver + gold), offline.

WHY: the 2026-06-22 DQ audit (doc/DATA_QUALITY_AUDIT.md) found that ~16% of
``public_payments_fact`` rows carried non-flag content in ``paid_flag`` — leaked
category text, payment-month dates, or amounts — because the generic public-body
parser's column-role heuristic grabbed the wrong column for some publishers. The
parser is now fixed at source (extractors/_paid_flag_clean.clean_paid_flag, wired
into procurement_public_body_extract + procurement_payments_consolidate), but
re-parsing every publisher would re-crawl the network. This applies the SAME
cleaner to the existing parquet so the fix lands without a re-fetch.

WHAT it does, per fact: keeps genuine flag tokens, moves recoverable category text
into ``description`` where that was empty, and nulls date/amount/other leaks. Row
count and ``amount_eur`` sum are invariant (asserted), so the save_parquet row
floor holds and the gold reconciliation stays valid. Idempotent — a no-op on a
fact whose paid_flag is already clean (e.g. nphdb/nta set it null). Reversible via
``git checkout`` on the parquet files.

Run: ./.venv/Scripts/python.exe tools/patch_paid_flag_misalignment.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from extractors._paid_flag_clean import clean_paid_flag, paid_flag_is_clean  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

TARGETS = [
    ROOT / "data/silver/parquet/public_payments_fact.parquet",
    ROOT / "data/silver/parquet/la_payments_fact.parquet",
    ROOT / "data/silver/parquet/dept_readingorder_payments_fact.parquet",
    ROOT / "data/silver/parquet/hse_tusla_payments_fact.parquet",
    ROOT / "data/silver/parquet/nphdb_payments_fact.parquet",
    ROOT / "data/silver/parquet/nta_payments_fact.parquet",
    ROOT / "data/silver/parquet/seai_payments_fact.parquet",
    ROOT / "data/gold/parquet/procurement_payments_fact.parquet",
]


def patch(path: Path) -> None:
    if not path.exists():
        print(f"  SKIP (absent): {path.name}")
        return
    df = pl.read_parquet(path)
    n = df.height
    if "paid_flag" not in df.columns:
        print(f"  SKIP (no paid_flag): {path.name}")
        return
    bad_before = paid_flag_is_clean(df)
    eur_before = float(df["amount_eur"].sum() or 0.0) if "amount_eur" in df.columns else None

    cleaned, stats = clean_paid_flag(df)

    # invariants
    assert cleaned.height == n, f"{path.name}: row count changed {n} -> {cleaned.height}"
    if eur_before is not None:
        eur_after = float(cleaned["amount_eur"].sum() or 0.0)
        assert abs(eur_after - eur_before) < 1.0, f"{path.name}: amount_eur sum drifted {eur_before} -> {eur_after}"
    bad_after = paid_flag_is_clean(cleaned)
    assert bad_after == 0, f"{path.name}: {bad_after} non-flag paid_flag values remain after clean"

    if bad_before == 0:
        print(f"  CLEAN already: {path.name} (rows={n})")
        return
    save_parquet(cleaned, path, min_rows=n)
    print(
        f"  PATCHED {path.name}: rows={n} (unchanged); "
        f"leak cleared={stats.get('n_leak', 0):,} "
        f"(recovered->description={stats.get('n_recovered', 0):,}, "
        f"dates={stats.get('n_month', 0):,}, amounts={stats.get('n_amount', 0):,}); "
        f"genuine flags kept={stats.get('n_genuine', 0):,}; bad {bad_before:,}→{bad_after}"
    )


if __name__ == "__main__":
    print("paid_flag misalignment repair (silver + gold):")
    for p in TARGETS:
        patch(p)
    print("done.")
