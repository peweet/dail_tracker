"""READ-ONLY review probe (LA/Housing review, 2026-06-05).

Confirms ground-truth claims for doc/LOCAL_AUTHORITY_HOUSING_REVIEW.md:
  1. la_afs_divisions schema + the 'net_expenditure-by-division' metric semantics
     (AFS != total spend), incl. the Housing division magnitude vs council total.
  2. Year-mixing across councils (Meath 2019 / DLR 2023 / Kildare 2025) and the
     reconciled-vs-not split — i.e. AFS is NOT a clean comparable cross-LA panel.
  3. Absence of any constituency column anywhere in the AFS/LA facts.

Writes nothing. Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/probe_review_lah_afs_schema.py
"""
from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "data" / "silver" / "parquet"


def banner(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def probe(name: str) -> None:
    p = SILVER / name
    if not p.exists():
        print(f"  MISSING: {p}")
        return
    df = pl.read_parquet(p)
    banner(f"{name}  ({df.height} rows)")
    print("columns:", df.columns)
    has_con = any("constitu" in c.lower() for c in df.columns)
    print("has any constituency column? ->", has_con)
    # year spread (is it a comparable panel?)
    for yc in ("year", "Year"):
        if yc in df.columns:
            print("year value_counts:")
            print(df[yc].value_counts().sort(yc))
            break
    print("sample rows:")
    with pl.Config(tbl_rows=8, fmt_str_lengths=40):
        print(df.head(8))


if __name__ == "__main__":
    for f in (
        "la_afs_divisions.parquet",
        "la_afs_capital_divisions.parquet",
        "afs_amalgamated_divisions.parquet",
        "la_payments_fact.parquet",
    ):
        probe(f)
