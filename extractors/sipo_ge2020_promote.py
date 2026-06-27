"""Promote the validated GE2020 national-agent silver -> gold (SEPARATE facts, so the
committed GE2024 facts + the live v_sipo_party_national_* views are untouched).

Reads (from the OCR'd + reconciled silver, see doc/OCR_RUN_ASSESSMENT_2026_06_26.md):
  data/silver/sipo/ge2020_national_agent_items.parquet
  data/silver/sipo/ge2020_national_agent_categories.parquet
Writes:
  data/gold/parquet/sipo_ge2020_expense_items.parquet
  data/gold/parquet/sipo_ge2020_expense_categories.parquet

No PII (party-level supplier/expense lines + statutory-heading totals — the public SIPO
record). Headline figure is the PRINTED overall (category_total_eur, is_overall); the
`reconciles` flag marks parties whose OCR'd line items don't sum to it (SF/IFP/Aontú —
verify against the official PDF). Consumed by v_sipo_ge2020_party_national_*.

Run:  ./.venv/Scripts/python.exe extractors/sipo_ge2020_promote.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/sipo"
GOLD = ROOT / "data/gold/parquet"


def main() -> None:
    print("=== PROMOTE SIPO GE2020 national-agent silver -> gold ===")
    items_src = SILVER / "ge2020_national_agent_items.parquet"
    cats_src = SILVER / "ge2020_national_agent_categories.parquet"
    if not items_src.exists() or not cats_src.exists():
        print(f"  !! missing GE2020 silver ({items_src.name} / {cats_src.name})")
        return
    GOLD.mkdir(parents=True, exist_ok=True)

    items = pl.read_parquet(items_src).with_columns(pl.lit("GE2020").alias("election_event"))
    cats = pl.read_parquet(cats_src).with_columns(pl.lit("GE2020").alias("election_event"))

    save_parquet(items, GOLD / "sipo_ge2020_expense_items.parquet")
    save_parquet(cats, GOLD / "sipo_ge2020_expense_categories.parquet")
    print(f"  items     -> sipo_ge2020_expense_items.parquet ({items.height} rows)")
    print(f"  categories-> sipo_ge2020_expense_categories.parquet ({cats.height} rows)")
    ov = cats.filter(pl.col("is_overall"))
    print(f"  parties with a printed overall: {ov['party'].n_unique()} "
          f"(reconciling item detail: {ov.filter(pl.col('reconciles'))['party'].n_unique()})")
    print("done.")


if __name__ == "__main__":
    main()
