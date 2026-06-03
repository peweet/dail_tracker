"""Regenerate the committed golden slice for test_la_payments.py.

Takes a small, representative slice of the full silver fact
(data/silver/parquet/la_payments_fact.parquet) — enough company rows for a stable CRO
band, plus sole-trader / id_code / public-body rows and both value_kinds — and commits it
under test/fixtures/la_payments/ (gitignore-negated) so the Tier-3 invariant tests run in
CI without the multi-megabyte fact or any network.

Run:  ./.venv/Scripts/python.exe test/fixtures/la_payments/_generate.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
FACT = ROOT / "data/silver/parquet/la_payments_fact.parquet"
OUT = Path(__file__).resolve().parent / "la_payments_golden.parquet"

with __import__("contextlib").suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")


def main() -> None:
    df = pl.read_parquet(FACT)
    parts = [
        df.filter(pl.col("supplier_class") == "company").head(40),
        df.filter(pl.col("supplier_class") == "sole_trader_or_individual").head(8),
        df.filter(pl.col("supplier_class") == "id_code").head(4),
        df.filter(pl.col("supplier_class") == "public_body").head(4),
        df.filter(pl.col("value_kind") == "payment_actual").head(6),
    ]
    slice_ = pl.concat(parts).unique(subset=["source_file_hash", "source_row_number", "publisher_id"])
    slice_.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)
    print(f"wrote {OUT}  rows={slice_.height}  councils={slice_['publisher_id'].n_unique()}")
    print(slice_.group_by("supplier_class").len().sort("len", descending=True))


if __name__ == "__main__":
    main()
