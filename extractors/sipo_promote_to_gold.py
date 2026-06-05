"""Promote the SIPO GE2024 silver facts → gold (an extractors/ script that writes
data/gold/parquet/, the sanctioned medallion pattern like the other extractors).

Promotes BOTH tracks:
  * donations    — data/silver/sipo/sipo_donations_fact.parquet → sipo_donations.parquet
  * expenses     — data/silver/sipo/sipo_expenses_fact.parquet  → sipo_expenses_fact.parquet
                   (the Part-3 candidate-summary fact the v_sipo_expenses_base view reads)
The Part-4 itemised/category facts stay silver-only until a view consumes them.

⚠️ PRIVACY (non-negotiable): gold/parquet/ is COMMITTED to the public repo. Donor
NAMES + AMOUNTS are the public SIPO record and may be promoted; donor HOME ADDRESSES
are NOT — `donor_address_raw` is DROPPED here so it can never reach git or the UI.
No-inference: gold carries figures + flags only.

Run:  ./.venv/Scripts/python.exe extractors/sipo_promote_to_gold.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/sipo"
GOLD = ROOT / "data/gold/parquet"

# Canonicalise party names to the registry/expenses spelling (donations forms use
# verbose legal names + an OCR all-caps; normalise so the two facts join cleanly).
PARTY_CANON = {
    "the labour party": "Labour",
    "people before profit - solidarity": "People Before Profit/Solidarity",
    "the green party / an comhaontas glas": "Green Party",
    "social democrats": "Social Democrats",
    "sinn féin": "Sinn Féin",
    "aontú": "Aontú",
    "fianna fáil": "Fianna Fáil",
    "fine gael": "Fine Gael",
}


def canon_party(name: str | None) -> str | None:
    if not name:
        return None
    return PARTY_CANON.get(name.strip().lower(), name.strip())


def address_columns(df: pl.DataFrame) -> list[str]:
    """Columns that look like they carry a postal address (PII). Donor NAMES + AMOUNTS
    are the public SIPO record and stay; only addresses are dropped/guarded. Name-based
    so a renamed/extra address column (not just donor_address_raw) is still caught."""
    return [c for c in df.columns if "address" in c.lower()]


def promote_donations() -> None:
    src = SILVER / "sipo_donations_fact.parquet"
    if not src.exists():
        print(f"  !! no donations fact at {src}")
        return
    df = pl.read_parquet(src)
    # DROP donor_address_raw (PII) + any address-bearing columns before gold.
    drop = address_columns(df)
    df = df.drop(drop)
    df = df.with_columns(
        pl.col("party").map_elements(canon_party, return_dtype=pl.Utf8).alias("party"),
        pl.lit("GE2024").alias("election_event"),
    )
    # PRIVACY INVARIANT (runtime, -O-proof; BEFORE the write): no address column may reach
    # committed gold. The old assert was -O-strippable AND ran post-write — both defeated it.
    leaked = address_columns(df)
    if leaked:
        raise RuntimeError(f"PII leak: address column(s) {leaked} must not reach gold")
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_donations.parquet"
    df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
    print(f"  donations -> {out.relative_to(ROOT)}  ({df.height} rows, address dropped: {bool(drop)})")
    print("  parties:", sorted(df["party"].unique().to_list()))
    print("  columns:", df.columns)


def promote_expenses() -> None:
    """Part-3 candidate-summary expenses → gold. No PII (candidate names + amounts
    are the public SIPO record); the schema the v_sipo_expenses_base view selects is
    carried through unchanged. Party names are already canonical (from PARTY_JOBS)."""
    src = SILVER / "sipo_expenses_fact.parquet"
    if not src.exists():
        print(f"  !! no expenses fact at {src}")
        return
    df = pl.read_parquet(src)
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_expenses_fact.parquet"
    df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
    blank = df.filter(pl.col("candidate_name_raw").str.strip_chars() == "").height
    print(f"  expenses  -> {out.relative_to(ROOT)}  ({df.height} rows, {blank} blank-name)")
    print("  parties:", sorted(df["party"].unique().to_list()))


def main() -> None:
    print("=== PROMOTE SIPO silver -> gold ===")
    promote_donations()
    promote_expenses()
    print("done. (Part-4 itemised/category facts stay silver until a view consumes them)")


if __name__ == "__main__":
    main()
