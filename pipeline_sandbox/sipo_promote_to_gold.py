"""Promote SIPO sandbox facts → gold (the sanctioned pattern: a pipeline_sandbox
script that writes data/gold/parquet/, like census_/cso_/housing_* already do).

CURRENTLY ACTIVE: donations only. The expenses facts (Part-3 candidate summary +
Part-4 itemised) are still being finished/owned by the other context (Part-4 = 6
parties pending) — their promotion is added here once that lands, NOT now.

⚠️ PRIVACY (non-negotiable): gold/parquet/ is COMMITTED to the public repo. Donor
NAMES + AMOUNTS are the public SIPO record and may be promoted; donor HOME ADDRESSES
are NOT — `donor_address_raw` is DROPPED here so it can never reach git or the UI.
No-inference: gold carries figures + flags only.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_promote_to_gold.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SANDBOX = ROOT / "pipeline_sandbox/_sipo_output"
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


def promote_donations() -> None:
    src = SANDBOX / "sipo_donations_fact.parquet"
    if not src.exists():
        print(f"  !! no donations fact at {src}")
        return
    df = pl.read_parquet(src)
    # DROP donor_address_raw (PII) + any address-bearing columns before gold.
    drop = [c for c in ("donor_address_raw",) if c in df.columns]
    df = df.drop(drop)
    df = df.with_columns(
        pl.col("party").map_elements(canon_party, return_dtype=pl.Utf8).alias("party"),
        pl.lit("GE2024").alias("election_event"),
    )
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_donations.parquet"
    df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
    assert "donor_address_raw" not in df.columns, "PII leak: address must not reach gold"
    print(f"  donations -> {out.relative_to(ROOT)}  ({df.height} rows, address dropped: {bool(drop)})")
    print("  parties:", sorted(df["party"].unique().to_list()))
    print("  columns:", df.columns)


def main() -> None:
    print("=== PROMOTE SIPO sandbox -> gold ===")
    promote_donations()
    # TODO (gated on other context): promote_expenses_candidates(), _categories(), _items()
    print("done. (expenses promotion deferred until Part-4 completes + consolidation)")


if __name__ == "__main__":
    main()
