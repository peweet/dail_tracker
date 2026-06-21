"""Promote the VETTED committee-evidence silver tables to the gold layer.

The committee-evidence extractor (committee_witnesses_extract.py) writes three
silver tables under data/silver/committee_evidence/:
  committee_meetings.parquet         one row per (committee, date): topics + counts
  committee_witnesses.parquet        one row per (committee, date, witness org)
  committee_witness_persons.parquet  one row per (committee, date, witness person)

This step is the sandbox->gold promotion
([[feedback_pipeline_changes_data_anchored_promotion]]): it reads those vetted
tables and writes the gold parquet the SQL view (v_committee_meetings) — and
therefore the Committees page meeting-history section — reads.

There is no transform here beyond a row-floor guard: the meeting spine is the
extractor's product, committee identity is reconciled at extraction time, and the
crosswalk to the membership committee name is done view-side (casefold). Raw
provenance (the API committee_name, the transcript URL) is carried unchanged.

Run (after the extractor): .venv/Scripts/python.exe extractors/committee_evidence_promote_gold.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from config import SILVER_DIR
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

SILVER = SILVER_DIR / "committee_evidence"
GOLD = Path("data/gold/parquet")

# (silver name, gold name, row floor). Floors are data-anchored to the current
# 2-committee scope (PAC + Housing); they refuse to atomically replace good gold
# with a truncated harvest, and rise as the committee scope widens.
TABLES = [
    ("committee_meetings.parquet", "committee_meetings.parquet", 20),
    ("committee_witnesses.parquet", "committee_witnesses.parquet", 20),
    ("committee_witness_persons.parquet", "committee_witness_persons.parquet", 50),
]


def main() -> int:
    setup_standalone_logging("committee_evidence_promote_gold")
    GOLD.mkdir(parents=True, exist_ok=True)

    for silver_name, gold_name, floor in TABLES:
        src = SILVER / silver_name
        if not src.exists():
            raise SystemExit(f"promote: missing silver input {src} — run committee_witnesses_extract.py first.")
        df = pl.read_parquet(src)
        save_parquet(df, GOLD / gold_name, min_rows=floor)
        log.info("GOLD promoted: %s (%d rows)", gold_name, df.height)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
