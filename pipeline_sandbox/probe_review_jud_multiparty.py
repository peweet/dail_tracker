"""probe_review_jud_multiparty.py — quantify the multi-party / consolidated leak.

Two structural blind spots in extractors/legal_diary_extract.py:anonymise():
  A. split on the FIRST ' v ' only (maxsplit=1) -> any second 'v' (CONSOLIDATED
     WITH X -v- Y) leaves Y un-anonymised;
  B. _is_org() is whole-side -> if a multi-party side mixes named individuals
     with one org token (e.g. '... and Mayo County Council'), the WHOLE side is
     kept verbatim, exposing the individuals.

Counts rows in published gold that hit A or B. Read-only.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold" / "parquet" / "judicial_legal_diary_cases.parquet"

ORG_KEYS = ["limited", " ltd", "d.a.c", " dac", " plc", "company", "bank", "insurance",
            "minister", "ireland", "attorney general", "commissioner", "council", "authority",
            "agency", "board", "revenue", " hse", "an garda", "designated activity",
            "university", "college", "credit union", "society", "fund", "holdings",
            "dpp", "director of public prosecutions", "people at the suit", "state"]


def _is_org(side: str) -> bool:
    return any(k in side.lower() for k in ORG_KEYS)


def main() -> int:
    df = pl.read_parquet(GOLD)
    vals = [v for v in df["case_anonymised"].to_list() if v]

    # A. a second ' v ' / '-v-' after the first split survives
    second_v = [v for v in vals if len(re.findall(r"\s-?v-?\s", v)) >= 2]
    print(f"rows_with_second_v_unanonymised={len(second_v)} / {len(vals)}")

    # B. an org-classed side that ALSO contains ' and ' joining multiple parties,
    #    where at least one ' and '-delimited chunk is itself NOT an org (an individual).
    mixed = []
    for v in vals:
        for side in re.split(r"\s+v\s+", v):
            if _is_org(side) and " and " in side.lower():
                chunks = re.split(r"\s+and\s+", side, flags=re.I)
                nonorg_named = [c for c in chunks
                                if not _is_org(c) and len(re.findall(r"\b[A-Z][a-z]{3,}\b", c)) >= 2]
                if nonorg_named:
                    mixed.append((v, nonorg_named))
                    break
    print(f"rows_with_named_individual_riding_in_org_side={len(mixed)}")
    for v, names in mixed[:10]:
        print(f"  LEAK names={names}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
