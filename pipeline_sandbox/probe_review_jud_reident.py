"""probe_review_jud_reident.py — READ-ONLY re-identification / leakage surface audit.

Looks at the PUBLISHED gold cases layer only. Two questions:
  1. Non-'v' rows: 'IN THE MATTER OF ...' and single-side entries — does the
     anonymiser still reduce a natural person, or can a full surname survive
     because it was treated as an org (over-broad ORG_KEYS) or had no 'v'?
  2. Org-side leakage: a side classed as org is kept VERBATIM. Could a verbatim
     org side actually carry an individual's name (e.g. 'John Murphy trading as
     Murphy Plant Hire Limited' -> kept in clear because of 'limited')?

Prints aggregate counts + a handful of already-published (gold) example strings
so a reviewer can judge. No raw/sandbox data is read.
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

# personal-name connectors that, if present inside a kept org side, hint a real
# person's name is riding along verbatim.
PERSON_HINTS = re.compile(r"\b(trading as|t/a|practising as|sole trader|a firm|"
                          r"deceased|in the estate of|formerly)\b", re.I)
NAME_TOKEN = re.compile(r"\b[A-Z][a-z]{3,}\b")


def _is_org(side: str) -> bool:
    return any(k in side.lower() for k in ORG_KEYS)


def main() -> int:
    if not GOLD.exists():
        print("GOLD MISSING")
        return 1
    df = pl.read_parquet(GOLD)
    vals = [v for v in df["case_anonymised"].to_list() if v]

    no_v = [v for v in vals if " v " not in v]
    print(f"total={len(vals)}  no_v_rows={len(no_v)}")

    # (1) non-v rows that still contain >=2 titlecase name tokens AND are not clearly org
    nonv_titlecase = [v for v in no_v if len(NAME_TOKEN.findall(v)) >= 2 and not _is_org(v)]
    print(f"non_v_multi_titlecase_nonorg={len(nonv_titlecase)}")
    for v in nonv_titlecase[:12]:
        print(f"  NONV<{v!r}>")

    # (2) org-classed sides carrying a person hint (trading as / estate of / deceased)
    person_in_org = [v for v in vals if PERSON_HINTS.search(v)]
    print(f"rows_with_person_hint_phrases={len(person_in_org)}")
    for v in person_in_org[:12]:
        print(f"  PERSONHINT<{v!r}>")

    # (3) very long verbatim sides (kept org) — eyeball for residual personal data
    long_sides = []
    for v in vals:
        for side in re.split(r"\s+v\s+", v):
            if len(side) > 55:
                long_sides.append(side.strip())
    print(f"long_kept_sides>55chars={len(long_sides)}")
    for s in long_sides[:8]:
        print(f"  LONG<{s!r}>")

    return 0


if __name__ == "__main__":
    sys.exit(main())
