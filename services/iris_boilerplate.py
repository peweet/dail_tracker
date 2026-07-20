"""Shared Iris Oifigiúil boilerplate signatures — ONE home for "this string is
gazette boilerplate, not an entity name".

Consumers (keep in sync by importing, never by copying):
  - iris/corporate_notices_enrichment.py — the A5 coverage gate's junk counter
  - utility/pages_code/corporate.py      — the card display fallback gate

Two related lists deliberately NOT merged here (different granularity):
  - the ETL's ``bad_pat`` (iris/iris_oifigiuil_etl_polars.py) filters candidate
    LINES during extraction and carries the curated liquidator-firm deny list;
  - the appointments page's ``_BODY_FRAGMENT_RE`` judges parsed body strings.
Both reference this module in comments so the four lists stay discoverable.

2026-07-18 assessment context: 23% of corporate gold entity names matched these
patterns (stable since 2012, creeping to ~29% in 2024-26) because the extractor
fell back to the pipe-joined title. The extractor now emits null instead; these
patterns remain as the display/measurement gate for rows extracted before the
fix and for future parser misses.
"""

from __future__ import annotations

# Case-insensitive-ready (callers apply their own casing convention: the
# coverage gate uppercases input, the UI matches against .str.upper()).
NOTICE_NAME_JUNK_PATTERNS: list[str] = [
    # \\b variant, not just "HEREBY": the gazette also writes "Notice is
    # herewith/further given…" — seen leaking through the display gate on the
    # 2026-07-17 issue.
    r"NOTICE IS\b",
    "ABOVE NAMED",
    "IN THE MATTER",
    "COMPANIES ACT",
    "ICAV ACT",
    "COLLECTIVE ASSET",
    "STRIKING.OFF",  # dot: the gazette writes both "striking off" and "striking-off"
    "STRIKE.OFF",
    r"\bTHE ICAV\b",  # definite article = back-reference boilerplate, never a name
    r"^UNLESS\b",
    # A government department is never the company a notice concerns — it is
    # the appointer line of a receivership notice surfacing as display_title.
    r"^DEPARTMENT OF\b",
    # Sentence fragments / court boilerplate seen as titles in the 2026-07-17
    # visual audit ("it was ordered that … be wound up under the").
    "^IT WAS ORDERED",
    "WOUND UP UNDER",
    "^THE HIGH COURT",
    # Bare company-form tokens: a wrapped name's continuation line alone.
    "^LIMITED$",
    "^UNLIMITED$",
    "^LIMITED COMPANY$",
    # Confirmed parse fragments by data check (display_title = 'THE HIGH
    # COURT'): two-word tails of a name wrapped mid-phrase.
    "^CONSTRUCTION LIMITED$",
    "^IRELAND LIMITED$",
]

NOTICE_NAME_JUNK_RE: str = "|".join(NOTICE_NAME_JUNK_PATTERNS)
