"""Curated buyer crosswalk — one public body across the award / TED / payments / AFS / live registers.

The same buyer is stored under different names in each register (awards say
"Limerick City and County Council", the payments register says "Limerick", AFS says
"Limerick") with no shared key. This module resolves any register string to the one
curated identity, FAIL-CLOSED: an unknown string returns ``None`` — callers must render
an honest "no match" note, never fuzzy-guess (mis-attributing one body's record to
another is how a brief becomes defamatory).

The crosswalk itself is a curated CSV (``data/_meta/procurement_publishers/buyer_xref.csv``,
90 bodies anchored on the payment publishers + the central purchasing bodies). Fuzzy
matching happened once at build time with per-row evidence; resolve time is exact lookup
on a conservative normalised key.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from typing import Any

from dail_tracker_core.db import PROJECT_ROOT

XREF_CSV = PROJECT_ROOT / "data" / "_meta" / "procurement_publishers" / "buyer_xref.csv"

_NAME_COLS = (
    "display_name",
    "payments_publisher_name",
    "etenders_name",
    "ted_buyer_name",
    "afs_council",
    "live_buyer",
)

# "city"/"county" are deliberately NOT stop tokens: Cork City Council and Cork County
# Council are different buyers and must key apart. Short payment names ("Limerick")
# resolve because every register variant of a row is indexed, not by token-stripping.
_STOP = {
    "council", "the", "of", "and", "plc", "clg", "ltd", "limited", "company", "by",
    "guarantee", "comhairle", "contae", "cathrach", "an", "na", "agus", "incorporating",
    "dac", "designated", "activity", "cuideachta",
}


def buyer_core(name: object) -> str:
    """Conservative buyer-name key: NFKD fold, drop parentheticals + stop tokens."""
    if name is None:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\betb\b", "education training board", s)
    s = re.sub(r"\bdept\b", "department", s)
    return " ".join(t for t in s.split() if t and t not in _STOP)


_CACHE: tuple[dict[str, str], dict[str, dict[str, str]]] | None = None


def _load() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    global _CACHE
    if _CACHE is None:
        index: dict[str, str] = {}
        by_id: dict[str, dict[str, str]] = {}
        with open(XREF_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                by_id[row["buyer_id"]] = row
                index.setdefault(row["buyer_id"].lower(), row["buyer_id"])
                for col in _NAME_COLS:
                    key = buyer_core(row.get(col, ""))
                    if key:
                        index.setdefault(key, row["buyer_id"])
        _CACHE = (index, by_id)
    return _CACHE


def resolve_buyer(query: object) -> dict[str, Any] | None:
    """Resolve any register string / buyer_id to the curated identity, else ``None``.

    The result carries ``match_tier`` and per-register names; callers fuse
    cross-register data only for ``match_tier == "curated_exact"`` rows and must
    degrade to an explicit gap note otherwise.
    """
    if query is None:
        return None
    index, by_id = _load()
    q = str(query).strip()
    buyer_id = q if q in by_id else index.get(q.lower()) or index.get(buyer_core(q))
    if not buyer_id:
        return None
    row = by_id[buyer_id]
    return {
        "buyer_id": row["buyer_id"],
        "display_name": row["display_name"],
        "buyer_type": row["buyer_type"],
        "match_tier": row["match_tier"],
        "registers": {
            "etenders": row["etenders_name"] or None,
            "ted": row["ted_buyer_name"] or None,
            "payments": row["payments_publisher_name"] or None,
            "afs": row["afs_council"] or None,
            "live": row["live_buyer"] or None,
        },
        "notes": row.get("notes") or None,
    }
