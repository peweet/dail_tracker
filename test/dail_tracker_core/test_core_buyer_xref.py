"""Core tests for the curated buyer crosswalk (data/_meta/procurement_publishers/buyer_xref.csv)."""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dail_tracker_core.buyer_xref import _NAME_COLS, XREF_CSV, buyer_core, resolve_buyer  # noqa: E402


@pytest.fixture(scope="module")
def rows() -> list[dict[str, str]]:
    if not XREF_CSV.exists():
        pytest.skip("buyer_xref.csv not present on this machine")
    with open(XREF_CSV, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_loads_and_row_shape(rows):
    assert len(rows) >= 85
    required = {"buyer_id", "display_name", "buyer_type", "match_tier", "needs_review"}
    assert required <= set(rows[0].keys())
    assert len({r["buyer_id"] for r in rows}) == len(rows), "buyer_id must be unique"


def test_no_core_key_collisions_across_buyers(rows):
    """Two different buyers must never share a normalised name key — a collision here
    silently mis-attributes one body's record to another (the defamation failure mode)."""
    owners: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        for col in _NAME_COLS:
            key = buyer_core(r.get(col, ""))
            if key:
                owners[key].add(r["buyer_id"])
    collisions = {k: v for k, v in owners.items() if len(v) > 1}
    assert not collisions, f"colliding keys: {collisions}"


def test_distinct_city_and_county_councils_stay_apart():
    cork_city = resolve_buyer("Cork City Council")
    cork_county = resolve_buyer("Cork County Council")
    if cork_city is None or cork_county is None:
        pytest.skip("Cork rows not in the crosswalk")
    assert cork_city["buyer_id"] != cork_county["buyer_id"]


def test_limerick_three_register_fusion():
    """The live-scenario defect of 2026-07-13: payments say 'Limerick', awards say
    'Limerick City and County Council' — both must resolve to one identity."""
    ids = {
        q: (resolve_buyer(q) or {}).get("buyer_id")
        for q in ("Limerick", "Limerick City and County Council", "ie_la_limerick")
    }
    assert len(set(ids.values())) == 1 and None not in ids.values(), ids
    r = resolve_buyer("Limerick City and County Council")
    assert r["registers"]["payments"] == "Limerick"
    assert r["match_tier"] == "curated_exact"


def test_dublin_city_aliases_consistent():
    ids = {
        q: (resolve_buyer(q) or {}).get("buyer_id")
        for q in ("Dublin City Council", "Dublin City", "ie_la_dublin_city")
    }
    assert len(set(ids.values())) == 1 and None not in ids.values(), ids


def test_fail_closed_on_unknown():
    assert resolve_buyer("Hogwarts County Council") is None
    assert resolve_buyer(None) is None
    assert resolve_buyer("") is None


def test_defunct_council_name_does_not_hijack_modern_entity(rows):
    """'Limerick City Council' (defunct, pre-2014) is not an indexed alias; it must
    either resolve to the merged council via its own tokens or fail closed — it must
    never resolve to a DIFFERENT buyer."""
    r = resolve_buyer("Limerick City Council")
    if r is not None:
        assert r["buyer_id"] == "ie_la_limerick"
