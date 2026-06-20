"""Constituency -> Local-Authority crosswalk: pure-fn units + golden-artifact invariants.

Two layers, mirroring test/test_la_afs.py:
  * pure-function units for the report-parsing mappers (no PDF needed)
  * golden invariants on the COMMITTED artifact data/_meta/constituency_la_crosswalk.csv
    (43/43 constituencies, all 31 LAs, canonical strings, known mappings) — so a
    silent parser regression or a stale re-run is caught in CI without the 30 MB PDF.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from reference.ec_constituency_crosswalk_extract import (
    _CONSTITUENCIES,
    _LOCAL_AUTHORITIES,
    _canon_constituency,
    _county_to_la,
)

_CSV = Path(__file__).resolve().parents[1] / "data" / "_meta" / "constituency_la_crosswalk.csv"


# ── pure-function units ───────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("kind", "name", "expected"),
    [
        ("county", "Cork", "Cork County"),
        ("city", "Cork", "Cork City"),
        ("city", "Dublin", "Dublin City"),
        ("county", "Fingal", "Fingal"),
        ("county", "South Dublin", "South Dublin"),
        ("county", "Dún Laoghaire-Rathdown", "Dun Laoghaire-Rathdown"),
        ("county", "Limerick", "Limerick"),  # "city and county of Limerick" collapses to one LA
        ("county", "Clare", "Clare"),
        ("county", "Galway", "Galway County"),
        ("city", "Galway", "Galway City"),
    ],
)
def test_county_to_la(kind, name, expected):
    assert _county_to_la(kind, name) == expected


@pytest.mark.parametrize(
    ("heading", "expected"),
    [
        ("Carlow Kilkenny", "Carlow-Kilkenny"),
        ("Roscommon Galway", "Roscommon-Galway"),
        ("Cavan-Monaghan", "Cavan-Monaghan"),
        ("D�n Laoghaire", "Dún Laoghaire"),  # mojibake 'ú' from fitz
        ("Dublin Bay North", "Dublin Bay North"),
    ],
)
def test_canon_constituency(heading, expected):
    assert _canon_constituency(heading) == expected


# ── golden-artifact invariants ────────────────────────────────────────────────
@pytest.fixture(scope="module")
def rows() -> list[dict]:
    if not _CSV.exists():
        pytest.skip("crosswalk CSV not built — run reference/ec_constituency_crosswalk_extract.py --write")
    with _CSV.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_all_43_constituencies_present(rows):
    assert {r["constituency_name"] for r in rows} == set(_CONSTITUENCIES)


def test_all_31_local_authorities_present(rows):
    assert {r["local_authority"] for r in rows} == set(_LOCAL_AUTHORITIES)


def test_every_constituency_maps_to_at_least_one_la(rows):
    by_con: dict[str, int] = {}
    for r in rows:
        by_con[r["constituency_name"]] = by_con.get(r["constituency_name"], 0) + 1
    assert all(n >= 1 for n in by_con.values())
    assert len(by_con) == 43


def test_link_type_values_are_clean(rows):
    assert {r["link_type"] for r in rows} <= {"primary", "partial"}


@pytest.mark.parametrize(
    ("constituency", "expected_las"),
    [
        ("Carlow-Kilkenny", {"Carlow", "Kilkenny"}),
        ("Cavan-Monaghan", {"Cavan", "Monaghan"}),
        ("Clare", {"Clare"}),
        ("Cork North-Central", {"Cork City", "Cork County"}),
        ("Dublin Bay North", {"Dublin City", "Fingal"}),
        ("Dublin Rathdown", {"Dun Laoghaire-Rathdown"}),
        ("Sligo-Leitrim", {"Sligo", "Leitrim", "Donegal"}),  # Donegal is the sliver
        ("Longford-Westmeath", {"Longford", "Westmeath"}),
        ("Wicklow-Wexford", {"Wexford", "Wicklow"}),
    ],
)
def test_known_mappings(rows, constituency, expected_las):
    got = {r["local_authority"] for r in rows if r["constituency_name"] == constituency}
    assert got == expected_las


def test_sligo_leitrim_donegal_is_partial(rows):
    """The Donegal sliver in Sligo-Leitrim must be flagged 'partial' so the UI de-emphasises it."""
    donegal = [r for r in rows if r["constituency_name"] == "Sligo-Leitrim" and r["local_authority"] == "Donegal"]
    assert len(donegal) == 1
    assert donegal[0]["link_type"] == "partial"


def test_local_authority_strings_join_the_facts(rows):
    """Every LA string that has spending data must match a fact's council/publisher spelling.

    Integration-only (needs the silver/gold parquets). The crosswalk intentionally
    carries all 31 LAs incl. ones with no published spending yet (Carlow/Cavan/Kerry/
    Roscommon), so we only assert the OVERLAP joins — never that all 31 are in a fact.
    """
    pl = pytest.importorskip("polars")
    root = Path(__file__).resolve().parents[1]
    afs = root / "data" / "silver" / "parquet" / "la_afs_divisions.parquet"
    proc = root / "data" / "gold" / "parquet" / "procurement_payments_fact.parquet"
    if not afs.exists() or not proc.exists():
        pytest.skip("LA facts not present (integration mode only)")
    fact_las = set(pl.read_parquet(afs)["council"].unique().to_list())
    proc_df = pl.read_parquet(proc, columns=["publisher_name", "publisher_type"])
    fact_las |= set(proc_df.filter(pl.col("publisher_type") == "local_authority")["publisher_name"].unique().to_list())
    xwalk_las = {r["local_authority"] for r in rows}
    # the facts must not contain a council spelling absent from the crosswalk
    assert fact_las <= xwalk_las, f"fact councils missing from crosswalk: {fact_las - xwalk_las}"
