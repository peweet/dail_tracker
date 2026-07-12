"""Tests for the ADOPTED local-authority budgets fact (extractors/la_budgets_extract.py).

Mirrors test_la_afs.py: pure-function units always run; parquet invariants run against the
committed silver fact (skip cleanly if absent). The fact is the FOURTH money grain (BUDGETED)
— the invariants pin the never-union guards so a refactor can't silently drop them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))

from la_budgets_extract import (  # noqa: E402
    CANON_31,
    align_to_anchors,
    canon_council,
    column_anchors,
    to_num,
)

PARQUET = ROOT / "data" / "silver" / "parquet" / "la_budget_divisions.parquet"
AFS_PARQUET = ROOT / "data" / "silver" / "parquet" / "la_afs_divisions.parquet"


# ── pure functions ─────────────────────────────────────────────────────────────────────────────
def test_canon_council_plain_forms():
    assert canon_council("Carlow County Council") == "Carlow"
    assert canon_council("Cork City Council") == "Cork City"
    assert canon_council("Cork County Council") == "Cork County"
    assert canon_council("Limerick City and County Council") == "Limerick"
    assert canon_council("Dublin City Council") == "Dublin City"


def test_canon_council_abbreviations_across_editions():
    # observed variants: 2026 p28, 2025 p24, 2023 p27 — the abbreviation class that broke
    # the per-column gate until prefix-matching replaced suffix-stripping
    assert canon_council("South Dublin County Co") == "South Dublin"
    assert canon_council("Sth Dublin County Council") == "South Dublin"
    assert canon_council("Waterford City & Co Co") == "Waterford"
    assert canon_council("Waterford City and Co Co") == "Waterford"
    assert canon_council("DLR County Council") == "Dun Laoghaire-Rathdown"
    assert canon_council("Dun Laoghaire Rathdown Cou Co") == "Dun Laoghaire-Rathdown"
    assert canon_council("Dún Laoghaire-Rathdown County Council") == "Dun Laoghaire-Rathdown"


def test_canon_council_rejects_non_councils():
    assert canon_council("Total") is None
    assert canon_council("Regional Assemblies") is None
    assert canon_council("Less: Inter Local Authority Contributions") is None
    assert canon_council("City & County Councils Agency & Recoupable") is None
    # a bare county token buried in prose must not match
    assert canon_council("Housing Grants HAP Programme Total For Service Division") is None


def test_to_num():
    assert to_num("86,074,270") == 86074270.0
    assert to_num("0") == 0.0
    assert to_num("(1,234)") == -1234.0
    assert to_num("€") is None
    assert to_num("Expenditure") is None
    assert to_num(",") is None  # the comma-only cell that once crashed the AFS run


def test_align_to_anchors_zero_fills_empty_cells():
    # the 2020-edition failure mode: a printed row with one truly EMPTY cell (7 of 8 values)
    anchors = [100.0, 200.0, 300.0, 400.0]
    vals = [(101.0, 5.0), (199.0, 6.0), (401.0, 7.0)]  # nothing near 300 → column 2 absent
    assert align_to_anchors(vals, anchors) == [5.0, 6.0, 0.0, 7.0]
    # a value far from every anchor invalidates the row rather than guessing
    assert align_to_anchors([(150.0, 9.0)], anchors) is None
    # two values claiming one column invalidates the row
    assert align_to_anchors([(99.0, 1.0), (102.0, 2.0)], anchors) is None


def test_column_anchors_clusters_right_edges():
    rows = [[(100.0, 1.0), (200.0, 2.0)], [(102.0, 3.0), (198.0, 4.0)], [(99.0, 5.0)]]
    anchors = column_anchors(rows)
    assert len(anchors) == 2
    assert abs(anchors[0] - 100.33) < 1 and abs(anchors[1] - 199.0) < 1


# ── parquet invariants (golden fact) ───────────────────────────────────────────────────────────
pl = pytest.importorskip("polars")
needs_fact = pytest.mark.skipif(not PARQUET.exists(), reason="budget fact parquet not present")


@pytest.fixture(scope="module")
def fact():
    return pl.read_parquet(PARQUET)


@needs_fact
def test_fourth_grain_guards_pinned(fact):
    # every row BUDGETED / budget_adopted / never summable — the never-union rail
    assert (fact["realisation_tier"] == "BUDGETED").all()
    assert (fact["value_kind"] == "budget_adopted").all()
    assert (~fact["value_safe_to_sum"]).all()


@needs_fact
def test_councils_and_years(fact):
    assert set(fact["council"].unique()) == CANON_31
    assert fact["year"].min() >= 2019
    assert fact["year"].max() >= 2026


@needs_fact
def test_no_duplicate_keys(fact):
    assert fact.group_by("council", "year", "division").len().filter(pl.col("len") > 1).height == 0


@needs_fact
def test_latest_year_complete_grid(fact):
    latest = fact.filter(pl.col("year") == fact["year"].max())
    assert latest.height == 31 * 8  # every council, every division


@needs_fact
def test_values_sane(fact):
    assert fact.filter(pl.col("expenditure_adopted") < 0).height == 0
    # county budgets run tens of millions to ~€1.5bn (Dublin City); a division above €2bn
    # would mean a column mis-pick (the order-number-as-amount class of bug)
    assert fact["expenditure_adopted"].max() < 2_000_000_000


@needs_fact
@pytest.mark.skipif(not AFS_PARQUET.exists(), reason="AFS fact parquet not present")
def test_divisions_join_afs_taxonomy(fact):
    afs = pl.read_parquet(AFS_PARQUET)
    assert set(fact["division"].unique()) <= set(afs["division"].unique())
    joined = fact.join(afs, on=["council", "year", "division"], how="inner")
    assert joined.height > 500  # budget-vs-actual has real overlap to compare
