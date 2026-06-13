"""DATA-QUALITY contract for the procurement GOLD tables.

These run against the real committed gold (data/gold/parquet/procurement_*.parquet),
not a synthetic fixture — they lock the invariants the extractor promises and guard
the corruption fixes made 2026-06-03 (entity-split, literal-"NULL" Tender ID, the
sub-€1 value floor) against a silent regression on the next regeneration.

Each suite SKIPS if its parquet is absent (regenerable; same pattern as the repo's
silver/gold tests). The view-level value semantics are tested separately, against a
synthetic fixture, in test/test_sql_views.py.

Run:
    ./.venv/Scripts/python.exe -m pytest test/test_procurement_gold_quality.py -q
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
AWARDS = ROOT / "data" / "gold" / "parquet" / "procurement_awards.parquet"
CRO = ROOT / "data" / "gold" / "parquet" / "procurement_supplier_cro_match.parquet"
OVERLAP = ROOT / "data" / "gold" / "parquet" / "procurement_lobbying_overlap.parquet"

# Bare company suffixes that only appear when a name with '&' was wrongly split on
# the ';' inside '&amp;' (the entity-split bug). If any survive, the fix regressed.
_FRAGMENT_SUFFIXES = {
    "Sons Ltd",
    "Sons",
    "Sons Limited",
    "Son Ltd",
    "Son Solicitors",
    "Company",
    "Co Ltd",
    "Co. Ltd",
    "Co. Limited",
    "Associates",
    "Partners",
}

_SUPPLIER_CLASSES = {"company", "sole_trader_or_individual", "foreign_company", "public_body"}
_VALUE_KINDS = {"contract_award_value", "framework_or_dps_ceiling", "framework_call_off"}


def _load(path: Path) -> pl.DataFrame:
    if not path.exists():
        pytest.skip(f"{path.name} not present — run pipeline.py (procurement chain) first")
    return pl.read_parquet(path)


# ---------------------------------------------------------------------------
# AWARDS
# ---------------------------------------------------------------------------


def test_tender_id_never_literal_null():
    """Literal string 'NULL' must have been normalised to a real null (2026-06-03 fix)."""
    aw = _load(AWARDS)
    assert (aw["Tender ID"] == "NULL").sum() == 0


def test_no_undecoded_html_entities_in_supplier():
    """'&amp;' in a supplier name means entities were not decoded before splitting."""
    aw = _load(AWARDS)
    assert aw.filter(pl.col("supplier").str.contains("&amp;", literal=True)).height == 0


def test_no_entity_split_fragment_suppliers():
    """No supplier should be a bare suffix like 'Sons Ltd' / 'Co. Limited' — those
    only appear when an '&'-name was fragmented (the entity-split bug)."""
    aw = _load(AWARDS)
    frags = aw.filter(pl.col("supplier").is_in(list(_FRAGMENT_SUFFIXES)))
    assert frags.height == 0, f"fragment suppliers present: {sorted(set(frags['supplier'].to_list()))}"


def test_supplier_class_and_value_kind_in_known_sets():
    aw = _load(AWARDS)
    assert set(aw["supplier_class"].unique().to_list()) <= _SUPPLIER_CLASSES
    assert set(aw["value_kind"].unique().to_list()) <= _VALUE_KINDS


def test_org_form_firms_reclassified_to_company():
    """Suffix-less firms carrying an organisation-form word (Bros / & Sons / Solicitors / Partners /
    Contractors …) are reclassified out of sole_trader_or_individual — they are firms, not lone
    individuals, so they must be matchable, rankable and drillable like any company (mirrors the
    payments consolidation). None may remain a sole trader."""
    aw = _load(AWARDS)
    org = r"(?i)\b(bros|brothers|sons|solicitors|partners|contractors|associates|developments|enterprises|industries)\b"
    stragglers = aw.filter(
        (pl.col("supplier_class") == "sole_trader_or_individual") & pl.col("supplier").str.contains(org)
    )
    assert stragglers.height == 0, (
        f"org-form firms left as sole traders: {sorted(set(stragglers['supplier'].to_list()))[:10]}"
    )


def test_value_safe_to_sum_is_a_strict_clean_subset():
    """Every value_safe_to_sum row must be a standalone, verifiable, plausible award:
    a contract-award value (not a ceiling), not shared across co-suppliers, with a
    real Tender ID and a value at or above the €1 plausibility floor."""
    aw = _load(AWARDS)
    safe = aw.filter(pl.col("value_safe_to_sum"))
    assert safe.height > 0
    assert (safe["value_kind"] == "contract_award_value").all()
    assert (~safe["is_framework_or_dps"]).all()
    assert (~safe["value_shared_across_suppliers"]).all()
    assert safe["Tender ID"].null_count() == 0
    assert safe["value_eur"].null_count() == 0
    assert (safe["value_eur"] >= 1.0).all(), "a sub-€1 value slipped past the plausibility floor"


def test_safe_sum_is_far_below_the_naive_sum():
    """The headline guard: framework ceilings + repeated multi-supplier rows make the
    naive total an order of magnitude too big. The safe sum must be a small fraction."""
    aw = _load(AWARDS)
    safe_total = aw.filter(pl.col("value_safe_to_sum"))["value_eur"].sum()
    naive_total = aw["value_eur"].sum()
    assert safe_total > 0
    assert safe_total < naive_total * 0.1, "safe-to-sum total is implausibly close to the naive total"


# ---------------------------------------------------------------------------
# CRO MATCH
# ---------------------------------------------------------------------------


def test_cro_match_confidence_and_method_consistent():
    cro = _load(CRO)
    assert (cro["match_confidence"] >= 0.0).all()
    assert (cro["match_confidence"] <= 1.0).all()
    # A no_match row carries no company number; a matched row does.
    no_match = cro.filter(pl.col("match_method") == "no_match")
    assert no_match["company_num"].null_count() == no_match.height
    matched = cro.filter(pl.col("match_method") != "no_match")
    assert matched["company_num"].null_count() == 0


# ---------------------------------------------------------------------------
# LOBBYING OVERLAP
# ---------------------------------------------------------------------------


def test_overlap_side_and_value_invariants():
    ov = _load(OVERLAP)
    assert set(ov["lobby_side"].unique().to_list()) <= {"registrant", "client"}
    assert (ov["awarded_value_safe_eur"] >= 0.0).all()
    assert ov["supplier_norm"].null_count() == 0
