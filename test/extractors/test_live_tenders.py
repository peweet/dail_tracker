"""Contract for v_procurement_live_tenders — the live national tender pipeline (open opportunities)
over the SANDBOX etenders.gov.ie snapshot. Skips if the snapshot isn't present.

Guards the honesty rails: it is the PLANNED (pre-award) lifecycle tier — a buyer ESTIMATE that is
NEVER summed with awards or payments — and only genuinely-open opportunities are surfaced.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views


@pytest.fixture(scope="module")
def con():
    c = connect_with_views(["procurement_live_tenders.sql"])
    try:
        c.execute("SELECT 1 FROM v_procurement_live_tenders LIMIT 1")
    except duckdb.Error:
        pytest.skip("live-tenders sandbox snapshot not available")
    yield c
    c.close()


def _q(con, sql):
    return con.execute(sql).fetchone()[0]


def test_tier_is_planned_only(con):
    # The live pipeline is a NEW lifecycle stage BEFORE awarded — never AWARDED/COMMITTED/SPENT.
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE realisation_tier <> 'PLANNED'")
    assert bad == 0


def test_value_kind_is_estimate(con):
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE value_kind <> 'estimate_advertised'")
    assert bad == 0


def test_only_open_opportunities(con):
    # Every surfaced tender closes in the future (genuinely open), excluding closed + far-future DPS windows.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        "WHERE submission_deadline < CURRENT_DATE OR submission_deadline >= CURRENT_DATE + INTERVAL 3 YEAR",
    )
    assert bad == 0


def test_has_detail_link(con):
    # Each opportunity links back to its eTenders detail page (verifiability / drill-through).
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE detail_url IS NULL OR detail_url = ''")
    assert bad == 0


def test_summary_reconciles_to_open_count(con):
    detail = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders")
    summ = _q(con, "SELECT COALESCE(SUM(n_open_tenders), 0) FROM v_procurement_live_tenders_summary")
    assert detail == summ


def test_buyer_name_is_clean(con):
    # The eTenders grid appends an internal org id ("Cork County Council_424") and school roll
    # numbers ("Scoil Ailbhe - (18030I)") to the buyer name; the extractor strips both. A real
    # acronym/place-name in parens ("…(HIQA)", "…(Navan)") must survive — so we only forbid the
    # identifier forms (trailing _<digits> and " - (<digit-led code>)"), never all parentheses.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        r"WHERE regexp_matches(buyer, '_[0-9]+$') OR regexp_matches(buyer, '[-–]\s*\([0-9]')",
    )
    assert bad == 0


def test_buyer_org_id_is_preserved(con):
    # The stripped org id is not discarded — it is lifted into its own column as a stable join key,
    # and where present it is digits-only.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        "WHERE buyer_org_id IS NOT NULL AND NOT regexp_matches(buyer_org_id, '^[0-9]+$')",
    )
    assert bad == 0


# ── CPV detail-page parser (pure unit; no snapshot / no browser needed) ─────────────
@pytest.mark.parametrize(
    ("text", "code", "division"),
    [
        ("CPV Codes: 45000000 - Construction work", "45000000", "Construction"),
        ("Common Procurement Vocabulary (CPV): 72000000 IT", "72000000", "IT services"),
        ("CPV Codes:\n  48000000 - Software package", "48000000", "Software"),
        ("no cpv anywhere here", None, None),
        ("CPV 03000000 farm products", "03000000", "Other/Unknown"),  # unknown division → labelled, not dropped
    ],
)
def test_cpv_parser(text, code, division):
    from extractors.etenders_live_tenders_extract import _cpv_from_text

    assert _cpv_from_text(text) == (code, division)


def test_live_tenders_cpv_division_valid_when_present(con):
    """Once the snapshot is CPV-enriched, every non-null cpv_division must be a real label (never
    an empty string); skips cleanly on an un-enriched snapshot that has no such column/values."""
    try:
        bad = _q(
            con,
            "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE cpv_division IS NOT NULL AND cpv_division = ''",
        )
    except duckdb.Error:
        pytest.skip("snapshot not CPV-enriched yet (no cpv_division column)")
    assert bad == 0
