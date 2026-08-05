"""
SQL view contract tests — corporate views (Iris Oifigiuil distress / register
notices).

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import pytest

from ._view_test_helpers import (
    DATA_DIR,
    GOLD_PARQUET_DIR,
    _assert_cols,
    _con,
    _load,
    _result,
    _skip_missing,
    _src,
)

# ---------------------------------------------------------------------------
# CORPORATE VIEWS  (Iris Oifigiúil distress / register notices)
# ---------------------------------------------------------------------------
#
# Privacy contract: personal insolvency (named-individual bankruptcies) is
# excluded upstream by policy. These tests lock the column shape the Corporate
# page reads — a drift that re-exposed a personal-bankruptcy field, or dropped
# the entity columns, must fail loudly. See [[feedback_personal_insolvency_privacy]].


@pytest.mark.sql
def test_v_corporate_notices_executes():
    _skip_missing(GOLD_PARQUET_DIR / "corporate_notices.parquet")
    con = _con()
    con.execute(_load("corporate_corporate_notices.sql"))
    result = _result(con, "v_corporate_notices")
    for col in (
        "notice_ref",
        "issue_date",
        "notice_category",
        "entity_name",
        "display_title",
        "brand_mentions",
        "parent_fund_mentions",
        # display/derived columns graduated out of the Corporate page (2026-07
        # logic-firewall audit) — the page renders these, it must not re-derive them
        "year",
        "display_ref",
        "parent_mentions_str",
        "is_receivership_shaped",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_corporate_notices"
    assert len(result) > 0


@pytest.mark.sql
def test_v_corporate_notices_display_ref_unique_and_nonnull():
    """display_ref is the feed's row key: notice_ref where present, else a stable
    'row-N' fallback for the null-ref split fragments — never null, never duplicated
    among the fallbacks."""
    _skip_missing(*_src("data/gold/parquet/corporate_notices_enriched.parquet"))
    con = _con()
    con.execute(_load("corporate_corporate_notices.sql"))
    n_null = con.execute(
        "SELECT count(*) FROM v_corporate_notices WHERE display_ref IS NULL OR display_ref = ''"
    ).fetchone()[0]
    assert n_null == 0
    n_dupe = con.execute(
        "SELECT count(*) - count(DISTINCT display_ref) FROM v_corporate_notices WHERE display_ref LIKE 'row-%'"
    ).fetchone()[0]
    assert n_dupe == 0


@pytest.mark.sql
def test_v_corporate_notices_receivership_shaped_supersets_flag():
    """is_receivership_shaped (subtype OR the appointment-of-receiver wording) must
    at minimum cover every subtype-tagged receivership — it's the firm view's
    receivership subset definition."""
    _skip_missing(*_src("data/gold/parquet/corporate_notices_enriched.parquet"))
    con = _con()
    con.execute(_load("corporate_corporate_notices.sql"))
    leaked = con.execute(
        "SELECT count(*) FROM v_corporate_notices WHERE notice_subtype = 'receivership' AND NOT is_receivership_shaped"
    ).fetchone()[0]
    assert leaked == 0
    assert con.execute("SELECT count(*) FROM v_corporate_notices WHERE is_receivership_shaped").fetchone()[0] > 0


@pytest.mark.sql
def test_v_corporate_firm_fund_counts_executes():
    """Fund↔firm co-mention counts behind the ?firm= landing (graduated out of the
    page's _explode_fund_counts). Grain: one row per (firm, parent); n_recv (the
    receivership-shaped series) can never exceed n_all."""
    _skip_missing(*_src("data/gold/parquet/corporate_notices_enriched.parquet"))
    con = _con()
    con.execute(_load("corporate_corporate_notices.sql"))
    con.execute(_load("corporate_zz_firm_fund_counts.sql"))
    result = _result(con, "v_corporate_firm_fund_counts")
    _assert_cols(result, "firm", "parent", "n_recv", "n_all")
    bad = con.execute(
        "SELECT count(*) FROM v_corporate_firm_fund_counts"
        " WHERE n_recv > n_all OR n_all < 1 OR firm IS NULL OR parent IS NULL OR parent = ''"
    ).fetchone()[0]
    assert bad == 0
    n_dupe = con.execute(
        "SELECT count(*) FROM (SELECT firm, parent FROM v_corporate_firm_fund_counts"
        " GROUP BY firm, parent HAVING count(*) > 1)"
    ).fetchone()[0]
    assert n_dupe == 0


@pytest.mark.sql
def test_v_corporate_firm_fund_counts_matches_notice_presence():
    """A (firm, parent) n_all count equals the number of notices where the curated
    firm tag and the parent mention co-occur — the view must not inflate on
    repeated mentions within one notice (list_distinct contract)."""
    _skip_missing(*_src("data/gold/parquet/corporate_notices_enriched.parquet"))
    con = _con()
    con.execute(_load("corporate_corporate_notices.sql"))
    con.execute(_load("corporate_zz_firm_fund_counts.sql"))
    row = con.execute(
        "SELECT firm, parent, n_all FROM v_corporate_firm_fund_counts ORDER BY n_all DESC LIMIT 1"
    ).fetchone()
    if row is None:
        pytest.skip("no firm/fund co-mentions in current gold")
    firm, parent, n_all = row
    direct = con.execute(
        "SELECT count(*) FROM v_corporate_notices"
        " WHERE list_contains(receiver_firms, ?) AND list_contains(parent_fund_mentions, ?)",
        [firm, parent],
    ).fetchone()[0]
    assert n_all == direct


@pytest.mark.sql
def test_v_corporate_brand_alias_groups_executes():
    """Methodology-expander rollup of the curated alias CSV (graduated out of the
    page's groupby): one row per (parent_fund, fund_type), brands sorted + joined,
    and no brand lost relative to the base alias view."""
    _skip_missing(*_src("data/_meta/loan_book_fund_aliases.csv"))
    con = _con()
    con.execute(_load("corporate_brand_aliases.sql"))
    con.execute(_load("corporate_zz_brand_alias_groups.sql"))
    result = _result(con, "v_corporate_brand_alias_groups")
    _assert_cols(result, "parent_fund", "fund_type", "brands", "notes_concat")
    n_groups_brands, n_aliases = con.execute(
        "SELECT (SELECT sum(len(string_split(brands, ', '))) FROM v_corporate_brand_alias_groups),"
        " (SELECT count(*) FROM v_corporate_brand_aliases)"
    ).fetchone()
    assert n_groups_brands == n_aliases


@pytest.mark.sql
def test_v_corporate_cbi_notice_match_executes():
    """Per-notice CBI-register badge lookup. Both CBI views read the sandbox
    cross-reference parquet (not v_corporate_notices), so they stand alone."""
    _skip_missing(DATA_DIR / "sandbox" / "parquet" / "cbi_xref_corporate_notices.parquet")
    con = _con()
    con.execute(_load("corporate_cbi_distress.sql"))
    result = _result(con, "v_corporate_cbi_notice_match")
    for col in (
        "notice_ref",
        "entity_name",
        "entity_norm",
        "registers",
        "ref_nos",
        "primary_register",
        "primary_ref_no",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_corporate_cbi_notice_match"
    assert len(result) > 0


@pytest.mark.sql
def test_v_corporate_cbi_repeat_distress_executes():
    """Per-firm repeat-distress aggregate. corporate_cbi_distress.sql creates
    BOTH CBI views; load it once and query the second. The HAVING gate can
    legitimately yield zero rows on a sparse dataset, so this asserts the column
    contract only — it does not require rows."""
    _skip_missing(DATA_DIR / "sandbox" / "parquet" / "cbi_xref_corporate_notices.parquet")
    con = _con()
    con.execute(_load("corporate_cbi_distress.sql"))
    result = _result(con, "v_corporate_cbi_repeat_distress")
    for col in (
        "entity_norm",
        "entity_name",
        "n_notices_total",
        "n_receivership",
        "n_distress",
        "n_routine",
        "primary_register",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_corporate_cbi_repeat_distress"
