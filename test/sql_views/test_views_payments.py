"""
SQL view contract tests — payments views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import pytest

from ._view_test_helpers import (
    GOLD_PARQUET_DIR,
    _con,
    _load,
    _result,
    _skip_missing,
)

# ---------------------------------------------------------------------------
# PAYMENTS VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_payments_base_executes():
    _skip_missing(GOLD_PARQUET_DIR / "payments_fact.parquet")
    con = _con()
    con.execute(_load("payments_base.sql"))
    result = _result(con, "v_payments_base")
    assert len(result) > 0


# The payments page is a dependency chain rooted at v_payments_base:
#   base → member_detail
#   base → summary
#   base → yearly_evolution → alltime_ranking → alltime_summary
# Each view reads its parent view, not parquet, so the test must CREATE every
# ancestor on the same connection before the leaf. The real source parquet is
# payments_full_psa.parquet (+ the Seanad sibling) — note the existing
# test_v_payments_base skip guard names payments_fact.parquet, which is the
# pre-PSA file; these chain tests guard on the file the view actually reads.
_PAYMENTS_SOURCE = GOLD_PARQUET_DIR / "payments_full_psa.parquet"


def _payments_chain(con, *leaves: str) -> None:
    """CREATE v_payments_base then each named leaf file, in order."""
    con.execute(_load("payments_base.sql"))
    for leaf in leaves:
        con.execute(_load(leaf))


@pytest.mark.sql
def test_v_payments_member_detail_executes():
    """Per-transaction audit trail. Locks the columns the member panel and the
    member-overview payments card read; unique_member_code is the cross-page key."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_member_detail.sql")
    result = _result(con, "v_payments_member_detail")
    for col in (
        "unique_member_code",
        "member_name",
        "date_paid",
        "narrative",
        "amount_num",
        "payment_year",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_payments_member_detail"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_summary_executes():
    """Dataset-level hero row. A single row of dataset totals."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_summary.sql")
    result = _result(con, "v_payments_summary")
    for col in ("members_count", "payment_count", "total_paid", "first_year", "last_year"):
        assert col in result.columns, f"Expected column '{col}' in v_payments_summary"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_yearly_evolution_executes():
    """Per-(member, year) aggregate with the pre-computed window columns the
    contract forbids Streamlit from computing (rank_high, year_total_paid,
    member_alltime_total). A rename here silently empties the yearly cards."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_yearly_evolution.sql")
    result = _result(con, "v_payments_yearly_evolution")
    for col in (
        "member_name",
        "payment_year",
        "total_paid",
        "rank_high",
        "year_total_paid",
        "member_alltime_total",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_payments_yearly_evolution"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_alltime_ranking_executes():
    """All-time (since-2020) ranking. Reads yearly_evolution, so the full chain
    base → yearly_evolution → alltime_ranking must be created in order."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_yearly_evolution.sql", "payments_zz_alltime_ranking.sql")
    result = _result(con, "v_payments_alltime_ranking")
    for col in (
        "member_name",
        "unique_member_code",
        "total_paid_since_2020",
        "rank_high",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_payments_alltime_ranking"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_alltime_summary_executes():
    """Single-row hero totals for the Rankings view; the deepest leaf in the
    chain (base → yearly_evolution → alltime_ranking → alltime_summary)."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(
        con,
        "payments_yearly_evolution.sql",
        "payments_zz_alltime_ranking.sql",
        "payments_zz_alltime_summary.sql",
    )
    result = _result(con, "v_payments_alltime_summary")
    for col in ("total_paid_since_2020", "member_count", "avg_per_td_since_2020"):
        assert col in result.columns, f"Expected column '{col}' in v_payments_alltime_summary"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_sources_executes():
    """Source-link stub view. Reads no parquet (SELECT over a literal), so it
    needs no data and no skip — if it stops compiling, the page footer breaks."""
    con = _con()
    con.execute(_load("payments_sources.sql"))
    result = _result(con, "v_payments_sources")
    for col in ("source_url", "source_summary"):
        assert col in result.columns, f"Expected column '{col}' in v_payments_sources"
    assert len(result) > 0
