"""
SQL view contract tests — attendance views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""


import pytest

from ._view_test_helpers import (
    GOLD_PARQUET_DIR,
    SILVER_DIR,
    _con,
    _load,
    _skip_missing,
    _result,
    _src,
    _assert_cols,
)


# ---------------------------------------------------------------------------
# ATTENDANCE VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_attendance_member_summary_executes():
    _skip_missing(
        SILVER_DIR / "aggregated_td_tables.csv",
        SILVER_DIR / "flattened_members.csv",
    )
    con = _con()
    con.execute(_load("attendance_member_summary.sql"))
    result = _result(con, "v_attendance_member_summary")
    assert "member_name" in result.columns
    assert "attendance_rate" in result.columns
    assert "party_name" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_summary_executes():
    _skip_missing(SILVER_DIR / "aggregated_td_tables.csv")
    con = _con()
    con.execute(_load("attendance_summary.sql"))
    result = _result(con, "v_attendance_summary")
    assert "members_count" in result.columns
    assert "sitting_count" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_member_year_summary_executes():
    _skip_missing(GOLD_PARQUET_DIR / "attendance_by_td_year.parquet")
    con = _con()
    con.execute(_load("attendance_member_year_summary.sql"))
    result = _result(con, "v_attendance_member_year_summary")
    assert "unique_member_code" in result.columns
    assert "year" in result.columns
    assert "attended_count" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_year_rank_executes():
    # v_attendance_year_rank reads v_attendance_member_year_summary —
    # both must be created in the same connection.
    _skip_missing(GOLD_PARQUET_DIR / "attendance_by_td_year.parquet")
    con = _con()
    con.execute(_load("attendance_member_year_summary.sql"))
    con.execute(_load("attendance_year_rank.sql"))
    result = _result(con, "v_attendance_year_rank")
    assert "unique_member_code" in result.columns
    assert "rank_high" in result.columns
    assert len(result) > 0


# ---------------------------------------------------------------------------
# ATTENDANCE VIEWS (gap backfill)
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_attendance_timeline_executes():
    _skip_missing(*_src("data/silver/aggregated_td_tables.csv", "data/silver/flattened_members.csv"))
    con = _con()
    con.execute(_load("attendance_timeline.sql"))
    result = _result(con, "v_attendance_timeline")
    _assert_cols(result, "sitting_date", "member_name", "present_flag", "attendance_status", "party_name", "house")
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_missing_members_executes():
    _skip_missing(*_src("data/silver/flattened_members.csv", "data/gold/parquet/attendance_by_td_year.parquet"))
    con = _con()
    con.execute(_load("attendance_missing_members.sql"))
    result = _result(con, "v_attendance_missing_members")
    _assert_cols(result, "member_name", "party_name", "missing_reason")
    # May legitimately be empty if every elected member appears in attendance —
    # this is a coverage gap detector, so 0 rows is a valid (good) outcome.


@pytest.mark.sql
def test_v_attendance_chamber_sitting_days_executes():
    _skip_missing(*_src("data/silver/aggregated_td_tables.csv"))
    con = _con()
    con.execute(_load("attendance_chamber_sitting_days.sql"))
    result = _result(con, "v_attendance_chamber_sitting_days")
    _assert_cols(result, "house", "year", "sitting_days")
    assert len(result) > 0
