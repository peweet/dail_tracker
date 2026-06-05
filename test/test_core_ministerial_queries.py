"""Tests for dail_tracker_core.queries.ministerial.

Two layers (same shape as test_core_procurement_queries):
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult — DuckDB failures are surfaced, not
     swallowed into a silent empty DataFrame.
  2. Integration (skips if data/silver/ministerial_tenure.parquet absent):
     against the real registered view, each query returns the contract columns,
     and the accountability primitive (minister_on_date) resolves correctly.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import ministerial as q
from dail_tracker_core.results import QueryResult

_CONTRACT_COLS = {
    "department_key",
    "department_label",
    "minister_name",
    "unique_member_code",
    "start_date",
    "end_date",
    "is_current",
    "tenure_days",
    "wikidata_person",
    "wikidata_position",
}


# ---------------------------------------------------------------------------
# 1. Unit — DuckDB failure surfaces as unavailable (no data needed)
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()  # no views registered → the view does not exist
    try:
        result = q.timeline(conn)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration — real view; skip if the silver parquet has not been built
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["member_ministerial_tenure.sql"], swallow_errors=True)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"ministerial_tenure not available: {result.unavailable_reason}")
    return result


def test_timeline_columns(conn):
    r = _result_or_skip(q.timeline(conn))
    assert _CONTRACT_COLS.issubset(set(r.data.columns))
    assert len(r.data) > 0


def test_minister_name_always_present(conn):
    # minister_name is the display field — it must never be null even when the
    # member-code link is absent (historical ministers).
    r = _result_or_skip(q.timeline(conn))
    assert r.data["minister_name"].notna().all()


def test_current_ministers_are_open_ended(conn):
    r = _result_or_skip(q.current_ministers(conn))
    if r.is_empty:
        pytest.skip("no sitting ministers in source")
    assert r.data["is_current"].all()
    assert r.data["end_date"].isna().all()


def test_department_filter_scopes(conn):
    deps = _result_or_skip(q.departments(conn))
    assert len(deps.data) > 0
    key = deps.data["department_key"].iloc[0]
    scoped = _result_or_skip(q.timeline(conn, department_key=key))
    assert (scoped.data["department_key"] == key).all()


def test_minister_on_date_resolves_one_holder(conn):
    # Pick a department, take its earliest-ending ended tenure, and assert the
    # holder is the one returned for a date inside that span.
    deps = _result_or_skip(q.departments(conn))
    key = deps.data["department_key"].iloc[0]
    spans = _result_or_skip(q.timeline(conn, department_key=key)).data
    ended = spans[spans["end_date"].notna()]
    if ended.empty:
        pytest.skip("no ended tenure to probe")
    row = ended.iloc[0]
    midpoint = row["start_date"] + (row["end_date"] - row["start_date"]) / 2
    on_date = str(midpoint.date())
    hit = _result_or_skip(q.minister_on_date(conn, key, on_date))
    assert len(hit.data) == 1
    assert hit.data["minister_name"].iloc[0] == row["minister_name"]


def test_tenures_for_member_links_current_members(conn):
    # At least some ministers carry a member code; the per-member query must
    # return only that member's rows.
    timeline = _result_or_skip(q.timeline(conn)).data
    coded = timeline[timeline["unique_member_code"].notna()]
    if coded.empty:
        pytest.skip("no member-coded ministers in source")
    code = coded["unique_member_code"].iloc[0]
    r = _result_or_skip(q.tenures_for_member(conn, code))
    assert len(r.data) > 0
    assert (r.data["unique_member_code"] == code).all()
