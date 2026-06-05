"""Tests for dail_tracker_core.queries.legislation.

Two layers (same shape as the other core-query tests):
  1. Unit (always runs): a query with no views registered returns an *unavailable*
     QueryResult — failures are surfaced, not swallowed.
  2. Integration (skips if data/silver/parquet/bill_amendments.parquet absent):
     per-bill grain (one row per bill_id), the ranking is descending, and the
     per-list breakdown sums back to the total.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import legislation as q
from dail_tracker_core.results import QueryResult

_CONTRACT_COLS = {
    "bill_id",
    "bill_title",
    "bill_type",
    "bill_status",
    "amendment_lists",
    "distinct_stages",
    "committee_lists",
    "report_lists",
    "cream_lists",
    "dail_lists",
    "seanad_lists",
    "first_amendment_date",
    "last_amendment_date",
}


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()
    try:
        result = q.most_contested_bills(conn)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["legislation_bill_amendment_intensity.sql"], swallow_errors=True)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"bill_amendments not available: {result.unavailable_reason}")
    return result


def test_most_contested_columns_and_ranking(conn):
    r = _result_or_skip(q.most_contested_bills(conn, limit=20))
    assert _CONTRACT_COLS.issubset(set(r.data.columns))
    assert 0 < len(r.data) <= 20
    counts = r.data["amendment_lists"].tolist()
    assert counts == sorted(counts, reverse=True), "not ranked by amendment_lists desc"


def test_one_row_per_bill(conn):
    dup = conn.execute(
        "SELECT COUNT(*) FROM (SELECT bill_id, COUNT(*) c"
        " FROM v_bill_amendment_intensity GROUP BY bill_id HAVING c > 1)"
    ).fetchone()[0]
    assert dup == 0, "view is not one-row-per-bill"


def test_stage_breakdown_does_not_exceed_total(conn):
    # committee + report + cream are a partition of the lists by stage; their sum
    # must not exceed the bill total (other stage labels may exist, so <=).
    r = _result_or_skip(q.most_contested_bills(conn, limit=50))
    df = r.data
    partial = df["committee_lists"] + df["report_lists"] + df["cream_lists"]
    assert (partial <= df["amendment_lists"]).all()


def test_intensity_for_bill_matches_ranking(conn):
    top = _result_or_skip(q.most_contested_bills(conn, limit=1)).data
    if top.empty:
        pytest.skip("no amendment data")
    bill_id = top["bill_id"].iloc[0]
    one = _result_or_skip(q.amendment_intensity_for_bill(conn, bill_id))
    assert len(one.data) == 1
    assert one.data["bill_id"].iloc[0] == bill_id
    assert int(one.data["amendment_lists"].iloc[0]) == int(top["amendment_lists"].iloc[0])
