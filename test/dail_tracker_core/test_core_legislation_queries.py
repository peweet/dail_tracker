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


# ── Wider legislation + SI surface (added with the legislation_data migration) ──

_INDEX_COLS = {
    "bill_id",
    "bill_title",
    "bill_status",
    "bill_type",
    "sponsor",
    "introduced_date",
    "current_stage",
    "stage_number",
    "oireachtas_url",
    "bill_no",
    "bill_year",
    "bill_phase",
}


@pytest.fixture(scope="module")
def full_conn():
    """All legislation_* views (bill index/detail/timeline/SIs/entity universe)."""
    c = connect_with_views(["legislation_*.sql"], swallow_errors=True)
    yield c
    c.close()


def test_index_filtered_columns_and_status_filter(full_conn):
    r = _result_or_skip(q.index_filtered(full_conn))
    assert _INDEX_COLS.issubset(set(r.data.columns))
    statuses = _result_or_skip(q.distinct_statuses(full_conn)).data
    if statuses.empty:
        pytest.skip("no bill statuses")
    s = statuses["bill_status"].iloc[0]
    sub = _result_or_skip(q.index_filtered(full_conn, status=s))
    assert len(sub.data) <= len(r.data)
    if not sub.data.empty:
        assert (sub.data["bill_status"] == s).all()


def test_bill_detail_one_row(full_conn):
    idx = _result_or_skip(q.index_filtered(full_conn))
    if idx.data.empty:
        pytest.skip("no bills")
    bid = str(idx.data["bill_id"].iloc[0])
    r = _result_or_skip(q.bill_detail(full_conn, bid))
    assert len(r.data) <= 1


def test_si_entity_index_runs(full_conn):
    r = _result_or_skip(q.si_entity_index(full_conn))
    # full SI universe — many rows, one registered surface
    assert "si_id" in r.data.columns or "si_title" in r.data.columns


def test_si_by_bill_eu_filter_narrows(full_conn):
    idx = _result_or_skip(q.index_filtered(full_conn))
    bid_si = None
    for b in idx.data["bill_id"].astype(str).tolist():
        ys = q.si_years_for_bill(full_conn, b)
        if ys.ok and not ys.is_empty:
            bid_si = b
            break
    if bid_si is None:
        pytest.skip("no bill with SIs")
    full = _result_or_skip(q.si_by_bill(full_conn, bid_si))
    eu = _result_or_skip(q.si_by_bill(full_conn, bid_si, eu_only=True))
    assert len(eu.data) <= len(full.data)


def test_circular_si_crosswalk_missing_view_is_unavailable():
    """No view registered → surfaced as unavailable, not a silent empty frame."""
    conn = duckdb.connect()
    try:
        r = q.circular_si_crosswalk(conn)
        assert isinstance(r, QueryResult) and r.ok is False and r.unavailable_reason
    finally:
        conn.close()


def test_circular_si_crosswalk_pairs_and_resolution(full_conn):
    """The crosswalk reads the git-tracked citation CSV and LEFT JOINs our SI index:
    every row carries the contract columns, and si_resolved TRUE rows expose the SI's
    title (the rule chain is complete only when the cited SI is in our holdings)."""
    r = _result_or_skip(q.circular_si_crosswalk(full_conn))
    cols = {"circular_no", "rule_type", "si_id", "si_resolved", "si_title", "circular_source_url"}
    assert cols.issubset(set(r.data.columns))
    assert len(r.data) > 0
    resolved = r.data[r.data["si_resolved"]]
    if len(resolved):
        assert resolved["si_title"].notna().all()  # resolved ⇒ SI attributes present
    # unresolved rows are real citations we don't hold, not errors: NULL SI title
    unresolved = r.data[~r.data["si_resolved"]]
    if len(unresolved):
        assert unresolved["si_title"].isna().all()


def test_circular_si_crosswalk_single_si_filter(full_conn):
    """Filtering to one SI returns only that SI's pairs (the 'which circular applies
    this SI?' lookup)."""
    r = _result_or_skip(q.circular_si_crosswalk(full_conn))
    if r.data.empty:
        pytest.skip("no crosswalk rows")
    row = r.data.iloc[0]
    one = _result_or_skip(q.circular_si_crosswalk(full_conn, si_year=int(row["si_year"]),
                                                  si_number=int(row["si_number"])))
    assert (one.data["si_id"] == row["si_id"]).all()
