"""Tests for the per-member vote retrieval functions in dail_tracker_core.queries.votes.

These three functions (member_vote_summary / member_vote_history / member_year_summary)
were extracted from raw ``conn.execute`` calls that used to live in
``utility/ui/vote_explorer.py`` (a logic-firewall leak: the UI layer ran its own SQL).

Two layers:
  1. Unit (always runs): against a connection with no views, each returns an
     *unavailable* QueryResult — proving DuckDB failures surface, not swallow.
  2. Integration PARITY (skips if the vote gold parquet is absent): against the real
     registered views, each function returns a frame byte-identical to the original
     inline SQL it replaced. This is the "data unchanged before/after the move" guard.
"""

from __future__ import annotations

import duckdb
import pytest
from pandas.testing import assert_frame_equal

from config import GOLD_SEANAD_VOTE_HISTORY_PARQUET, GOLD_VOTE_HISTORY_PARQUET
from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import votes as q
from dail_tracker_core.results import QueryResult

_TD_HISTORY_LIMIT = 5000  # the exact limit vote_explorer.render_member_votes passes


# ---------------------------------------------------------------------------
# 1. Unit — DuckDB failure surfaces as unavailable (no data needed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda c: q.member_vote_summary(c, "X"),
        lambda c: q.member_vote_history(c, "X"),
        lambda c: q.member_year_summary(c, "X"),
    ],
)
def test_missing_view_is_unavailable_not_silent_empty(call):
    conn = duckdb.connect()  # no views registered → the view does not exist
    try:
        result = call(conn)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration PARITY — real views; skip if the vote gold parquet is absent
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(
        ["vote*.sql"],
        substitutions={
            "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
            "{SEANAD_VOTE_PARQUET_PATH}": GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix(),
        },
        swallow_errors=True,
    )
    yield c
    c.close()


def _sample_member_id(conn) -> str:
    res = q.member_vote_summary(conn, "__definitely_absent__")
    if not res.ok:
        pytest.skip(f"vote views not available: {res.unavailable_reason}")
    row = conn.execute("SELECT member_id FROM td_vote_summary WHERE member_id IS NOT NULL LIMIT 1").fetchone()
    if not row:
        pytest.skip("td_vote_summary has no rows")
    return row[0]


def test_member_vote_summary_parity(conn):
    mid = _sample_member_id(conn)
    got = q.member_vote_summary(conn, mid).data
    ref = conn.execute(
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        [mid],
    ).df()
    assert_frame_equal(got, ref)


def test_member_vote_history_parity(conn):
    mid = _sample_member_id(conn)
    got = q.member_vote_history(conn, mid, limit=_TD_HISTORY_LIMIT).data
    ref = conn.execute(
        "SELECT vote_id, vote_date, debate_title, vote_type, vote_outcome, oireachtas_url"
        " FROM v_vote_member_detail WHERE member_id = ? ORDER BY vote_date DESC LIMIT ?",
        [mid, _TD_HISTORY_LIMIT],
    ).df()
    assert_frame_equal(got, ref)


def test_member_year_summary_parity(conn):
    mid = _sample_member_id(conn)
    got = q.member_year_summary(conn, mid).data
    ref = conn.execute(
        "SELECT year, yes_count, no_count, abstained_count"
        " FROM td_vote_year_summary WHERE member_id = ? ORDER BY year ASC LIMIT 50",
        [mid],
    ).df()
    assert_frame_equal(got, ref)
