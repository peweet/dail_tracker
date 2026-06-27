"""Query-layer tests for the Your-Councillors retrieval path.

dail_tracker_core.queries.your_councillors holds the council/LEA filtering SQL the
Streamlit page runs (the data-access layer is a firewall-clean cache passthrough over
this). The real gold views read parquet that is gitignored AND not built on every box,
so instead of a perpetually-skipped view-registered test, this builds SYNTHETIC
in-memory tables matching the view contracts. That exercises the actual WHERE / ORDER BY
/ parameter-binding of each function deterministically and ALWAYS runs.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dail_tracker_core.queries import your_councillors as q  # noqa: E402


@pytest.fixture()
def conn():
    """A connection carrying tiny stand-ins for the v_la_* views the queries read.
    Carlow + Cork County so the council/LEA filters have something to discriminate."""
    c = duckdb.connect()
    c.execute(
        """
        CREATE TABLE v_la_councillors AS SELECT * FROM (VALUES
            ('Carlow', 'Carlow', 'Ann Murphy',  'Independent', 'current'),
            ('Carlow', 'Carlow', 'Brian Nolan', 'Fianna Fail', 'current'),
            ('Carlow', 'Tullow', 'Cara Byrne',  'Fine Gael',   'current'),
            ('Carlow', '',       'No LEA Seat', 'Green',        'current'),
            ('Cork County', 'Bandon', 'Don Walsh', 'Labour',    'current')
        ) AS t(local_authority, lea, name, party, status)
        """
    )
    c.execute(
        """
        CREATE TABLE v_la_council_meeting_coverage AS SELECT * FROM (VALUES
            ('Carlow', 2024, 11),
            ('Cork County', 2024, 9)
        ) AS t(local_authority, year, meetings_covered)
        """
    )
    c.execute(
        """
        CREATE TABLE v_la_chief_executives AS SELECT * FROM (VALUES
            ('Carlow', 'Coilin O Reilly', 'Chief Executive', 2023, 'https://carlow.ie/ce')
        ) AS t(local_authority, chief_executive, head_title, appointed_year, source_url)
        """
    )
    return c


def test_councils_distinct_sorted(conn):
    res = q.councils(conn)
    assert res.ok
    assert res.data["local_authority"].tolist() == ["Carlow", "Cork County"]


def test_leas_filters_to_council_and_drops_blank(conn):
    """LEAs are scoped to the chosen council and the empty-LEA seat is excluded so the
    page's LEA picker never shows a blank option."""
    res = q.leas(conn, "Carlow")
    assert res.ok
    assert res.data["lea"].tolist() == ["Carlow", "Tullow"]  # sorted, no '' , no Bandon


def test_roster_scoped_to_council_and_lea(conn):
    """Roster returns only the seats in the (council, LEA) pair, name-sorted."""
    res = q.roster(conn, "Carlow", "Carlow")
    assert res.ok
    assert res.data["name"].tolist() == ["Ann Murphy", "Brian Nolan"]
    assert set(res.data.columns) == {"name", "party", "lea", "status"}


def test_councillor_exact_match(conn):
    res = q.councillor(conn, "Carlow", "Cara Byrne")
    assert res.ok and len(res.data) == 1
    assert res.data.iloc[0]["lea"] == "Tullow"


def test_councillor_wrong_council_returns_empty(conn):
    """A name that exists in another council must not leak across the council filter."""
    res = q.councillor(conn, "Cork County", "Cara Byrne")
    assert res.ok and res.data.empty


def test_coverage_filtered_by_council(conn):
    res = q.coverage(conn, "Carlow")
    assert res.ok and len(res.data) == 1
    assert res.data.iloc[0]["meetings_covered"] == 11


def test_chief_executive_reused_from_ce_view(conn):
    res = q.chief_executive(conn, "Carlow")
    assert res.ok and len(res.data) == 1
    assert res.data.iloc[0]["chief_executive"] == "Coilin O Reilly"


def test_unavailable_when_view_absent(conn):
    """A query against a view this connection lacks (votes/agendas/standing_orders are
    not created here) must surface 'unavailable', not raise."""
    res = q.votes(conn, "Carlow", "Ann Murphy")
    assert not res.ok
    assert res.unavailable_reason and "your_councillors" in res.unavailable_reason


def test_unknown_council_is_empty_not_unavailable(conn):
    """A council with no rows is a successful empty result (render an empty state), NOT
    an unavailable source."""
    res = q.roster(conn, "Atlantis", "Nowhere")
    assert res.ok and res.data.empty
