"""Tripwire for the Your-Councillors gold views (v_la_councillors + 4 siblings).

Each reads a git-tracked data/_meta CSV (committed, like la_chief_executives.csv), so this runs
in CI (no skip). Guards: roster covers all 31 councils, the coverage tier set is valid, Carlow is
the roll_call council with named votes, agendas are non-empty, and Standing Orders carry the
records_named_votes flag.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
SQL = ROOT / "sql_views" / "constituency"
META = ROOT / "data" / "_meta"

VIEWS = {
    "constituency_la_councillors.sql": "la_councillors.csv",
    "constituency_la_council_meeting_coverage.sql": "la_council_meeting_coverage.csv",
    "constituency_la_councillor_votes.sql": "la_councillor_votes.csv",
    "constituency_la_meeting_agendas.sql": "la_meeting_agendas.csv",
    "constituency_la_standing_orders.sql": "la_standing_orders.csv",
}

pytestmark = pytest.mark.skipif(not (META / "la_councillors.csv").exists(), reason="councillor gold CSVs absent")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    for fname, csv in VIEWS.items():
        sql = (SQL / fname).read_text(encoding="utf-8")
        sql = sql.replace(f"data/_meta/{csv}", str(META / csv).replace("\\", "/"))
        c.execute(sql)
    return c


def test_roster_covers_31_councils(con):
    n = con.execute("SELECT COUNT(DISTINCT local_authority) FROM v_la_councillors").fetchone()[0]
    assert n == 31
    total = con.execute("SELECT COUNT(*) FROM v_la_councillors").fetchone()[0]
    assert total > 850  # ~916, ~96% of the ~949 elected


def test_coverage_tiers_valid(con):
    tiers = {r[0] for r in con.execute("SELECT DISTINCT tier FROM v_la_council_meeting_coverage").fetchall()}
    assert tiers <= {"roll_call", "proposer_seconder", "scanned_pending", "cmis_pending", "unseeded"}
    carlow = con.execute("SELECT tier FROM v_la_council_meeting_coverage WHERE local_authority='Carlow'").fetchone()[0]
    assert carlow == "roll_call"


def test_carlow_has_named_votes(con):
    n = con.execute("SELECT COUNT(*) FROM v_la_councillor_votes WHERE local_authority='Carlow'").fetchone()[0]
    assert n > 50
    bad = con.execute(
        "SELECT COUNT(*) FROM v_la_councillor_votes WHERE vote NOT IN ('for','against','abstain','absent')"
    ).fetchone()[0]
    assert bad == 0


def test_agendas_present(con):
    n = con.execute("SELECT COUNT(*) FROM v_la_meeting_agendas WHERE agenda <> ''").fetchone()[0]
    assert n > 100  # ~212 meetings


def test_standing_orders_named_vote_flag(con):
    rows = con.execute("SELECT local_authority, records_named_votes FROM v_la_standing_orders").fetchall()
    assert len(rows) >= 5
    gc = con.execute(
        "SELECT records_named_votes FROM v_la_standing_orders WHERE local_authority='Galway County'"
    ).fetchone()
    assert gc is not None and bool(gc[0]) is True
