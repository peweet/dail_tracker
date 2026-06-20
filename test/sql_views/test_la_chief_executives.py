"""Tripwire for the hand-curated local-authority Chief Executive roster.

data/_meta/la_chief_executives.csv is the ONLY identity source for the 31 LA
executive heads (no API exists; see constituency_la_chief_executives.sql). It is
git-tracked and read at runtime, so these invariants guard against silent drift:
a dropped/duplicated council, a blank name, or a local_authority value that no
longer joins the crosswalk (which would orphan the council on the page).

Reads the real tracked CSV directly (it is always present, unlike parquet
fixtures), then exercises the view SQL against it.
"""

from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
CSV = PROJECT_ROOT / "data" / "_meta" / "la_chief_executives.csv"
CROSSWALK = PROJECT_ROOT / "data" / "_meta" / "constituency_la_crosswalk.csv"
VIEW_SQL = PROJECT_ROOT / "sql_views" / "constituency" / "constituency_la_chief_executives.sql"

EXPECTED_COUNCILS = 31
EXPECTED_TYPE_COUNTS = {"City": 3, "City and County": 2, "County": 26}


_CSV_LIT = str(CSV).replace("\\", "/")
_CROSSWALK_LIT = str(CROSSWALK).replace("\\", "/")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(
        f"CREATE VIEW roster AS SELECT * FROM read_csv('{_CSV_LIT}', header=true, AUTO_DETECT=true)"
    )
    return c


def test_csv_exists():
    assert CSV.exists(), f"curated roster missing: {CSV}"


def test_thirty_one_distinct_councils(con):
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT local_authority) FROM roster"
    ).fetchone()
    assert n == EXPECTED_COUNCILS, f"expected {EXPECTED_COUNCILS} rows, got {n}"
    assert distinct == EXPECTED_COUNCILS, "duplicate local_authority value(s)"


def test_no_blank_identity_or_source(con):
    """Every row must name the official and cite a source — the accountability point."""
    bad = con.execute(
        """
        SELECT local_authority FROM roster
        WHERE chief_executive IS NULL OR trim(chief_executive) = ''
           OR local_authority IS NULL OR trim(local_authority) = ''
           OR source_url IS NULL OR trim(source_url) = ''
        """
    ).fetchall()
    assert not bad, f"rows missing name/LA/source: {bad}"


def test_council_type_split(con):
    got = dict(
        con.execute("SELECT council_type, count(*) FROM roster GROUP BY 1").fetchall()
    )
    assert got == EXPECTED_TYPE_COUNTS, got


def test_local_authority_joins_crosswalk(con):
    """local_authority must match the crosswalk EXACTLY or the council orphans on the page."""
    miss = con.execute(
        f"""
        SELECT local_authority FROM roster
        WHERE local_authority NOT IN (
            SELECT DISTINCT local_authority
            FROM read_csv('{_CROSSWALK_LIT}', header=true, AUTO_DETECT=true)
        )
        """
    ).fetchall()
    assert not miss, f"local_authority values absent from crosswalk: {miss}"


def test_view_sql_builds_and_returns_31():
    """The registered view itself builds against the real CSV (path made absolute)."""
    sql = VIEW_SQL.read_text(encoding="utf-8").replace(
        "data/_meta/la_chief_executives.csv", str(CSV).replace("\\", "/")
    )
    c = duckdb.connect()
    c.execute(sql)
    (n,) = c.execute("SELECT count(*) FROM v_la_chief_executives").fetchone()
    assert n == EXPECTED_COUNCILS
