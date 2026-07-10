"""Tripwire for the LPT Local Adjustment Factor gold view (v_la_lpt_adjustment).

data/_meta/lpt_local_adjustment_factors.csv is git-tracked (curated-meta pattern,
like la_chief_executives.csv) and read at runtime, so this runs in CI (no skip).
Guards: every year covers exactly the 31 councils with no duplicates, the adopted
factor stays inside the statutory +/-15% band, every local_authority value joins
the crosswalk EXACTLY (or the council orphans on the page), and the registered
view SQL itself builds against the real CSV.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
CSV = ROOT / "data" / "_meta" / "lpt_local_adjustment_factors.csv"
CROSSWALK = ROOT / "data" / "_meta" / "constituency_la_crosswalk.csv"
VIEW_SQL = ROOT / "sql_views" / "constituency" / "constituency_la_lpt_adjustment.sql"

EXPECTED_COUNCILS = 31

_CSV_LIT = str(CSV).replace("\\", "/")
_CROSSWALK_LIT = str(CROSSWALK).replace("\\", "/")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    sql = VIEW_SQL.read_text(encoding="utf-8").replace("data/_meta/lpt_local_adjustment_factors.csv", _CSV_LIT)
    c.execute(sql)
    return c


def test_csv_exists():
    assert CSV.exists(), f"curated LAF table missing: {CSV}"


def test_31_councils_per_year_no_dupes(con):
    rows = con.execute(
        "SELECT year, count(*), count(DISTINCT local_authority) FROM v_la_lpt_adjustment GROUP BY year"
    ).fetchall()
    assert rows, "view returned no years"
    for year, n, distinct in rows:
        assert n == EXPECTED_COUNCILS, f"{year}: expected {EXPECTED_COUNCILS} rows, got {n}"
        assert distinct == EXPECTED_COUNCILS, f"{year}: duplicate local_authority value(s)"


def test_current_year_present(con):
    (mx,) = con.execute("SELECT max(year) FROM v_la_lpt_adjustment").fetchone()
    assert mx >= 2026, f"latest LAF year is {mx}; extractor refresh overdue"


def test_pct_within_statutory_band(con):
    bad = con.execute(
        "SELECT year, local_authority, adjustment_pct FROM v_la_lpt_adjustment"
        " WHERE adjustment_pct < -15 OR adjustment_pct > 15 OR adjustment_pct IS NULL"
    ).fetchall()
    assert not bad, f"factors outside the statutory +/-15% band: {bad}"


def test_local_authority_joins_crosswalk(con):
    """local_authority must match the crosswalk EXACTLY or the council orphans on the page."""
    miss = con.execute(
        f"""
        SELECT DISTINCT local_authority FROM v_la_lpt_adjustment
        WHERE local_authority NOT IN (
            SELECT DISTINCT local_authority
            FROM read_csv('{_CROSSWALK_LIT}', header=true, AUTO_DETECT=true)
        )
        """
    ).fetchall()
    assert not miss, f"local_authority values absent from crosswalk: {miss}"


def test_every_row_cites_source(con):
    bad = con.execute(
        "SELECT count(*) FROM v_la_lpt_adjustment"
        " WHERE source_url IS NULL OR trim(source_url) = '' OR retrieved_date IS NULL"
    ).fetchone()[0]
    assert bad == 0, "rows missing source_url/retrieved_date — provenance is mandatory"
