"""Tripwire for v_la_derelict_sites_levy (council enforcement signal).

Reads the real gold parquet (data/gold/parquet/derelict_sites_levy_wide.parquet,
built by extractors/derelict_sites_levy_extract.py).
The parquet is gitignored, so these SKIP in CI and run on a dev box / integration.

Guards: all 31 councils present and joining the CE roster (the City-and-County
name collapse must keep working), money non-negative, and the national-outstanding
window total reconciles to the per-row sum (catches a broken window/dup).
"""

from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
LEVY = PROJECT_ROOT / "data" / "gold" / "parquet" / "derelict_sites_levy_wide.parquet"
LEVY_SQL = PROJECT_ROOT / "sql_views" / "constituency" / "constituency_la_derelict_sites_levy.sql"
CE_SQL = PROJECT_ROOT / "sql_views" / "constituency" / "constituency_la_chief_executives.sql"

pytestmark = pytest.mark.skipif(not LEVY.exists(), reason=f"gold source absent (CI): {LEVY.name}")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(
        CE_SQL.read_text(encoding="utf-8").replace(
            "data/_meta/la_chief_executives.csv",
            str(PROJECT_ROOT / "data" / "_meta" / "la_chief_executives.csv").replace("\\", "/"),
        )
    )
    c.execute(
        LEVY_SQL.read_text(encoding="utf-8").replace(
            "data/gold/parquet/derelict_sites_levy_wide.parquet", str(LEVY).replace("\\", "/")
        )
    )
    return c


def test_all_31_councils(con):
    n = con.execute("SELECT count(*) FROM v_la_derelict_sites_levy").fetchone()[0]
    assert n == 31, f"expected 31 councils, got {n}"


def test_every_council_joins_ce_roster(con):
    orphans = con.execute(
        """
        SELECT local_authority FROM v_la_derelict_sites_levy
        WHERE local_authority NOT IN (SELECT local_authority FROM v_la_chief_executives)
        """
    ).fetchall()
    assert not orphans, f"City-and-County name collapse broke — orphans: {orphans}"


def test_money_non_negative(con):
    bad = con.execute(
        """
        SELECT local_authority FROM v_la_derelict_sites_levy
        WHERE amount_levied_eur < 0 OR total_received_eur < 0 OR cumulative_outstanding_eur < 0
        """
    ).fetchall()
    assert not bad, f"negative money: {bad}"


def test_national_window_reconciles(con):
    """The national_outstanding window total must equal the sum of per-row values."""
    row_sum, window_total = con.execute(
        """
        SELECT SUM(cumulative_outstanding_eur), MAX(national_outstanding_eur)
        FROM v_la_derelict_sites_levy
        """
    ).fetchone()
    assert abs(row_sum - window_total) < 1, (row_sum, window_total)
