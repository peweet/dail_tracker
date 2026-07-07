"""Tripwire for v_la_housing_performance (council social-housing management signals).

Council-grain DISTINCT of v_constituency_council_housing_performance (NOAC H-series).
Reads the gold noac_h*_wide parquets via that view → SKIP in CI (gitignored), run on
a dev box. Guards: one row per council (the DISTINCT must not fan out), joins the CE
roster, national medians constant.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
H2 = ROOT / "data" / "gold" / "parquet" / "noac_h2_vacancies_wide.parquet"
SQL_DIR = ROOT / "sql_views" / "constituency"
CSV = ROOT / "data" / "_meta" / "la_chief_executives.csv"

pytestmark = pytest.mark.skipif(not H2.exists(), reason="NOAC H-series gold absent (CI)")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    ce = (
        SQL_DIR.joinpath("constituency_la_chief_executives.sql")
        .read_text(encoding="utf-8")
        .replace("data/_meta/la_chief_executives.csv", str(CSV).replace("\\", "/"))
    )
    c.execute(ce)
    # dependency chain: crosswalk -> council_housing_performance -> la_housing_performance
    for f in (
        "constituency_la_crosswalk.sql",
        "constituency_council_housing_performance.sql",
        "constituency_la_housing_performance.sql",
    ):
        c.execute(SQL_DIR.joinpath(f).read_text(encoding="utf-8"))
    return c


def test_one_row_per_council(con):
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT local_authority) FROM v_la_housing_performance"
    ).fetchone()
    assert n == distinct == 31, f"DISTINCT fanned out: {n} rows, {distinct} councils"


def test_joins_ce_roster(con):
    orphans = con.execute(
        """
        SELECT local_authority FROM v_la_housing_performance
        WHERE local_authority NOT IN (SELECT local_authority FROM v_la_chief_executives)
        """
    ).fetchall()
    assert not orphans, orphans


def test_national_medians_constant(con):
    n = con.execute("SELECT count(DISTINCT nat_vacancy_pct) FROM v_la_housing_performance").fetchone()[0]
    assert n == 1
