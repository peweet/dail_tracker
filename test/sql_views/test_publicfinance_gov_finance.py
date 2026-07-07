"""Tripwire for v_gov_finance_annual (the national-spend denominator view).

Reads data/gold/parquet/cso_gfa01.parquet (CSO PxStat GFA01) + the view. Gold is
gitignored -> SKIP in CI, run on a dev box.

Guards the firewall promise that this view is a pure extraction/pivot: one row per
year (no double count), whole-euro units, and an honest surplus/deficit sign — so a
downstream "share of total spend" can trust it as a denominator.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
GFA = ROOT / "data" / "gold" / "parquet" / "cso_gfa01.parquet"
SQL = ROOT / "sql_views" / "publicfinance" / "publicfinance_gov_finance_annual.sql"

pytestmark = pytest.mark.skipif(not GFA.exists(), reason=f"gold source absent (CI): {GFA.name}")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    sql = SQL.read_text(encoding="utf-8").replace("data/gold/parquet/cso_gfa01.parquet", str(GFA).replace("\\", "/"))
    c.execute(sql)
    return c


def test_one_row_per_year_no_double_count(con):
    """GFA01's native grain is (Year x Item) with one row per pair, so the conditional
    pivot must collapse to exactly one row per year."""
    n, distinct_years = con.execute("SELECT count(*), count(DISTINCT year) FROM v_gov_finance_annual").fetchone()
    assert n == distinct_years
    assert n >= 25


def test_units_multiplied_to_whole_euros(con):
    """CSO ships €millions; the view scales by 1e6. A single year's revenue is therefore
    >€100bn — catches a missing/extra unit multiplier."""
    mx = con.execute("SELECT max(revenue_eur) FROM v_gov_finance_annual").fetchone()[0]
    assert mx > 1e11


def test_surplus_sign_convention(con):
    """B9 is positive for a surplus, negative for a deficit. 2024 (Apple windfall) must be
    a surplus; an austerity-era year (2010) must be a deficit."""
    s2024 = con.execute("SELECT surplus_deficit_eur FROM v_gov_finance_annual WHERE year = 2024").fetchone()[0]
    s2010 = con.execute("SELECT surplus_deficit_eur FROM v_gov_finance_annual WHERE year = 2010").fetchone()[0]
    assert s2024 > 0
    assert s2010 < 0


def test_no_ratios_in_view(con):
    """Firewall: this view co-locates raw totals only. The three published series are the
    ONLY non-key columns; a ratio sneaking in (e.g. *_pct) breaks the extraction promise."""
    cols = [d[0] for d in con.execute("SELECT * FROM v_gov_finance_annual LIMIT 0").description]
    assert cols == ["year", "revenue_eur", "expenditure_eur", "surplus_deficit_eur"]
