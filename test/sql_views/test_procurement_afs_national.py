"""Tripwire for v_procurement_afs_national_* (amalgamated national LA AFS).

Reads data/silver/parquet/afs_amalgamated_divisions.parquet (Dept of Housing's
audited amalgamation of all 31 councils, promoted by afs_amalgamated_extract.py) +
the two views. Silver is gitignored -> SKIP in CI, run on a dev box.

Guards: the by_year spine is a faithful Σ-across-divisions of the by_division grain
(the GROUP BY lives in the view so the page never aggregates), net = gross - income
holds on both grains, and the BUDGET-grain firewall fields (realisation_tier /
value_kind) are carried so this is never summed with the over-€20k PO register.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
AFS = ROOT / "data" / "silver" / "parquet" / "afs_amalgamated_divisions.parquet"
SQL = ROOT / "sql_views" / "procurement" / "procurement_afs_national.sql"

pytestmark = pytest.mark.skipif(not AFS.exists(), reason=f"silver source absent (CI): {AFS.name}")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(SQL.read_text(encoding="utf-8"))  # whole file: 2 views; comments hold ';'
    return c


def test_by_division_grain(con):
    """One row per (year x service division), every published year 2016-2023, division
    never NULL (the WHERE filters it)."""
    n, n_div, n_year, mn, mx = con.execute(
        """
        SELECT count(*), count(DISTINCT division), count(DISTINCT year),
               min(year), max(year)
        FROM v_procurement_afs_national_by_division
        """
    ).fetchone()
    assert n == n_div * n_year, "grain is not a clean year x division grid"
    assert (mn, mx) == (2016, 2023)
    nulls = con.execute(
        "SELECT count(*) FROM v_procurement_afs_national_by_division WHERE division IS NULL"
    ).fetchone()[0]
    assert nulls == 0


def test_net_equals_gross_minus_income_by_division(con):
    """net cost = gross expenditure - the service's own income/grants, on every row.

    net is PUBLISHED independently in the amalgamation (not derived here), so it tracks
    gross - income only within the source's ~€10k rounding (e.g. 2019 Housing differs by
    €10k). The €50k tolerance accepts that rounding while still catching a column-swap or
    sign bug, which would be off by hundreds of millions."""
    bad = con.execute(
        """
        SELECT count(*) FROM v_procurement_afs_national_by_division
        WHERE abs(net_expenditure_eur - (gross_expenditure_eur - income_eur)) > 50_000
        """
    ).fetchone()[0]
    assert bad == 0


def test_by_year_reconciles_to_division_sum(con):
    """The by_year spine must equal the Σ across divisions for the same year — the page
    relies on the view, not its own aggregation, to total."""
    mismatches = con.execute(
        """
        WITH d AS (
            SELECT year, SUM(gross_expenditure_eur) g, SUM(income_eur) i,
                   SUM(net_expenditure_eur) n, count(*) c
            FROM v_procurement_afs_national_by_division GROUP BY year
        )
        SELECT count(*) FROM v_procurement_afs_national_by_year y JOIN d USING (year)
        WHERE abs(y.gross_expenditure_eur - d.g) > 1.0
           OR abs(y.income_eur            - d.i) > 1.0
           OR abs(y.net_expenditure_eur   - d.n) > 1.0
           OR y.n_divisions <> d.c
        """
    ).fetchone()[0]
    assert mismatches == 0


def test_by_year_is_eight_years_ascending(con):
    rows = con.execute("SELECT year FROM v_procurement_afs_national_by_year ORDER BY year").fetchall()
    years = [r[0] for r in rows]
    assert years == sorted(years)
    assert years == list(range(2016, 2024))


def test_budget_grain_firewall_fields_present(con):
    """realisation_tier + value_kind ride along so this BUDGET grain is never unioned/summed
    with the narrower over-€20k PO euros (see project_procurement_phase_taxonomy)."""
    cols = [d[0] for d in con.execute("SELECT * FROM v_procurement_afs_national_by_division LIMIT 0").description]
    assert "realisation_tier" in cols
    assert "value_kind" in cols
