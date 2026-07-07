"""Query-layer tests for the public-finance denominator path.

Exercises dail_tracker_core.queries.publicfinance against the registered
v_gov_finance_annual view — the same retrieval the Streamlit pages use to turn an
isolated € figure into a "share of national spend", minus Streamlit.

Skips in CI: the view reads data/gold/parquet/cso_gfa01.parquet, which is
gitignored. Runs on a dev box / integration where the pipeline output is present.
The unavailable-path test needs no source and always runs.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dail_tracker_core.queries import publicfinance as q  # noqa: E402

SQL = ROOT / "sql_views" / "publicfinance" / "publicfinance_gov_finance_annual.sql"
GFA = ROOT / "data" / "gold" / "parquet" / "cso_gfa01.parquet"

pytestmark = pytest.mark.skipif(not GFA.exists(), reason="CSO GFA01 source absent (CI)")


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect()
    sql = SQL.read_text(encoding="utf-8").replace("data/gold/parquet/cso_gfa01.parquet", str(GFA).replace("\\", "/"))
    c.execute(sql)
    return c


def test_gov_finance_annual_shape(conn):
    res = q.gov_finance_annual(conn)
    assert res.ok
    assert list(res.data.columns) == [
        "year",
        "revenue_eur",
        "expenditure_eur",
        "surplus_deficit_eur",
    ]
    assert len(res.data) >= 25  # annual series 1995-> ; ~31 years today


def test_gov_finance_annual_sorted_newest_first(conn):
    """The view orders DESC so the page renders the latest year without re-sorting."""
    years = q.gov_finance_annual(conn).data["year"].tolist()
    assert years == sorted(years, reverse=True)
    assert years[0] >= 2024  # source kept current


def test_gov_finance_annual_units_are_whole_euros(conn):
    """CSO publishes €millions; the view multiplies to whole euros for clean ratios.
    Revenue must therefore be in the hundreds-of-billions, not the hundreds-of-thousands."""
    res = q.gov_finance_annual(conn).data
    assert res["revenue_eur"].max() > 1e11


def test_2024_apple_windfall_is_a_surplus(conn):
    """2024 carries the Apple-CJEU windfall: a large POSITIVE balance (ESA2010 B9).
    Guards the surplus sign convention the downstream 'share of spend' copy relies on."""
    res = q.gov_finance_annual(conn).data
    row = res.loc[res["year"] == 2024].iloc[0]
    assert row["surplus_deficit_eur"] > 20e9


def test_surplus_reconciles_to_revenue_minus_expenditure(conn):
    """B9 is published independently by CSO, but should track revenue - expenditure to
    within €millions rounding — a tripwire on the conditional pivot mapping the wrong Item."""
    res = q.gov_finance_annual(conn).data
    row = res.loc[res["year"] == 2024].iloc[0]
    implied = row["revenue_eur"] - row["expenditure_eur"]
    assert abs(implied - row["surplus_deficit_eur"]) < 50e6


def test_unavailable_when_view_missing():
    """A bare connection has no v_gov_finance_annual: the query must surface
    'unavailable' (ok=False) rather than raising or returning a silent empty frame."""
    res = q.gov_finance_annual(duckdb.connect())
    assert not res.ok
    assert res.unavailable_reason and "publicfinance" in res.unavailable_reason
