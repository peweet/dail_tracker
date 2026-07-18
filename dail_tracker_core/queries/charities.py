"""Charity financial retrieval — Streamlit-free.

Surfaces ``v_charity_financials_by_year`` (data/silver/charities/annual_reports.parquet):
the multi-year income/expenditure/funding series per charity. Gold previously
exposed only the latest-year snapshot; these functions return the full trajectory
so a consumer can draw trend lines or compute year-on-year change.

Pure retrieval (SELECT / WHERE / ORDER BY / LIMIT). Figures are returned as filed
— see the view header on outliers; no winsorising happens here.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_VIEW = "v_charity_financials_by_year"
_COLS = (
    "rcn, registered_charity_name, period_year, period_end_date,"
    " gross_income, gross_expenditure, surplus_deficit, gov_share,"
    " income_govt_or_la, income_other_public_bodies, income_donations,"
    " income_trading, income_other, total_assets, net_assets,"
    " total_liabilities, cash_at_hand, employees_full_time, employees_part_time,"
    " employees_band, volunteers_band"
)


_run = make_runner("charity", _log)


def financials_by_year(conn: duckdb.DuckDBPyConnection, rcn: int) -> QueryResult:
    """Full annual financial series for one charity (oldest→newest).

    Empty (success, no rows) for a charity with no filed returns.
    """
    return _run(
        conn,
        f"SELECT {_COLS} FROM {_VIEW} WHERE rcn = ? ORDER BY period_year",
        [rcn],
    )


def latest_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The most recent period_year present across all charities (for labelling)."""
    return _run(conn, f"SELECT MAX(period_year) AS latest_year FROM {_VIEW}")


def sector_totals_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Register-wide income/expenditure totals per year.

    Reads ``v_charity_sector_totals_by_year`` (the rollup lives in that view, so
    this stays a plain projection). Powers the "money through the charity sector
    over the decade" story.
    """
    return _run(
        conn,
        "SELECT period_year, n_charities, total_gross_income,"
        " total_gross_expenditure, total_income_govt_or_la"
        " FROM v_charity_sector_totals_by_year ORDER BY period_year",
    )
