"""Public-finance retrieval — Streamlit-free.

Retrieval-only SQL against the registered ``publicfinance`` views (defined in
``sql_views/publicfinance/``). The aggregation lives in the views (these read CSO
general-government series); this layer only reads. DuckDB failures surface as
``unavailable`` QueryResults rather than silent empty frames.

Build a connection with
``dail_tracker_core.db.connect_with_views(["publicfinance_*.sql"])``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


_run = make_runner("publicfinance", _log)


def gov_finance_annual(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National general-government revenue / expenditure / balance per year (CSO GFA01).
    The authoritative denominator for "share of total public spend" context. Newest first.
    Pre-aggregated in ``v_gov_finance_annual``; this only reads."""
    return _run(
        conn,
        "SELECT year, revenue_eur, expenditure_eur, surplus_deficit_eur"
        " FROM v_gov_finance_annual WHERE year IS NOT NULL ORDER BY year DESC",
    )
