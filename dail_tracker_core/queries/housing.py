"""National Housing screen retrieval — Streamlit-free.

Retrieval-only SQL against the registered ``v_ssha_waiting_list_*`` views (built by
``dail_tracker_core.connections.housing_conn``). All aggregation / unpivot / rollup /
per-capita lives in ``sql_views/housing/*`` — this layer only SELECTs and filters by
grain/area, returning a ``QueryResult`` so the page can tell "source unavailable" from
"no rows".
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="housing", log=_log)


def waiting_list_totals(conn: duckdb.DuckDBPyConnection, grain: str) -> QueryResult:
    """League-table headline per area at one grain ('county' | 'la' | 'national'):
    waiting total, YoY, %4yr+/%7yr+, population, waiters-per-1,000. Ordered by size."""
    return _run(
        conn,
        "SELECT * FROM v_ssha_waiting_list_totals WHERE grain = ? ORDER BY waiting_total DESC",
        [grain],
    )


def supply_national(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National supply & affordability headline (vacancy, avg private rent, HAP) — the
    counterpart to the demand-side waiting list. Single row; each metric self-periods."""
    return _run(conn, "SELECT * FROM v_housing_supply_national")


def accommodation_spend_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """State asylum (international-protection) + Ukraine accommodation spend per year,
    split by stream — from the published over-€20k purchase-order registers."""
    return _run(conn, "SELECT * FROM v_accommodation_spend_by_year ORDER BY year")


def accommodation_spend_providers(conn: duckdb.DuckDBPyConnection, limit: int = 40) -> QueryResult:
    """Providers ranked by total committed asylum/Ukraine accommodation spend."""
    return _run(
        conn,
        "SELECT * FROM v_accommodation_spend_providers ORDER BY total_eur DESC LIMIT ?",
        [limit],
    )


def hap_national(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National HAP profile: households, % working, rent burden, years to social housing."""
    return _run(conn, "SELECT * FROM v_housing_hap_national")


def completions_trend(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National new-dwelling completions per complete year (CSO NDQ09 'State' row)."""
    return _run(conn, "SELECT year, completions FROM v_housing_completions_trend ORDER BY year")


def rent_by_county(conn: duckdb.DuckDBPyConnection, county: str) -> QueryResult:
    """Average weekly private rent for one county (Census 2022). Empty for Dublin /
    Galway (F2023B splits them with no single total)."""
    return _run(
        conn,
        "SELECT county, avg_weekly_private_rent, rent_period FROM v_housing_rent_by_county WHERE county = ?",
        [county],
    )


def waiting_list_composition(conn: duckdb.DuckDBPyConnection, grain: str, area: str) -> QueryResult:
    """The five demographic breakdowns for one area (the distribution stripes).
    ord-then-count ordering is applied in the view."""
    return _run(
        conn,
        "SELECT dimension, category, ord, count, pct FROM v_ssha_waiting_list_composition "
        "WHERE grain = ? AND area = ? AND year = 2025",
        [grain, area],
    )
