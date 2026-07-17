"""Local-government ("Who runs your county") retrieval — Streamlit-free.

Retrieval-only SQL against the registered council-grain views (built by
``dail_tracker_core.connections.constituency_conn``):
  v_la_chief_executives · v_la_collection_rates · v_la_planning_overturn ·
  v_la_derelict_sites_levy · v_la_accountability_summary

All aggregation / joins / grain-guards live in ``sql_views/constituency/*`` — this
layer only SELECTs and filters by local_authority, returning a ``QueryResult`` so
the page can tell "source unavailable" from "no rows".
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="local government", log=_log)


def chief_executives(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """All 31 council Chief Executives — the index grid."""
    return _run(conn, "SELECT * FROM v_la_chief_executives ORDER BY local_authority")


def chief_executive(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Single-row CE for one council dossier."""
    return _run(conn, "SELECT * FROM v_la_chief_executives WHERE local_authority = ?", [la])


def collection_rates(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_collection_rates WHERE local_authority = ?", [la])


def planning_overturn(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_planning_overturn WHERE local_authority = ?", [la])


def noac_scorecard(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Seven NOAC 2024 accountability indicators (finance/workforce/roads/fire/litter) for
    one council, each with the national median; powers the dossier scorecard cards."""
    return _run(conn, "SELECT * FROM v_la_noac_scorecard WHERE local_authority = ?", [la])


def noac_scorecard_history(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Scorecard metrics across NOAC report years (2022-2024) for one council — feeds the
    trend sparklines beside each headline metric."""
    return _run(conn, "SELECT * FROM v_la_noac_scorecard_history WHERE local_authority = ? ORDER BY year", [la])


def noac_indicators(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Every published NOAC 2024 indicator for one council (~125 series, raw values) — the
    'All NOAC indicators' reference drill-down."""
    return _run(
        conn,
        "SELECT family, series_label, raw_value, source_page, deep_link "
        "FROM v_la_noac_indicators WHERE local_authority = ? "
        "ORDER BY family, indicator_code, series_label",
        [la],
    )


def cash_signals(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """The three published finance/collection figures (revenue balance, rates collection,
    derelict-levy collection) for one council, co-located, each beside its national median.
    No relationship between them is asserted."""
    return _run(conn, "SELECT * FROM v_la_cash_signals WHERE local_authority = ?", [la])


def derelict_sites_levy(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_derelict_sites_levy WHERE local_authority = ?", [la])


def derelict_levy_ranking(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """All councils ranked for cross-council derelict-levy ENFORCEMENT comparison — the
    national view the per-council ``derelict_sites_levy`` can't give in one call. The view
    already carries national window totals + the ``levied_nothing`` flag + the arrears-aware
    ``collection_rate_pct``; here we just return every council, worst outstanding first."""
    return _run(conn, "SELECT * FROM v_la_derelict_sites_levy ORDER BY cumulative_outstanding_eur DESC NULLS LAST")


def housing_performance(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_housing_performance WHERE local_authority = ?", [la])


def lgas_audit(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """The independent LGAS statutory audit reports for one council, newest first — the
    auditor's own opinion + findings on each year's AFS. Verbatim only (opinion text, literal
    heading flags); no derived score. Executive accountability: the CE administers the accounts
    the auditor examines, councillors sign none of it."""
    return _run(
        conn,
        "SELECT year, audit_opinion_text, has_emphasis_of_matter, has_ce_response, "
        "section_headings, pages, report_page_url "
        "FROM v_la_lgas_audit WHERE local_authority = ? ORDER BY year DESC",
        [la],
    )


def council_money(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Council procurement scale (purchase orders / payments over €20k) — context for
    the size of money the executive signs off. Only ~23/31 councils publish."""
    return _run(conn, "SELECT * FROM v_procurement_council_summary WHERE council = ?", [la])


def national_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row national headline for the landing page."""
    return _run(conn, "SELECT * FROM v_la_accountability_summary")


def map_layers(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """All 31 councils with choropleth layer values + quintile buckets (index map)."""
    return _run(conn, "SELECT * FROM v_la_map_layers ORDER BY local_authority")
