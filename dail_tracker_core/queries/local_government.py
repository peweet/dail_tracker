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

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


_run = make_runner("local government", _log)


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


def capital_history(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Audited capital-account investment by year for one local authority.

    This is the build/acquire account, not revenue expenditure, purchase orders,
    payments, budgets or tender values. The registered view has already summed
    the service divisions and reconciled each year to the printed AFS total.
    """
    return _run(
        conn,
        "SELECT year, capital_expenditure_eur, capital_income_eur, n_divisions, "
        "reconciled, parser, source_url, source_page_number "
        "FROM v_procurement_afs_capital_by_year WHERE council = ? ORDER BY year DESC",
        [la],
    )


def capital_divisions(conn: duckdb.DuckDBPyConnection, la: str, year: int) -> QueryResult:
    """One audited council-year capital account, broken down by service division."""
    return _run(
        conn,
        "SELECT division, capital_expenditure_eur, capital_income_eur, reconciled, "
        "source_file_url AS source_url, source_page_number "
        "FROM v_procurement_afs_capital_by_division WHERE council = ? AND year = ? "
        "ORDER BY capital_expenditure_eur DESC",
        [la, int(year)],
    )


def minutes_coverage(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Document-grain coverage facts for the vetted council-minutes corpus."""
    return _run(conn, "SELECT * FROM v_la_council_minutes_coverage WHERE local_authority = ?", [la])


def minutes_documents(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Recent vetted minute documents, never passage/chunk counts."""
    return _run(
        conn,
        "SELECT document_id, meeting, meeting_date, meeting_date_parsed, meeting_scope, "
        "source_status, source_url FROM v_la_council_minutes_documents "
        "WHERE local_authority = ? ORDER BY meeting_date_parsed DESC NULLS LAST, meeting DESC LIMIT 12",
        [la],
    )


def ce_report_coverage(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Published CE-report coverage and review-queue counts for one council."""
    return _run(conn, "SELECT * FROM v_la_ce_report_coverage WHERE local_authority = ?", [la])


def ce_report_documents(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Recent published Chief Executive reports with authoritative source links."""
    return _run(
        conn,
        "SELECT document_id, report_title, report_month, date_parse_status, source_status, "
        "source_url, source_pages FROM v_la_ce_report_documents WHERE local_authority = ? "
        "ORDER BY report_month DESC NULLS LAST, report_title DESC LIMIT 12",
        [la],
    )


def ce_report_signals(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Only source-reviewed forward-work observations permitted for publication."""
    return _run(
        conn,
        "SELECT lead_id, report_title, report_month, quote, lead_types, amount_mentions, "
        "reviewed_project_name, reviewed_stage, evidence_band, source_url, source_page "
        "FROM v_la_ce_report_signals WHERE local_authority = ? "
        "ORDER BY report_month DESC NULLS LAST, reviewed_project_name",
        [la],
    )


def national_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row national headline for the landing page."""
    return _run(conn, "SELECT * FROM v_la_accountability_summary")


def map_layers(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """All 31 councils with choropleth layer values + quintile buckets (index map)."""
    return _run(conn, "SELECT * FROM v_la_map_layers ORDER BY local_authority")
