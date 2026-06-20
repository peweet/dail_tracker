"""Corporate notices retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/corporate_data.py``. Retrieval-only
SELECTs against the registered ``corporate_*`` views; the page does its own
faceting/search/aggregation in pandas off these frames (unchanged). Build a
connection with ``connect_with_views(["corporate_*.sql"], swallow_errors=True)``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.exception("corporate query failed")
        return QueryResult.unavailable(f"corporate query failed: {exc}")


def corporate_notices(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every in-scope corporate notice (personal insolvency excluded upstream)."""
    return _run(conn, "SELECT * FROM v_corporate_notices")


def corporate_notices_for_company(conn: duckdb.DuckDBPyConnection, company_num) -> QueryResult:
    """Every CRO-matched corporate notice (Iris Oifigiúil) for ONE company, keyed on the
    hard CRO ``company_num`` — the per-firm slice of ``v_corporate_cro_notice_match`` that
    the company dossier's corporate-register panel renders. Newest first.

    Display-only public-record annotation: appearing on a statutory notice (insolvency /
    liquidation / receivership / register change) is a public record, NOT a finding of
    wrongdoing, and a firm may appear as the SUBJECT of a notice or as a named insolvency
    practitioner — the firm's own ``company_status`` (carried on every row) is what states
    whether THIS legal entity is itself in distress."""
    return _run(
        conn,
        "SELECT notice_ref, entity_name, issue_date, notice_category, notice_subtype,"
        " company_num, company_status, company_reg_date, comp_dissolved_date, status_pill_value"
        " FROM v_corporate_cro_notice_match WHERE company_num = ?"
        " ORDER BY issue_date DESC NULLS LAST",
        [company_num],
    )


def cbi_notice_matches(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-notice CBI authorisation lookup (EXPERIMENTAL — sandbox source)."""
    return _run(conn, "SELECT * FROM v_corporate_cbi_notice_match")


def cbi_repeat_distress(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-firm repeat-distress aggregate (EXPERIMENTAL — sandbox source)."""
    return _run(conn, "SELECT * FROM v_corporate_cbi_repeat_distress")


def brand_aliases(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Brand -> parent_fund -> fund_type curated alias map."""
    return _run(conn, "SELECT * FROM v_corporate_brand_aliases")
