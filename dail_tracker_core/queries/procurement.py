"""Procurement (eTenders) retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/procurement_data.py`` (the exemplar
thin wrapper). Every function is retrieval-only SQL against the registered
``procurement_*`` views; all aggregation/joins/value-gating already live in the
views (see e.g. ``sql_views/procurement_supplier_summary.sql``). The SQL strings
are byte-for-byte the same as the old wrapper so output is unchanged — the only
difference is the return type (``QueryResult`` instead of a bare DataFrame, with
DuckDB failures surfaced as ``unavailable`` instead of a silent empty frame).

Build a connection with ``dail_tracker_core.db.connect_with_views(["procurement_*.sql"])``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    """Execute retrieval SQL and wrap the outcome.

    A DuckDB error (missing view, missing parquet, bad column) becomes an
    ``unavailable`` result rather than the old swallow-to-empty-DataFrame, so the
    caller can tell "source down" from "no rows". The exception is logged for the
    server-side trail exactly as the old ``_safe`` did.
    """
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.exception("procurement query failed")
        return QueryResult.unavailable(f"procurement query failed: {exc}")


def supplier_summary(conn: duckdb.DuckDBPyConnection, *, limit: int | None = None) -> QueryResult:
    """Supplier ranking — one row per distinct supplier (company-class), ordered by
    contract count (the trustworthy metric). Carries CRO match + lobbying flags."""
    sql = (
        "SELECT supplier, supplier_norm, n_awards, n_authorities, awarded_value_safe_eur,"
        " company_num, company_status, cro_match_method,"
        " on_lobbying_register, lobbying_returns, is_lobbying_registrant, is_lobbying_client"
        " FROM v_procurement_supplier_summary ORDER BY n_awards DESC"
    )
    if limit is not None:
        return _run(conn, sql + " LIMIT ?", [int(limit)])
    return _run(conn, sql)


def awards_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """Every award row for one supplier (detail view), most recent first."""
    return _run(
        conn,
        "SELECT tender_id, contracting_authority, cpv_code, cpv_description,"
        " competition_type, award_date, value_eur, value_kind, value_safe_to_sum"
        " FROM v_procurement_awards WHERE supplier_norm = ?"
        " ORDER BY award_date DESC NULLS LAST",
        [supplier_norm],
    )


def authority_summary(conn: duckdb.DuckDBPyConnection, *, limit: int = 50) -> QueryResult:
    """Contracting authorities ranked by number of awards."""
    return _run(
        conn,
        "SELECT contracting_authority, n_awards, n_suppliers, awarded_value_safe_eur"
        " FROM v_procurement_authority_summary ORDER BY n_awards DESC LIMIT ?",
        [int(limit)],
    )


def cpv_summary(conn: duckdb.DuckDBPyConnection, *, limit: int = 50) -> QueryResult:
    """CPV categories ranked by number of awards."""
    return _run(
        conn,
        "SELECT cpv_code, cpv_description, n_awards, n_suppliers, awarded_value_safe_eur"
        " FROM v_procurement_cpv_summary ORDER BY n_awards DESC LIMIT ?",
        [int(limit)],
    )


def lobbying_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _run(
        conn,
        "SELECT lobby_name, lobby_side, supplier, supplier_norm, n_lobby_returns,"
        " n_award_rows, n_authorities, awarded_value_safe_eur"
        " FROM v_procurement_lobbying_overlap ORDER BY n_award_rows DESC",
    )
