"""SIPO political-finance retrieval — Streamlit-free.

Covers both lenses of the Payments page's SIPO section — party DONATIONS and
GE2024 election EXPENSES — since both read the same ``sipo_*.sql`` view set.
Moved verbatim from ``utility/data_access/sipo_donations_data.py`` and
``sipo_expenses_data.py``. Retrieval-only; all rollups live in the
``v_sipo_*_by_party`` views.

The two headline-totals queries SUM the per-party rollup into a single row; the
aggregate columns are aliased here so the thin wrapper can read them by name and
build its ``{...}`` dict (the old code read a bare ``fetchone()`` tuple
positionally — same values, clearer access).

Privacy/no-inference (unchanged): donor name + amount are the public SIPO
record; there is no donor-address column. OCR-derived rows carry a verify mark.

Build with ``connect_with_views(["sipo_*.sql"], swallow_errors=False)``.
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
        _log.warning("sipo query failed: %s | %s", sql[:120], exc)
        return QueryResult.unavailable(f"sipo query failed: {exc}")


# ── Donations ─────────────────────────────────────────────────────────────────


def donations_totals(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row headline totals across all parties (sums the rollup view)."""
    return _run(
        conn,
        "SELECT SUM(total_value) AS total_value, SUM(donation_count) AS donation_count,"
        " COUNT(*) AS parties FROM v_sipo_donations_by_party",
    )


def donations_by_party(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One row per party — drives the Party-Donations cards."""
    return _run(
        conn,
        "SELECT party, donation_count, total_value, min_value, max_value, verify_count"
        " FROM v_sipo_donations_by_party"
        " ORDER BY total_value DESC",
    )


def party_donors(conn: duckdb.DuckDBPyConnection, party: str) -> QueryResult:
    """Donor receipts for one party — name, amount, date, method, verify flag."""
    return _run(
        conn,
        "SELECT donor_name, value_eur, date_received_raw, nature,"
        " description_of_donor, needs_verify, source_page"
        " FROM v_sipo_donations"
        " WHERE party = ?"
        " ORDER BY value_eur DESC",
        [party],
    )


# ── Election expenses ─────────────────────────────────────────────────────────


def expenses_totals(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row headline totals across all parties (sums the rollup view)."""
    return _run(
        conn,
        "SELECT SUM(total_expenditure) AS total_expenditure, SUM(candidate_count) AS candidate_count,"
        " COUNT(*) AS parties, SUM(excluded_count) AS excluded_count"
        " FROM v_sipo_expenses_by_party",
    )


def expenses_by_party(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One row per party — drives the Election-Expenses cards."""
    return _run(
        conn,
        "SELECT party, candidate_count, total_expenditure, max_expenditure,"
        " verify_count, excluded_count"
        " FROM v_sipo_expenses_by_party"
        " ORDER BY total_expenditure DESC",
    )


def party_candidates(conn: duckdb.DuckDBPyConnection, party: str) -> QueryResult:
    """Per-candidate expenditure for one party — name, constituency, amount, flag."""
    return _run(
        conn,
        "SELECT candidate_name, constituency, expenditure_eur, flag,"
        " is_verified, source_page"
        " FROM v_sipo_expenses_base"
        " WHERE party = ?"
        " ORDER BY (flag = 'over_limit_verify'), expenditure_eur DESC",
        [party],
    )
