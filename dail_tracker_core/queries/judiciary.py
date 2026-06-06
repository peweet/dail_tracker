"""Judiciary legal-diary retrieval — Streamlit-free.

Retrieval-only SELECTs against the registered ``judiciary_*`` views
(sql_views/judiciary_legal_diary_*.sql), which read the gold parquets produced
by extractors/legal_diary_extract.py. The page does its own faceting / grouping
in pandas off these frames.

Build a connection with
``connect_with_views(["judiciary_*.sql"], swallow_errors=True)``.

PRIVACY: the cases view is the ANONYMISED layer only — statutory in-camera
matters are dropped at the extractor and every natural person is reduced to
initials. There is no un-anonymised text to retrieve here by design.
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
        _log.exception("judiciary query failed")
        return QueryResult.unavailable(f"judiciary query failed: {exc}")


def legal_diary_schedule(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Tier A — judge sitting-sessions (officials only, no party data)."""
    return _run(conn, "SELECT * FROM v_judiciary_legal_diary_schedule")


def legal_diary_counts(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Tier B — per-session case-item counts (aggregate density)."""
    return _run(conn, "SELECT * FROM v_judiciary_legal_diary_counts")


def legal_diary_cases(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Tier C — ANONYMISED case listings + provenance link."""
    return _run(conn, "SELECT * FROM v_judiciary_legal_diary_cases")


# ── The Bench & Courts (green core) ─────────────────────────────────────────
# All joins / classification live in the views (sql_views/judiciary_*.sql); these
# are plain retrievals. Scope is appointment / office / rank / assignment / salary
# band only — no performance, conduct, or ranking data.
def roster(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The sitting bench — one row per judge (identity grain)."""
    return _run(conn, "SELECT * FROM v_judiciary_roster")


def appointments(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Judicial appointment events + gov.ie nomination context (event grain)."""
    return _run(conn, "SELECT * FROM v_judiciary_appointments")


def profile(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-judge identity summary for the career-arc drill-down."""
    return _run(conn, "SELECT * FROM v_judiciary_profile")


def nominations(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """gov.ie nomination announcements (vacancy-lifecycle context)."""
    return _run(conn, "SELECT * FROM v_judiciary_nominations")


def authority_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Aggregate: appointment-notice count by appointing authority."""
    return _run(conn, "SELECT * FROM v_judiciary_authority_summary")


def elevation_ladder(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Aggregate: real promotions per court transition."""
    return _run(conn, "SELECT * FROM v_judiciary_elevation_ladder")


# ── The Courts — system health (no named judges) ────────────────────────────
# Aggregate court-throughput facts only; the clearance metric + week-parsing live
# in the views (sql_views/judiciary_courts_*.sql). No row here names a judge.
def courts_clearance(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Annual case clearance by court, 2017–2024 (incoming/resolved/clearance_pct)."""
    return _run(conn, "SELECT * FROM v_courts_clearance")


def courts_waiting_times(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Published waiting-time lists, latest two years + parsed weeks for ranking."""
    return _run(conn, "SELECT * FROM v_courts_waiting_times")


def courthouses(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Active, geocoded courthouses for the venue map."""
    return _run(conn, "SELECT * FROM v_courthouses")
