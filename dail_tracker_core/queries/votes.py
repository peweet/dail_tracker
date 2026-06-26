"""Votes retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/votes_data.py``. Every function returns
a ``QueryResult`` wrapping the raw retrieval frame; the thin Streamlit wrapper
does the small UI-shaping the page wants (``.tolist()`` for picker options, a
single ``str`` for the redirect name). The parameterised WHERE/ILIKE assembly is
retrieval plumbing (not business logic) and lives here.

The vote views union both chambers and need ``{PARQUET_PATH}`` /
``{SEANAD_VOTE_PARQUET_PATH}`` substitutions; the caller supplies those to
``connect_with_views`` — these functions only take a ready connection.
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

VOTE_INDEX_LIMIT = 500
DIVISION_MEMBERS_LIMIT = 5000


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.warning("votes query failed: %s | params=%s | error=%s", sql[:120], params, exc)
        return QueryResult.unavailable(f"votes query failed: {exc}")


def _and_clauses(clauses: list[str]) -> str:
    sql = ""
    for c in clauses:
        sql = (sql + " AND " if sql else "") + c
    return sql


def result_summary(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT division_count, member_count, first_vote_date, last_vote_date"
        " FROM v_vote_result_summary WHERE house = ? LIMIT 1",
        [house],
    )


def vote_years(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT CAST(EXTRACT(YEAR FROM vote_date) AS INTEGER) AS year"
        " FROM v_vote_index WHERE vote_date IS NOT NULL AND house = ?"
        " ORDER BY year DESC LIMIT 20",
        [house],
    )


def member_names(conn: duckdb.DuckDBPyConnection, party: str = "", house: str = "Dáil") -> QueryResult:
    if party:
        return _run(
            conn,
            "SELECT DISTINCT member_name FROM td_vote_summary"
            " WHERE member_name IS NOT NULL AND house = ? AND party_name = ?"
            " ORDER BY member_name ASC LIMIT 1000",
            [house, party],
        )
    return _run(
        conn,
        "SELECT DISTINCT member_name FROM td_vote_summary"
        " WHERE member_name IS NOT NULL AND house = ? ORDER BY member_name ASC LIMIT 1000",
        [house],
    )


def party_names(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT party_name FROM td_vote_summary"
        " WHERE party_name IS NOT NULL AND house = ? ORDER BY party_name ASC LIMIT 100",
        [house],
    )


def td_row_by_name(conn: duckdb.DuckDBPyConnection, member_name: str, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_name = ? AND house = ? LIMIT 1",
        [member_name, house],
    )


def td_name_by_id(conn: duckdb.DuckDBPyConnection, member_id: str) -> QueryResult:
    return _run(
        conn,
        "SELECT member_name FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        [member_id],
    )


def vote_index(conn: duckdb.DuckDBPyConnection, date_from, date_to, outcome, house: str = "Dáil") -> QueryResult:
    clauses: list[str] = ["house = ?"]
    params: list = [house]
    if date_from:
        clauses.append("vote_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("vote_date <= ?")
        params.append(date_to)
    if outcome:
        clauses.append("vote_outcome = ?")
        params.append(outcome)
    where = ""
    body = _and_clauses(clauses)
    if body:
        where = " WHERE " + body
    sql = (
        "SELECT vote_id, vote_date, debate_title, vote_outcome,"
        " yes_count, no_count, abstained_count, margin, oireachtas_url"
        f" FROM v_vote_index{where} ORDER BY vote_date DESC LIMIT ?"
    )
    params.append(VOTE_INDEX_LIMIT)
    return _run(conn, sql, params)


def vote_by_id(conn: duckdb.DuckDBPyConnection, vote_id: str) -> QueryResult:
    return _run(
        conn,
        "SELECT vote_id, vote_date, debate_title, vote_outcome,"
        " yes_count, no_count, abstained_count, margin, oireachtas_url"
        " FROM v_vote_index WHERE vote_id = ? LIMIT 1",
        [vote_id],
    )


def party_breakdown(conn: duckdb.DuckDBPyConnection, vote_id) -> QueryResult:
    return _run(
        conn,
        "SELECT party_name, vote_type, member_count, vote_pct FROM party_vote_breakdown WHERE vote_id = ? LIMIT 500",
        [vote_id],
    )


def division_members(conn: duckdb.DuckDBPyConnection, vote_id) -> QueryResult:
    return _run(
        conn,
        "SELECT member_id, member_name, party_name, constituency, vote_type"
        " FROM v_vote_member_detail WHERE vote_id = ?"
        " AND member_name IS NOT NULL"
        " ORDER BY party_name ASC, member_name ASC LIMIT ?",
        [vote_id, DIVISION_MEMBERS_LIMIT],
    )


def sources(conn: duckdb.DuckDBPyConnection, vote_id) -> QueryResult:
    return _run(
        conn,
        "SELECT source_url, source_document_url, official_pdf_url, legislation_url, source_label"
        " FROM v_vote_sources WHERE vote_id = ? LIMIT 50",
        [vote_id],
    )


def topical_votes(conn: duckdb.DuckDBPyConnection, topics: tuple[str, ...], house: str = "Dáil") -> QueryResult:
    """Recent member votes on hot-topic debates. ``topics`` are ILIKE patterns."""
    patterns = list(topics)
    if not patterns:
        return QueryResult.success(pd.DataFrame())
    likes = " OR ".join(["debate_title ILIKE ?" for _ in patterns])
    sql = (
        "SELECT vote_id, vote_date, member_id, member_name, party_name, constituency,"
        " vote_type, debate_title, vote_outcome"
        " FROM v_vote_member_detail"
        " WHERE vote_type IN ('Voted Yes', 'Voted No')"
        " AND member_name IS NOT NULL AND house = ?"
        f" AND ({likes})"
        " ORDER BY vote_date DESC LIMIT 2000"
    )
    return _run(conn, sql, [house, *patterns])
