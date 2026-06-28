"""Ministerial tenure retrieval — Streamlit-free.

Surfaces ``v_member_ministerial_tenure`` (data/silver/ministerial_tenure.parquet):
the "who ran each department, and when" timeline. ``minister_name`` is always
present; ``unique_member_code`` is set only for ministers who are current
Oireachtas members, so the caller links a clickable profile only when it is
non-null.

The interesting non-obvious function is :func:`minister_on_date` — the
accountability primitive that powers "who was Minister for X when this SI was
signed / this payment was made / this vote was taken". Everything else is a
plain projection of the view.

Pure retrieval (SELECT / WHERE / ORDER BY / LIMIT). No JOIN, GROUP BY or business
metric definition lives here — those belong in the SQL view.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_VIEW = "v_member_ministerial_tenure"
_COLS = (
    "department_key, department_label, minister_name, unique_member_code,"
    " start_date, end_date, is_current, tenure_days, wikidata_person, wikidata_position"
)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="ministerial", log=_log)


def departments(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Distinct (key, label) department list for a picker."""
    return _run(
        conn,
        f"SELECT DISTINCT department_key, department_label FROM {_VIEW}"
        " WHERE department_label IS NOT NULL ORDER BY department_label",
    )


def timeline(conn: duckdb.DuckDBPyConnection, department_key: str | None = None) -> QueryResult:
    """Full tenure timeline, optionally scoped to one department (most recent first)."""
    if department_key:
        return _run(
            conn,
            f"SELECT {_COLS} FROM {_VIEW} WHERE department_key = ? ORDER BY start_date DESC",
            [department_key],
        )
    return _run(conn, f"SELECT {_COLS} FROM {_VIEW} ORDER BY department_label, start_date DESC")


def current_ministers(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One row per department currently filled (end_date NULL)."""
    return _run(conn, f"SELECT {_COLS} FROM {_VIEW} WHERE is_current ORDER BY department_label")


def tenures_for_member(conn: duckdb.DuckDBPyConnection, unique_member_code: str) -> QueryResult:
    """Ministerial history for one current member — feeds the member-overview panel.

    Empty (success, no rows) for members who never held office or whose tenure
    predates the member spine.
    """
    return _run(
        conn,
        f"SELECT {_COLS} FROM {_VIEW} WHERE unique_member_code = ? ORDER BY start_date DESC",
        [unique_member_code],
    )


def minister_on_date(conn: duckdb.DuckDBPyConnection, department_key: str, on_date: str) -> QueryResult:
    """Who held ``department_key`` on ``on_date`` (ISO 'YYYY-MM-DD').

    The accountability primitive: the post is held if it started on/before the
    date and either has not ended or ended on/after it. Returns at most one row.
    """
    return _run(
        conn,
        f"SELECT {_COLS} FROM {_VIEW}"
        " WHERE department_key = ? AND start_date <= ?"
        " AND (end_date IS NULL OR end_date >= ?)"
        " ORDER BY start_date DESC LIMIT 1",
        [department_key, on_date, on_date],
    )
