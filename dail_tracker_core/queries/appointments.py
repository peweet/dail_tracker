"""Public Appointments retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/appointments_data.py``. One registered
analytical surface; the page does its filtering/faceting/grouping in pandas off
this frame. Build with ``connect_with_views(["appointments_*.sql"], swallow_errors=True)``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


_run = make_runner("appointments", _log)


def public_appointments(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every public-appointment notice as a row — the full v_public_appointments view."""
    return _run(conn, "SELECT * FROM v_public_appointments")


def stateboards_roster(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every CURRENT state-board seat as a row — the full v_stateboards_roster view.
    The live DPER membership register; complements the Iris appointment EVENTS."""
    return _run(conn, "SELECT * FROM v_stateboards_roster")


def stateboards_boards(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The state-board universe — one row per board, with legal basis and
    gender-balance metadata (v_stateboards_boards)."""
    return _run(conn, "SELECT * FROM v_stateboards_boards")
