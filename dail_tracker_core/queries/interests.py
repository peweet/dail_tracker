"""Register of Members' Interests retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/interests_data.py``. Retrieval-only
SELECTs against the two registered views ``v_member_interests_detail`` and
``v_member_interests_index`` (the leaderboard rank/counts/flags are produced in
the index view, not here).

Each function takes an explicit ``conn`` and returns a ``QueryResult`` so a
missing view surfaces as *unavailable* rather than the old ``_safe`` silent
empty frame. The thin Streamlit wrapper reshapes the success frames into the
exact bool / dict / DataFrame contracts the page already depends on.

Build with ``connect_with_views(["member_interests_*.sql",
"member_zz_interests_*.sql"], swallow_errors=False)`` — the detail-view glob
sorts before the index glob so dependency order holds (index reads detail).
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
        _log.warning("interests query failed: %s | %s", sql[:120], exc)
        return QueryResult.unavailable(f"interests query failed: {exc}")


# Detail column contract shared by the browse list and the per-TD view.
_DETAIL_COLS = (
    "member_name, party_name, constituency, declaration_year,"
    " interest_category, interest_text, landlord_flag, property_flag"
)


# ── Availability ──────────────────────────────────────────────────────────────


def availability(conn: duckdb.DuckDBPyConnection, house: str) -> QueryResult:
    """One-row probe: does v_member_interests_detail hold any row for this house?
    The wrapper maps (ok AND has-row) -> True."""
    return _run(conn, "SELECT 1 AS one FROM v_member_interests_detail WHERE house = ? LIMIT 1", [house])


# ── Filter options ────────────────────────────────────────────────────────────


def distinct_years(conn: duckdb.DuckDBPyConnection, house: str) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT declaration_year FROM v_member_interests_detail"
        " WHERE house = ? AND declaration_year IS NOT NULL"
        " ORDER BY declaration_year DESC",
        [house],
    )


def distinct_members(conn: duckdb.DuckDBPyConnection, house: str) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT member_name FROM v_member_interests_detail"
        " WHERE house = ? AND member_name IS NOT NULL"
        " ORDER BY member_name",
        [house],
    )


# ── Detail retrieval ──────────────────────────────────────────────────────────


def detail(
    conn: duckdb.DuckDBPyConnection,
    house: str,
    name_q: str = "",
    years: tuple[int, ...] = (),
    landlord_only: bool = False,
) -> QueryResult:
    """Browse-list rows. Filters AND together. LIMIT 1000 matches prior behaviour."""
    clauses: list[str] = ["house = ?"]
    params: list = [house]
    if name_q:
        clauses.append("member_name ILIKE ?")
        params.append(f"%{name_q}%")
    if years:
        placeholders = ", ".join("?" for _ in years)
        clauses.append(f"declaration_year IN ({placeholders})")
        params.extend(int(y) for y in years)
    if landlord_only:
        clauses.append("landlord_flag = ?")
        params.append(True)
    where = " WHERE " + " AND ".join(clauses)
    return _run(
        conn,
        f"SELECT {_DETAIL_COLS} FROM v_member_interests_detail"
        f"{where} ORDER BY declaration_year DESC, member_name LIMIT 1000",
        params,
    )


def td_interests(conn: duckdb.DuckDBPyConnection, house: str, td_name: str) -> QueryResult:
    """Every declaration for one TD across all years."""
    return _run(
        conn,
        f"SELECT {_DETAIL_COLS} FROM v_member_interests_detail"
        " WHERE house = ? AND member_name = ?"
        " ORDER BY declaration_year DESC, interest_category",
        [house, td_name],
    )


# ── Member index (ranked leaderboard) ─────────────────────────────────────────


def member_index(conn: duckdb.DuckDBPyConnection, house: str, year: int) -> QueryResult:
    """Ranked member index for a house × year (rank/counts/flags from the view)."""
    return _run(
        conn,
        "SELECT rank, member_name, party_name, constituency,"
        " total_declarations, directorship_count, property_count, share_count,"
        " is_landlord, is_property_owner"
        " FROM v_member_interests_index"
        " WHERE house = ? AND declaration_year = ?"
        " ORDER BY rank",
        [house, int(year)],
    )
