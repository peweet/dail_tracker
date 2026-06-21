"""Committees retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/committees_data.py``. Retrieval-only
SELECTs against the four ``committees_*`` views. The per-committee summary view
returns ``party_seats_json`` as a JSON string; the small decode into Python
tuples + the DECIMAL->int casts are a one-shot shaping on a ≤100-row frame and
stay in the thin Streamlit wrapper (the page's existing behaviour, firewall-clean).

Build with ``connect_with_views(["committees_*.sql"], swallow_errors=False)`` —
committees registers loud (a missing view is a real break, not a soft-empty).
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

# Committee-name → crosswalk-stem normalizer, as a SQL expression on a single bind
# placeholder. MUST stay byte-identical to the `committee_key` derivation in
# sql_views/committee_evidence/committee_evidence_meetings.sql: folds curly
# apostrophes to ASCII, collapses whitespace, lowercases, then strips the leading
# committee-formation prefix ("Joint/Select Committee on X" / "Comhchoiste/Roghchoiste
# X" → "x") so a meeting attaches regardless of apostrophe glyph or Joint/Select
# formation. (test_committee_meetings_crosswalk_* locks both halves together.)
_COMMITTEE_KEY_SQL = (
    "regexp_replace("
    " lower(trim(regexp_replace("
    "  replace(replace(?, chr(8217), ''''), chr(8216), ''''), '\\s+', ' ', 'g'))),"
    " '^(seanad |dail )?(joint |select )?(committee (on|of) |comhchoiste |roghchoiste )', '')"
)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.warning("committees query failed: %s | %s", sql[:120], exc)
        return QueryResult.unavailable(f"committees query failed: {exc}")


def assignments(conn: duckdb.DuckDBPyConnection, chamber: str) -> QueryResult:
    """One row per (member × committee) for the chamber."""
    return _run(
        conn,
        "SELECT name, party, constituency, dail_number, committee, committee_url,"
        ' type, status, role, is_chair, start, "end"'
        " FROM v_committee_assignments WHERE chamber = ?",
        [chamber],
    )


def office_holders(conn: duckdb.DuckDBPyConnection, chamber: str) -> QueryResult:
    """One row per (member × office)."""
    return _run(
        conn,
        'SELECT name, party, office, start, "end" FROM v_committee_office_holders WHERE chamber = ?',
        [chamber],
    )


def member_detail(conn: duckdb.DuckDBPyConnection, chamber: str) -> QueryResult:
    """Per-committee rollup (raw — includes party_seats_json for the wrapper to decode)."""
    return _run(
        conn,
        "SELECT committee, members, parties, chairs, status, type, url,"
        " chair_name, chair_party, party_seats_json"
        " FROM v_committee_member_detail WHERE chamber = ?",
        [chamber],
    )


def meetings(conn: duckdb.DuckDBPyConnection, committee: str, limit: int = 60) -> QueryResult:
    """Reverse-chron meeting history for one committee (the timeline spine).

    The page selects a committee by its human-readable register name; the API records
    the same committee under a different apostrophe glyph and possibly the other
    formation (Joint vs Select). We match on the view's normalized `committee_key`,
    applying the identical normalization (`_COMMITTEE_KEY_SQL`) to the selection here
    so both formations' meetings surface regardless of which page the user is on.
    """
    return _run(
        conn,
        "SELECT committee_name, date, transcript_url, source_xml, topics, n_topics, n_orgs, n_persons,"
        " witness_orgs, witness_persons"
        " FROM v_committee_meetings"
        f" WHERE committee_key = {_COMMITTEE_KEY_SQL}"
        " ORDER BY date DESC LIMIT ?",
        [committee, limit],
    )


def party_seats(conn: duckdb.DuckDBPyConnection, chamber: str, committee: str | None = None) -> QueryResult:
    """Long-format party seats per committee; optionally filtered to one committee."""
    if committee is not None:
        return _run(
            conn,
            "SELECT committee, party, seats FROM v_committee_party_seats"
            " WHERE chamber = ? AND committee = ? ORDER BY seats DESC, party",
            [chamber, committee],
        )
    return _run(
        conn,
        "SELECT committee, party, seats FROM v_committee_party_seats"
        " WHERE chamber = ? ORDER BY committee, seats DESC, party",
        [chamber],
    )
