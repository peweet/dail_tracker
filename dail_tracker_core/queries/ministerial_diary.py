"""Ministerial-diary retrieval — Streamlit-free.

Retrieval-only SELECTs against the registered ``ministerial_diary_*`` views
(sql_views/diary/ministerial_diary_*.sql), which read the gold parquet produced
by extractors/diary_promote_gold.py (the vetted sandbox->gold promotion). The
page does its own faceting / grouping in pandas off these frames.

Build a connection with
``connect_with_views(["ministerial_diary_*.sql"], swallow_errors=True)``.

FRAMING (no inference — surfaced in the page provenance): a diary meeting is
co-occurrence, NOT a lobbying return; counts are coverage-driven; data is
quarterly-in-arrears. The only register cross-ref exposed is the POSITIVE
``corroborated`` flag (met AND lobbied the same minister).
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


_run = make_runner("ministerial-diary", _log)


def org_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Organisations ranked by ministerial meetings (+ corroboration / state-body split)."""
    return _run(conn, "SELECT * FROM v_ministerial_diary_org_overlap")


def engagements(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-(engagement x org) drill-down rows (minister, dept, date, subject, source)."""
    return _run(conn, "SELECT * FROM v_ministerial_diary_engagements")


def meetings(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The BROAD landscape — every external meeting (one row each, NO org-match required)."""
    return _run(conn, "SELECT * FROM v_ministerial_diary_meetings")


def minister_briefs(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Incoming-minister BRIEF corpus — per-department stated goals / priorities / machinery-of-
    government changes (the agenda layer that pairs with the diaries). Display-only, the dept's own
    words; view = v_minister_briefs (sql_views/diary/minister_briefs.sql)."""
    return _run(conn, "SELECT * FROM v_minister_briefs")


# ── Period-grain rollups (the page's Year/Month filter as a WHERE clause) ─────────────────
#
# The ministerial_diary_zz_* views precompute every rollup the page renders at three period
# grains — 'all' (whole corpus), 'year', 'month' — so the retrieval below is WHERE-only and
# the page never re-aggregates in pandas (logic-firewall audit 2026-07-16).


def _period_where(year: int | None, month: int | None) -> tuple[str, list]:
    """WHERE fragment selecting one period grain of a ministerial_diary_zz_* rollup view."""
    if year is None:
        return "period_grain = 'all'", []
    if month is None:
        return "period_grain = 'year' AND period_year = ?", [int(year)]
    return "period_grain = 'month' AND period_year = ? AND period_month = ?", [int(year), int(month)]


def minister_rollup(conn: duckdb.DuckDBPyConnection, year: int | None = None, month: int | None = None) -> QueryResult:
    """Per-minister meeting counts + date span + portfolio for one period grain."""
    where, params = _period_where(year, month)
    return _run(
        conn,
        "SELECT minister, meetings, first_meeting, last_meeting, depts "
        f"FROM v_ministerial_diary_minister_period WHERE {where} ORDER BY meetings DESC, minister",
        params,
    )


def dept_rollup(conn: duckdb.DuckDBPyConnection, year: int | None = None, month: int | None = None) -> QueryResult:
    """Per-department meeting counts + distinct named ministers for one period grain."""
    where, params = _period_where(year, month)
    return _run(
        conn,
        "SELECT department, meetings, ministers, first_meeting, last_meeting "
        f"FROM v_ministerial_diary_dept_period WHERE {where} ORDER BY meetings DESC, department",
        params,
    )


def dept_minister_rollup(
    conn: duckdb.DuckDBPyConnection, dept: str, year: int | None = None, month: int | None = None
) -> QueryResult:
    """One department's ministers (current + former) by meetings logged, for one period grain.
    ``depts`` is each minister's FULL portfolio in the period (for the card badges)."""
    where, params = _period_where(year, month)
    return _run(
        conn,
        "SELECT minister, meetings, first_meeting, last_meeting, depts "
        f"FROM v_ministerial_diary_dept_minister_period WHERE department = ? AND {where} "
        "ORDER BY meetings DESC, minister",
        [dept, *params],
    )


def top_orgs(
    conn: duckdb.DuckDBPyConnection,
    entity_kind: str,
    year: int | None = None,
    month: int | None = None,
    top: int = 3,
) -> QueryResult:
    """Most-named organisations per minister or per department ('Most-met' card context) for
    one period grain. entity_kind ∈ {'minister', 'department'}; rnk is precomputed in the view."""
    kind = entity_kind if entity_kind in {"minister", "department"} else "minister"
    where, params = _period_where(year, month)
    return _run(
        conn,
        "SELECT entity, organisation, n, rnk FROM v_ministerial_diary_top_orgs "
        f"WHERE entity_kind = ? AND {where} AND rnk <= ? ORDER BY entity, rnk",
        [kind, *params, int(top)],
    )


# ── Parameterised retrieval for the MCP/API surface (no Streamlit page to facet) ──────────


def org_overlap_ranked(conn: duckdb.DuckDBPyConnection, limit: int = 25, outside_only: bool = True) -> QueryResult:
    """Organisations ranked by how many meetings ministers logged with them. ``outside_only``
    drops state/semi-state bodies (the page leads with outside interests)."""
    where = " WHERE NOT is_state_body" if outside_only else ""
    return _run(
        conn,
        "SELECT organisation, sector, is_state_body, meetings, ministers_met, "
        "ministers_lobbied_and_met, total_lobbying_returns, corroborated, first_meeting, last_meeting "
        f"FROM v_ministerial_diary_org_overlap{where} ORDER BY meetings DESC, ministers_met DESC LIMIT ?",
        [int(limit)],
    )


def organisation_summary(conn: duckdb.DuckDBPyConnection, name: str) -> QueryResult:
    """Overlap summary row(s) for one organisation (fuzzy name match)."""
    return _run(
        conn,
        "SELECT organisation, sector, is_state_body, meetings, ministers_met, "
        "ministers_lobbied_and_met, total_lobbying_returns, corroborated, first_meeting, last_meeting "
        "FROM v_ministerial_diary_org_overlap WHERE lower(organisation) LIKE ? ORDER BY meetings DESC LIMIT 5",
        [f"%{name.lower()}%"],
    )


def organisation_meetings(conn: duckdb.DuckDBPyConnection, name: str, limit: int = 40) -> QueryResult:
    """Individual logged meetings naming an organisation (which minister, when, the subject)."""
    return _run(
        conn,
        "SELECT minister, department, entry_date, subject, source_pdf_url "
        "FROM v_ministerial_diary_engagements WHERE lower(organisation) LIKE ? "
        "ORDER BY entry_date DESC LIMIT ?",
        [f"%{name.lower()}%", int(limit)],
    )


def meeting_search(
    conn: duckdb.DuckDBPyConnection, minister: str = "", topic: str = "", limit: int = 30
) -> QueryResult:
    """Search every logged external meeting by minister surname and/or a subject keyword."""
    sql = (
        "SELECT minister, department, entry_date, subject, source_pdf_url FROM v_ministerial_diary_meetings WHERE TRUE"
    )
    params: list = []
    if minister:
        sql += " AND lower(coalesce(minister, '')) LIKE ?"
        params.append(f"%{minister.lower()}%")
    if topic:
        sql += " AND lower(subject) LIKE ?"
        params.append(f"%{topic.lower()}%")
    sql += " ORDER BY entry_date DESC LIMIT ?"
    params.append(int(limit))
    return _run(conn, sql, params)


# ── Access × money cross-reference (v_ministerial_diary_company_influence) ─────────────────


def company_influence(conn: duckdb.DuckDBPyConnection, name: str) -> QueryResult:
    """One company's access×money profile (fuzzy name): meetings, ministers met, lobbying
    returns, contracts won (€) and public payments (€), with the matched supplier name."""
    return _run(
        conn,
        "SELECT * FROM v_ministerial_diary_company_influence WHERE lower(organisation) LIKE ? "
        "ORDER BY awards_eur DESC, meetings DESC LIMIT 10",
        [f"%{name.lower()}%"],
    )


def access_to_contracts(conn: duckdb.DuckDBPyConnection, limit: int = 25, order_by: str = "awards_eur") -> QueryResult:
    """Companies that met ministers AND won/were paid public money, ranked. order_by ∈
    {awards_eur, paid_eur, meetings, total_lobbying_returns}."""
    col = order_by if order_by in {"awards_eur", "paid_eur", "meetings", "total_lobbying_returns"} else "awards_eur"
    return _run(
        conn,
        "SELECT organisation, sector, meetings, ministers_met, total_lobbying_returns, corroborated, "
        "n_awards, awards_eur, paid_eur, matched_supplier "
        f"FROM v_ministerial_diary_company_influence WHERE won_public_money ORDER BY {col} DESC LIMIT ?",
        [int(limit)],
    )
