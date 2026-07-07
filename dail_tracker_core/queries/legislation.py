"""Legislation + Statutory-Instruments retrieval — Streamlit-free.

Backs both the legislation page (bill index / detail / timeline / amendments /
sources / PDFs / debates / pre-2014 Acts / SIs-under-a-bill) and the standalone
statutory-instruments page (the full SI universe + the SI→SI amendment graph).
Moved verbatim from ``utility/data_access/legislation_data.py``.

Pure retrieval (SELECT / WHERE / ORDER BY / LIMIT) returning ``QueryResult`` — the
old ``_safe`` swallow-to-empty is replaced by the 3-state result; the thin
Streamlit wrapper flattens ``.data`` (empty on unavailable) and keeps the
dict/list shaping + the ``v_bill_amendment_intensity`` column projections the
page contract expects.

The amendment-intensity figures count published amendment-LIST documents per
stage (numbered + cream lists), not individual amendments — a faithful
contestation signal, never framed as a clause count.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_VIEW = "v_bill_amendment_intensity"
_COLS = (
    "bill_id, bill_title, bill_type, bill_status, amendment_lists, distinct_stages,"
    " committee_lists, report_lists, cream_lists, dail_lists, seanad_lists,"
    " first_amendment_date, last_amendment_date"
)

MOST_CONTESTED_LIMIT = 50


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="legislation", log=_log)


# ── Bill index ────────────────────────────────────────────────────────────────


def index_filtered(
    conn: duckdb.DuckDBPyConnection,
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    title_search: str | None = None,
) -> QueryResult:
    clauses: list[str] = []
    params: list = []
    if start_date and end_date:
        clauses.append("introduced_date BETWEEN ? AND ?")
        params.extend([start_date, end_date])
    if status:
        clauses.append("bill_status = ?")
        params.append(status)
    if title_search:
        clauses.append("bill_title ILIKE ?")
        params.append(f"%{title_search}%")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return _run(
        conn,
        "SELECT bill_id, bill_title, bill_status, bill_type, sponsor,"
        " introduced_date, current_stage, stage_number, oireachtas_url, bill_no, bill_year,"
        " bill_phase"
        f" FROM v_legislation_index{where}"
        " ORDER BY introduced_date DESC NULLS LAST",
        params or None,
    )


def distinct_statuses(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT bill_status FROM v_legislation_index"
        " WHERE bill_status IS NOT NULL AND bill_status != '—'"
        " ORDER BY bill_status",
    )


# ── Bill detail / amendments / timeline / sources / PDFs / debates ────────────


def bill_detail(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_legislation_detail WHERE bill_id = ? LIMIT 1", [bill_id])


def most_contested_bills(conn: duckdb.DuckDBPyConnection, limit: int = MOST_CONTESTED_LIMIT) -> QueryResult:
    """Bills ranked by amendment-list activity (most contested first)."""
    return _run(
        conn,
        f"SELECT {_COLS} FROM {_VIEW} ORDER BY amendment_lists DESC, bill_id LIMIT ?",
        [limit],
    )


def amendment_intensity_for_bill(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """Amendment activity for one bill. Empty (success, no rows) when none."""
    return _run(conn, f"SELECT {_COLS} FROM {_VIEW} WHERE bill_id = ? LIMIT 1", [bill_id])


def bill_timeline(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    return _run(
        conn,
        "SELECT stage_name, stage_date, stage_number, is_current_stage, chamber"
        " FROM v_legislation_timeline WHERE bill_id = ?"
        " ORDER BY stage_number ASC NULLS LAST, stage_date ASC NULLS LAST",
        [bill_id],
    )


def bill_sources(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_legislation_sources WHERE bill_id = ? LIMIT 1", [bill_id])


def bill_pdfs(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """All Oireachtas-issued PDFs for a bill (versions → related → amendments)."""
    return _run(
        conn,
        "SELECT pdf_category, pdf_subtype, pdf_label, pdf_url, pdf_date, pdf_lang"
        " FROM v_legislation_pdfs WHERE bill_id = ?"
        " ORDER BY category_order, pdf_date DESC NULLS LAST, pdf_label",
        [bill_id],
    )


def bill_debates(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    return _run(
        conn,
        "SELECT debate_date, debate_title, debate_url, chamber"
        " FROM v_legislation_debates WHERE bill_id = ?"
        " ORDER BY debate_date ASC NULLS LAST",
        [bill_id],
    )


# ── Pre-2014 primary Acts (curated table) ─────────────────────────────────────


def pre2014_act(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """Hero info for a synthetic ``act_<year>_<slug>`` bill_id (page guards the
    ``act_`` prefix + builds the dict)."""
    return _run(
        conn,
        "SELECT act_short_title, act_year, policy_domain"
        " FROM v_legislation_pre2014_acts WHERE canonical_bill_id = ? LIMIT 1",
        [bill_id],
    )


# ── Statutory Instruments under a bill ────────────────────────────────────────


def si_composition(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """Operation-mix summary for the composition sentence (GROUP BY lives in view)."""
    return _run(
        conn,
        "SELECT si_operation, n FROM v_bill_si_operation_mix WHERE bill_id = ? ORDER BY n DESC",
        [bill_id],
    )


def si_freshness(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """One-row total + first/last SI date + EU count for the freshness line."""
    return _run(
        conn,
        "SELECT MIN(si_signed_date) AS first_si,"
        " MAX(si_signed_date) AS last_si,"
        " COUNT(*) AS total,"
        " SUM(CASE WHEN si_is_eu THEN 1 ELSE 0 END) AS eu_count"
        " FROM v_bill_statutory_instruments WHERE bill_id = ?",
        [bill_id],
    )


def si_years_for_bill(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT si_year FROM v_bill_statutory_instruments WHERE bill_id = ? ORDER BY si_year DESC",
        [bill_id],
    )


def si_by_bill(
    conn: duckdb.DuckDBPyConnection,
    bill_id: str,
    year: int | None = None,
    operation: str | None = None,
    eu_only: bool = False,
) -> QueryResult:
    clauses = ["bill_id = ?"]
    params: list = [bill_id]
    if year is not None:
        clauses.append("si_year = ?")
        params.append(year)
    if operation:
        clauses.append("si_operation = ?")
        params.append(operation)
    if eu_only:
        clauses.append("si_is_eu = TRUE")
    return _run(
        conn,
        "SELECT si_year, si_number, si_id, si_title, si_signed_date,"
        " si_minister, si_minister_named, si_policy_domain, si_operation,"
        " si_form, si_is_eu, eisb_url"
        " FROM v_bill_statutory_instruments"
        f" WHERE {' AND '.join(clauses)}"
        " ORDER BY si_signed_date DESC NULLS LAST",
        params,
    )


def act_commencement(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """Commencement-order timeline for an Act — one row per commencement SI made
    under it (exact bill match only). A commencement HISTORY, not a consolidated
    in-force status. Empty (success, no rows) when the Act has no commencement
    order (likely self-executing)."""
    return _run(
        conn,
        "SELECT si_id, si_year, si_number, si_title, si_commenced_sections,"
        " si_signed_date, si_minister_name, si_minister_member_code,"
        " si_responsible_actor, si_department_label, order_current_state, eisb_url"
        " FROM v_act_commencement WHERE bill_id = ?"
        " ORDER BY si_signed_date ASC NULLS LAST, si_number",
        [bill_id],
    )


# ── Statutory Instruments — first-class entity (full universe) ────────────────


def si_entity_index(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every SI as a row (the page facets/filters in pandas off this frame)."""
    return _run(conn, "SELECT * FROM v_statutory_instruments")


def si_entity_index_classified(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """v_statutory_instruments + LRC subject classification (LEFT JOIN in the view).
    Unavailable when the LRC gold table is absent — the page falls back to the
    unclassified index."""
    return _run(conn, "SELECT * FROM v_statutory_instruments_classified")


def si_amendments_made(conn: duckdb.DuckDBPyConnection, si_year: int, si_number: int) -> QueryResult:
    """The instruments THIS SI amends/revokes (forward direction of the SI→SI graph)."""
    return _run(
        conn,
        "SELECT effect, affected_number, affected_year, affected_title, affected_eli_url, provision_note "
        "FROM v_si_amendments WHERE amender_number = ? AND amender_year = ? "
        "ORDER BY affected_year DESC, affected_number DESC",
        [si_number, si_year],
    )
