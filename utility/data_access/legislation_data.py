"""
Legislation data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/legislation_*.sql
- All retrieval SQL for the legislation page (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL
- CREATE VIEW / CREATE TABLE
- pandas groupby, merge, pivot
- Business metric definitions
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views


@st.cache_resource
def get_legislation_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["legislation_*.sql"], swallow_errors=True)
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        conn = get_legislation_conn()
        return conn.execute(sql, params or []).df()
    except Exception:
        return pd.DataFrame()


# ── Index ──────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_legislation_index_filtered(
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    title_search: str | None = None,
) -> pd.DataFrame:
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
    return _safe(
        f"SELECT bill_id, bill_title, bill_status, bill_type, sponsor,"
        f" introduced_date, current_stage, stage_number, oireachtas_url, bill_no, bill_year,"
        f" bill_phase"
        f" FROM v_legislation_index{where}"
        f" ORDER BY introduced_date DESC NULLS LAST",
        params or None,
    )


@st.cache_data(ttl=300)
def fetch_all_statuses() -> list[str]:
    df = _safe(
        "SELECT DISTINCT bill_status FROM v_legislation_index"
        " WHERE bill_status IS NOT NULL AND bill_status != '—'"
        " ORDER BY bill_status"
    )
    return df["bill_status"].tolist() if not df.empty else []


# ── Detail ─────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_detail(bill_id: str) -> pd.DataFrame:
    return _safe(
        "SELECT * FROM v_legislation_detail WHERE bill_id = ? LIMIT 1",
        [bill_id],
    )


# ── Timeline ───────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_timeline(bill_id: str) -> pd.DataFrame:
    return _safe(
        "SELECT stage_name, stage_date, stage_number, is_current_stage, chamber"
        " FROM v_legislation_timeline WHERE bill_id = ?"
        " ORDER BY stage_number ASC NULLS LAST, stage_date ASC NULLS LAST",
        [bill_id],
    )


# ── Sources ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_sources(bill_id: str) -> pd.DataFrame:
    return _safe(
        "SELECT * FROM v_legislation_sources WHERE bill_id = ? LIMIT 1",
        [bill_id],
    )


# ── PDF documents ──────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_pdfs(bill_id: str) -> pd.DataFrame:
    """All Oireachtas-issued PDFs for a bill: versions, related docs, amendments.

    One row per PDF, ordered by category (versions → related_docs → amendments)
    then by pdf_date descending. Returns columns:
        pdf_category, pdf_subtype, pdf_label, pdf_url, pdf_date, pdf_lang
    """
    return _safe(
        "SELECT pdf_category, pdf_subtype, pdf_label, pdf_url, pdf_date, pdf_lang"
        " FROM v_legislation_pdfs WHERE bill_id = ?"
        " ORDER BY category_order, pdf_date DESC NULLS LAST, pdf_label",
        [bill_id],
    )


# ── Debates ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_debates(bill_id: str) -> pd.DataFrame:
    return _safe(
        "SELECT debate_date, debate_title, debate_url, chamber"
        " FROM v_legislation_debates WHERE bill_id = ?"
        " ORDER BY debate_date ASC NULLS LAST",
        [bill_id],
    )


# ── Pre-2014 primary Acts (curated table) ─────────────────────────────────────


@st.cache_data(ttl=3600)
def fetch_pre2014_act_detail(bill_id: str) -> dict:
    """Return hero info for a synthetic 'act_<year>_<slug>' bill_id by
    selecting from v_legislation_pre2014_acts. Returns {} on miss."""
    if not (isinstance(bill_id, str) and bill_id.startswith("act_")):
        return {}
    rows = _safe(
        "SELECT act_short_title, act_year, policy_domain"
        " FROM v_legislation_pre2014_acts WHERE canonical_bill_id = ? LIMIT 1",
        [bill_id],
    )
    if rows.empty:
        return {}
    r = rows.iloc[0]
    return {
        "act_short_title": str(r.get("act_short_title") or ""),
        "act_year": int(r.get("act_year") or 0),
        "policy_domain": str(r.get("policy_domain") or ""),
    }


# ── Statutory Instruments under a bill ────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_si_composition(bill_id: str) -> pd.DataFrame:
    """Operation-mix summary for the 'composition sentence' above the SI list.

    Reads from v_bill_si_operation_mix — the GROUP BY now lives in the view.
    """
    return _safe(
        "SELECT si_operation, n FROM v_bill_si_operation_mix WHERE bill_id = ? ORDER BY n DESC",
        [bill_id],
    )


@st.cache_data(ttl=300)
def fetch_si_freshness(bill_id: str) -> dict:
    """Total + first/last SI date + EU share for the freshness line."""
    df = _safe(
        "SELECT MIN(si_signed_date) AS first_si,"
        " MAX(si_signed_date) AS last_si,"
        " COUNT(*) AS total,"
        " SUM(CASE WHEN si_is_eu THEN 1 ELSE 0 END) AS eu_count"
        " FROM v_bill_statutory_instruments WHERE bill_id = ?",
        [bill_id],
    )
    if df.empty or int(df.iloc[0]["total"] or 0) == 0:
        return {}
    r = df.iloc[0]
    return {
        "first_si": r["first_si"],
        "last_si": r["last_si"],
        "total": int(r["total"] or 0),
        "eu_count": int(r["eu_count"] or 0),
    }


@st.cache_data(ttl=300)
def fetch_si_years_for_bill(bill_id: str) -> list[int]:
    df = _safe(
        "SELECT DISTINCT si_year FROM v_bill_statutory_instruments WHERE bill_id = ? ORDER BY si_year DESC",
        [bill_id],
    )
    return [int(y) for y in df["si_year"].dropna().tolist()] if not df.empty else []


@st.cache_data(ttl=300)
def fetch_si_by_bill(
    bill_id: str,
    year: int | None = None,
    operation: str | None = None,
    eu_only: bool = False,
) -> pd.DataFrame:
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
    return _safe(
        "SELECT si_year, si_number, si_id, si_title, si_signed_date,"
        " si_minister, si_minister_named, si_policy_domain, si_operation,"
        " si_form, si_is_eu, eisb_url"
        " FROM v_bill_statutory_instruments"
        f" WHERE {' AND '.join(clauses)}"
        " ORDER BY si_signed_date DESC NULLS LAST",
        params,
    )


# ── Statutory Instruments — first-class entity (v_statutory_instruments) ──────
#
# Backs the standalone Statutory Instruments page. Distinct from the
# fetch_si_*_bill functions above: those are bill-gated (SIs under one Act);
# this browses the full SI universe (~5,900 SIs, 2016+), bill link optional.
# The page filters / facets / KPIs in pandas off this single frame.


@st.cache_data(ttl=300)
def fetch_si_entity_index() -> pd.DataFrame:
    """Every Statutory Instrument as a row — the full v_statutory_instruments
    view. One registered analytical surface; the page does its filtering and
    facet derivation in pandas off this frame."""
    return _safe("SELECT * FROM v_statutory_instruments")


@st.cache_data(ttl=300)
def fetch_si_amendments_made(si_year: int, si_number: int) -> pd.DataFrame:
    """The instruments THIS SI amends/revokes — the forward direction of the
    SI→SI amendment graph (v_si_amendments). The reverse direction ("amended /
    revoked BY …") is already surfaced by the legal-status block from
    affecting_sis, so the detail panel renders only this side to avoid
    duplication. Plain SELECT off the view; the inversion/JOIN lives in
    v_si_amendments, keeping this within the retrieval-only contract."""
    return _safe(
        "SELECT effect, affected_number, affected_year, affected_title, affected_eli_url, provision_note "
        "FROM v_si_amendments WHERE amender_number = ? AND amender_year = ? "
        "ORDER BY affected_year DESC, affected_number DESC",
        [si_number, si_year],
    )
