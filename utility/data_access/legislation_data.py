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

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_SQL_VIEWS = Path(__file__).resolve().parents[2] / "sql_views"


@st.cache_resource
def get_legislation_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("legislation_*.sql")):
        conn.execute(sql_file.read_text(encoding="utf-8"))
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
        f" introduced_date, current_stage, oireachtas_url, bill_no, bill_year"
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
