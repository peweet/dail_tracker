"""
SIPO election-EXPENSES data access layer (companion to sipo_donations_data.py).

Owns:
- DuckDB connection bootstrapped from sql_views/sipo_*.sql
- Retrieval SQL for the Election-Expenses lens of the Payments page
  (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as the page files):
- JOIN, GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL (rollups live in the views)
- CREATE VIEW / CREATE TABLE, read_parquet / read_csv, pandas groupby/merge/pivot,
  business-metric definitions.

What this is: the national-agent "expenditure on the candidate" column (Part 3) of
each party's GE2024 Election Expenses Statement — money SPENT campaigning, NOT
donations. It is per-candidate-allocated spend, so it UNDER-counts parties that book
spend centrally (Sinn Féin etc.); it is not a party's total campaign outlay.
No-inference: figures + source only; OCR-derived rows carry a "verify vs SIPO PDF" mark.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views


@st.cache_resource
def get_expenses_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["sipo_*.sql"], swallow_errors=False)
    return conn


@st.cache_data(ttl=300)
def fetch_expenses_totals() -> dict[str, float | int]:
    """Headline totals across all parties (aggregates the rollup view; no GROUP BY here)."""
    row = (
        get_expenses_conn()
        .execute(
            "SELECT SUM(total_expenditure), SUM(candidate_count), COUNT(*),"
            " SUM(excluded_count) FROM v_sipo_expenses_by_party"
        )
        .fetchone()
    )
    if not row:
        return {"total": 0.0, "candidates": 0, "parties": 0, "excluded": 0}
    return {
        "total": float(row[0] or 0.0),
        "candidates": int(row[1] or 0),
        "parties": int(row[2] or 0),
        "excluded": int(row[3] or 0),
    }


@st.cache_data(ttl=300)
def fetch_expenses_by_party() -> pd.DataFrame:
    """One row per party — drives the Election-Expenses cards."""
    return (
        get_expenses_conn()
        .execute(
            "SELECT party, candidate_count, total_expenditure, max_expenditure,"
            " verify_count, excluded_count"
            " FROM v_sipo_expenses_by_party"
            " ORDER BY total_expenditure DESC"
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_party_candidates(party: str) -> pd.DataFrame:
    """Per-candidate expenditure for one party — name, constituency, amount, flag."""
    return (
        get_expenses_conn()
        .execute(
            "SELECT candidate_name, constituency, expenditure_eur, flag,"
            " is_verified, source_page"
            " FROM v_sipo_expenses_base"
            " WHERE party = ?"
            " ORDER BY (flag = 'over_limit_verify'), expenditure_eur DESC",
            [party],
        )
        .df()
    )
