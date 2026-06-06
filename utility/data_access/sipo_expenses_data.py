"""SIPO election-EXPENSES data access — thin Streamlit wrapper over dail_tracker_core.

Companion to sipo_donations_data.py. Retrieval SQL + QueryResult state-handling
live in ``dail_tracker_core.queries.sipo``; this file owns only the Streamlit
caching and the headline-totals dict shaping.

What this is: the national-agent "expenditure on the candidate" column (Part 3)
of each party's GE2024 Election Expenses Statement — money SPENT campaigning,
NOT donations. Per-candidate-allocated spend, so it UNDER-counts parties that
book spend centrally; it is not a party's total campaign outlay. No-inference:
figures + source only; OCR-derived rows carry a "verify vs SIPO PDF" mark.

Forbidden here (unchanged): read_parquet, parquet_scan, CREATE VIEW,
pandas groupby/merge/pivot business logic, multi-dim GROUP BY.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import sipo as _q


@st.cache_resource
def get_expenses_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["sipo_*.sql"], swallow_errors=False)


@st.cache_data(ttl=300)
def fetch_expenses_totals() -> dict[str, float | int]:
    """Headline totals across all parties (aggregates the rollup view; no GROUP BY here)."""
    r = _q.expenses_totals(get_expenses_conn())
    if not r.ok or r.is_empty:
        return {"total": 0.0, "candidates": 0, "parties": 0, "excluded": 0}
    row = r.data.iloc[0]
    # .df() yields NaN (truthy) where the old fetchone() gave SQL NULL -> None;
    # coerce NaN -> 0 so an empty/NULL aggregate matches the old None-or-0 path.
    return {
        "total": float(row["total_expenditure"]) if pd.notna(row["total_expenditure"]) else 0.0,
        "candidates": int(row["candidate_count"]) if pd.notna(row["candidate_count"]) else 0,
        "parties": int(row["parties"]) if pd.notna(row["parties"]) else 0,
        "excluded": int(row["excluded_count"]) if pd.notna(row["excluded_count"]) else 0,
    }


@st.cache_data(ttl=300)
def fetch_expenses_by_party() -> pd.DataFrame:
    """One row per party — drives the Election-Expenses cards."""
    return _q.expenses_by_party(get_expenses_conn()).data


@st.cache_data(ttl=300)
def fetch_party_candidates(party: str) -> pd.DataFrame:
    """Per-candidate expenditure for one party — name, constituency, amount, flag."""
    return _q.party_candidates(get_expenses_conn(), party).data
