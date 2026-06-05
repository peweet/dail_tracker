"""Payments data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL (incl. the unique_member_code/member_name branch) and QueryResult
state-handling live in ``dail_tracker_core.queries.payments``; this file owns the
Streamlit caching and the small UI-shaping the page consumes:
  - fetch_payments_summary  -> a single pd.Series (the one summary row)
  - fetch_filter_options    -> {"members": [...], "years": [...str...]}
  - fetch_since_2020_summary-> {"total": float, "members": int, "avg_per_td": float}
All other fetchers return the DataFrame unchanged.

Return contracts are preserved exactly. Note the NULL handling in
fetch_since_2020_summary: the old code read the row via .fetchone() (SQL NULL ->
Python None -> ``None or 0.0`` -> 0.0). Reading via .df() turns NULL into NaN
(which is truthy), so we coerce with an explicit pd.notna guard to reproduce the
old None->0 behaviour exactly.

Forbidden here (unchanged): JOIN/GROUP BY/HAVING/WINDOW in ad-hoc SQL,
CREATE VIEW, read_parquet/read_csv, pandas groupby/merge/pivot, business metrics.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import payments as _q


@st.cache_resource
def get_payments_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["payments_*.sql"], swallow_errors=False)


@st.cache_data(ttl=300)
def fetch_payments_summary() -> pd.Series:
    df = _q.summary(get_payments_conn()).data
    return df.iloc[0] if not df.empty else pd.Series()


@st.cache_data(ttl=300)
def fetch_filter_options(house: str = "Dáil") -> dict[str, list]:
    conn = get_payments_conn()
    members_df = _q.member_options(conn, house).data
    years_df = _q.year_options(conn, house).data
    return {
        "members": members_df["member_name"].tolist() if not members_df.empty else [],
        "years": [str(y) for y in years_df["payment_year"].tolist()] if not years_df.empty else [],
    }


@st.cache_data(ttl=300)
def fetch_year_ranking(year: int, house: str = "Dáil") -> pd.DataFrame:
    return _q.year_ranking(get_payments_conn(), year, house).data


@st.cache_data(ttl=300)
def fetch_member_all_years(member_name: str, unique_member_code: str | None = None) -> pd.DataFrame:
    """All years for a member — all-years summary table, chart, and all-time total.

    Prefers ``unique_member_code`` when provided (post-enrichment join key);
    falls back to ``member_name`` string match for legacy callers."""
    return _q.member_all_years(get_payments_conn(), member_name, unique_member_code).data


@st.cache_data(ttl=300)
def fetch_member_year_summary(member_name: str, year: int, unique_member_code: str | None = None) -> pd.DataFrame:
    """Single row for a member+year — summary metrics."""
    return _q.member_year_summary(get_payments_conn(), member_name, year, unique_member_code).data


@st.cache_data(ttl=300)
def fetch_member_payments(member_name: str, year: int, unique_member_code: str | None = None) -> pd.DataFrame:
    """Individual payment transactions for a member+year — the audit trail."""
    return _q.member_payments(get_payments_conn(), member_name, year, unique_member_code).data


@st.cache_data(ttl=3600)
def fetch_alltime_ranking(house: str = "Dáil") -> pd.DataFrame:
    """All-time PSA ranking since 2020 from v_payments_alltime_ranking."""
    return _q.alltime_ranking(get_payments_conn(), house).data


@st.cache_data(ttl=3600)
def fetch_since_2020_summary(house: str = "Dáil") -> dict[str, float | int]:
    """Summary stats (total / member-count / avg) from v_payments_alltime_summary."""
    df = _q.alltime_summary(get_payments_conn(), house).data
    if df.empty:
        return {"total": 0.0, "members": 0, "avg_per_td": 0.0}
    r = df.iloc[0]

    def _f(v) -> float:
        return float(v) if pd.notna(v) else 0.0

    def _i(v) -> int:
        return int(v) if pd.notna(v) else 0

    return {
        "total": _f(r["total_paid_since_2020"]),
        "members": _i(r["member_count"]),
        "avg_per_td": _f(r["avg_per_td_since_2020"]),
    }
