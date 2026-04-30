"""
Payments data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/payments_*.sql
- All retrieval SQL for the payments page (SELECT / WHERE / ORDER BY / LIMIT only)
- fetch_since_2020_summary: reads the pipeline silver parquet directly (Polars)

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
import polars as pl
import streamlit as st

from config import GOLD_PARQUET_DIR

_SQL_VIEWS = Path(__file__).resolve().parents[2] / "sql_views"


@st.cache_resource
def get_payments_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("payments_*.sql")):
        conn.execute(sql_file.read_text(encoding="utf-8"))
    return conn


@st.cache_data(ttl=300)
def fetch_payments_summary() -> pd.Series:
    row = get_payments_conn().execute(
        "SELECT members_count, payment_count, total_paid,"
        " first_payment_date, last_payment_date, first_year, last_year,"
        " source_summary, latest_fetch_timestamp_utc, mart_version, code_version"
        " FROM v_payments_summary LIMIT 1"
    ).df()
    return row.iloc[0] if not row.empty else pd.Series()


@st.cache_data(ttl=300)
def fetch_filter_options() -> dict[str, list]:
    conn = get_payments_conn()
    members = conn.execute(
        "SELECT DISTINCT member_name FROM v_payments_member_detail"
        " ORDER BY member_name LIMIT 2000"
    ).fetchall()
    years = conn.execute(
        "SELECT DISTINCT payment_year FROM v_payments_yearly_evolution"
        " ORDER BY payment_year DESC LIMIT 50"
    ).fetchall()
    return {
        "members": [r[0] for r in members],
        "years":   [str(r[0]) for r in years],
    }


@st.cache_data(ttl=300)
def fetch_year_ranking(year: int) -> pd.DataFrame:
    return get_payments_conn().execute(
        "SELECT member_name, position, party_name, constituency,"
        " taa_band_label, total_paid, payment_count, rank_high,"
        " year_total_paid, year_member_count, year_avg_per_td"
        " FROM v_payments_yearly_evolution"
        " WHERE payment_year = ?"
        " ORDER BY rank_high ASC",
        [year],
    ).df()


@st.cache_data(ttl=300)
def fetch_member_all_years(member_name: str) -> pd.DataFrame:
    """All years for a member — used for the all-years summary table, chart, and all-time total."""
    return get_payments_conn().execute(
        "SELECT payment_year, total_paid, payment_count, rank_high,"
        " taa_band_label, position, party_name, constituency, member_alltime_total"
        " FROM v_payments_yearly_evolution"
        " WHERE member_name = ?"
        " ORDER BY payment_year DESC",
        [member_name],
    ).df()


@st.cache_data(ttl=300)
def fetch_member_year_summary(member_name: str, year: int) -> pd.DataFrame:
    """Single row for a member+year — summary metrics."""
    return get_payments_conn().execute(
        "SELECT member_name, position, party_name, constituency,"
        " taa_band_label, total_paid, payment_count, rank_high"
        " FROM v_payments_yearly_evolution"
        " WHERE member_name = ? AND payment_year = ? LIMIT 1",
        [member_name, year],
    ).df()


@st.cache_data(ttl=300)
def fetch_member_payments(member_name: str, year: int) -> pd.DataFrame:
    """Individual payment transactions for a member+year — the audit trail."""
    return get_payments_conn().execute(
        "SELECT date_paid, narrative, amount_num, taa_band_label"
        " FROM v_payments_member_detail"
        " WHERE member_name = ? AND payment_year = ?"
        " ORDER BY date_paid ASC, narrative ASC",
        [member_name, year],
    ).df()


@st.cache_data(ttl=3600)
def fetch_alltime_ranking() -> pd.DataFrame:
    """Current 34th Dáil TDs ranked by total PSA received since 2020, deduplicated."""
    path = GOLD_PARQUET_DIR / "current_td_payment_rankings.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pl.read_parquet(path).to_pandas()
    if "total_amount_paid_since_2020" not in df.columns:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=3600)
def fetch_since_2020_summary() -> dict[str, float | int]:
    """Summary stats from the current TD payment rankings (34th Dáil only)."""
    path = GOLD_PARQUET_DIR / "current_td_payment_rankings.parquet"
    if not path.exists():
        return {"total": 0.0, "members": 0, "avg_per_td": 0.0}
    df = pl.read_parquet(path)
    total   = float(df["total_amount_paid_since_2020"].sum())
    members = int(df["join_key"].n_unique())
    avg     = total / members if members else 0.0
    return {"total": total, "members": members, "avg_per_td": avg}
