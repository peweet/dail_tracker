"""
Payments data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/payments_*.sql
- All retrieval SQL for the payments page (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL
- CREATE VIEW / CREATE TABLE
- read_parquet / read_csv from inside this module (pipeline writes the views;
  this module only SELECTs from them)
- pandas groupby, merge, pivot
- Business metric definitions
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SQL_VIEWS = _PROJECT_ROOT / "sql_views"


def _absolutize_data_paths(sql: str) -> str:
    # SQL views use literals like read_parquet('data/gold/parquet/...').
    # DuckDB resolves those against CWD, so a Streamlit launch from utility/
    # breaks queries. Rewrite to absolute project paths at registration time.
    return sql.replace("'data/", f"'{_PROJECT_ROOT.as_posix()}/data/")


@st.cache_resource
def get_payments_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("payments_*.sql")):
        conn.execute(_absolutize_data_paths(sql_file.read_text(encoding="utf-8")))
    return conn


@st.cache_data(ttl=300)
def fetch_payments_summary() -> pd.Series:
    row = (
        get_payments_conn()
        .execute(
            "SELECT members_count, payment_count, total_paid,"
            " first_payment_date, last_payment_date, first_year, last_year,"
            " source_summary, latest_fetch_timestamp_utc, mart_version, code_version"
            " FROM v_payments_summary LIMIT 1"
        )
        .df()
    )
    return row.iloc[0] if not row.empty else pd.Series()


@st.cache_data(ttl=300)
def fetch_filter_options() -> dict[str, list]:
    conn = get_payments_conn()
    members = conn.execute(
        "SELECT DISTINCT member_name FROM v_payments_member_detail ORDER BY member_name LIMIT 2000"
    ).fetchall()
    years = conn.execute(
        "SELECT DISTINCT payment_year FROM v_payments_yearly_evolution ORDER BY payment_year DESC LIMIT 50"
    ).fetchall()
    return {
        "members": [r[0] for r in members],
        "years": [str(r[0]) for r in years],
    }


@st.cache_data(ttl=300)
def fetch_year_ranking(year: int) -> pd.DataFrame:
    return (
        get_payments_conn()
        .execute(
            "SELECT member_name, position, party_name, constituency,"
            " taa_band_label, total_paid, payment_count, rank_high,"
            " year_total_paid, year_member_count, year_avg_per_td"
            " FROM v_payments_yearly_evolution"
            " WHERE payment_year = ?"
            " ORDER BY rank_high ASC",
            [year],
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_member_all_years(member_name: str, unique_member_code: str | None = None) -> pd.DataFrame:
    """All years for a member — used for the all-years summary table, chart, and all-time total.

    Prefers ``unique_member_code`` when provided (post-enrichment join key);
    falls back to ``member_name`` string match for legacy callers (the stand-
    alone /rankings-payments page populates its picker from the parquet's
    native "Last, First" format, where member_name is the working key).
    """
    if unique_member_code:
        return (
            get_payments_conn()
            .execute(
                "SELECT payment_year, total_paid, payment_count, rank_high,"
                " taa_band_label, position, party_name, constituency, member_alltime_total"
                " FROM v_payments_yearly_evolution"
                " WHERE unique_member_code = ?"
                " ORDER BY payment_year DESC",
                [unique_member_code],
            )
            .df()
        )
    return (
        get_payments_conn()
        .execute(
            "SELECT payment_year, total_paid, payment_count, rank_high,"
            " taa_band_label, position, party_name, constituency, member_alltime_total"
            " FROM v_payments_yearly_evolution"
            " WHERE member_name = ?"
            " ORDER BY payment_year DESC",
            [member_name],
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_member_year_summary(
    member_name: str, year: int, unique_member_code: str | None = None
) -> pd.DataFrame:
    """Single row for a member+year — summary metrics."""
    if unique_member_code:
        return (
            get_payments_conn()
            .execute(
                "SELECT member_name, position, party_name, constituency,"
                " taa_band_label, total_paid, payment_count, rank_high"
                " FROM v_payments_yearly_evolution"
                " WHERE unique_member_code = ? AND payment_year = ? LIMIT 1",
                [unique_member_code, year],
            )
            .df()
        )
    return (
        get_payments_conn()
        .execute(
            "SELECT member_name, position, party_name, constituency,"
            " taa_band_label, total_paid, payment_count, rank_high"
            " FROM v_payments_yearly_evolution"
            " WHERE member_name = ? AND payment_year = ? LIMIT 1",
            [member_name, year],
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_member_payments(
    member_name: str, year: int, unique_member_code: str | None = None
) -> pd.DataFrame:
    """Individual payment transactions for a member+year — the audit trail."""
    if unique_member_code:
        return (
            get_payments_conn()
            .execute(
                "SELECT date_paid, narrative, amount_num, taa_band_label"
                " FROM v_payments_member_detail"
                " WHERE unique_member_code = ? AND payment_year = ?"
                " ORDER BY date_paid ASC, narrative ASC",
                [unique_member_code, year],
            )
            .df()
        )
    return (
        get_payments_conn()
        .execute(
            "SELECT date_paid, narrative, amount_num, taa_band_label"
            " FROM v_payments_member_detail"
            " WHERE member_name = ? AND payment_year = ?"
            " ORDER BY date_paid ASC, narrative ASC",
            [member_name, year],
        )
        .df()
    )


@st.cache_data(ttl=3600)
def fetch_alltime_ranking() -> pd.DataFrame:
    """All-time PSA ranking since 2020 from v_payments_alltime_ranking.

    Returns the full ranked list of every member with PSA payments since
    2020. Columns: member_name, position, party_name, constituency,
    taa_band_label, total_paid_since_2020, payment_count_since_2020,
    earliest_year, latest_year, rank_high.

    Audit fix (2026-05-26): replaced direct parquet read +
    schema-mismatched column lookup. The parquet's schema had drifted
    (`member_name` → `identifier` slug) and every Rankings card rendered
    "—". The pipeline now owns the ranking via v_payments_alltime_ranking.
    """
    return (
        get_payments_conn()
        .execute(
            "SELECT member_name, position, party_name, constituency,"
            " taa_band_label, total_paid_since_2020, payment_count_since_2020,"
            " earliest_year, latest_year, rank_high"
            " FROM v_payments_alltime_ranking"
            " ORDER BY rank_high ASC"
        )
        .df()
    )


@st.cache_data(ttl=3600)
def fetch_since_2020_summary() -> dict[str, float | int]:
    """Summary stats (total / member-count / avg) from v_payments_alltime_summary.

    Audit fix (2026-05-26): replaced direct parquet read +
    ``pl.read_parquet(...).sum()`` / ``.n_unique()`` in Streamlit, which
    violated the page contract (no read_parquet, SUM not in allowed
    aggregates). The aggregation now lives in v_payments_alltime_summary.
    """
    row = (
        get_payments_conn()
        .execute(
            "SELECT total_paid_since_2020, member_count, avg_per_td_since_2020"
            " FROM v_payments_alltime_summary LIMIT 1"
        )
        .fetchone()
    )
    if not row:
        return {"total": 0.0, "members": 0, "avg_per_td": 0.0}
    return {
        "total": float(row[0] or 0.0),
        "members": int(row[1] or 0),
        "avg_per_td": float(row[2] or 0.0),
    }
