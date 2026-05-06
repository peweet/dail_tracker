"""
============================================================================
  EXPERIMENTAL — DELETE ON INTEGRATION
============================================================================

Isolated data access for the sandbox preview page. Loads ONLY
sql_views/experimental_*.sql into its own DuckDB connection, so production
data access (legislation_data, payments_data) is unaffected.

Each view here reads from a sandbox-produced file rather than production
silver/gold:
    v_experimental_legislation_index    -> pipeline_sandbox/out/silver/sponsors.parquet
    v_experimental_legislation_detail   -> pipeline_sandbox/out/silver/sponsors.parquet
    v_experimental_legislation_timeline -> pipeline_sandbox/out/silver/stages.parquet
    v_experimental_legislation_debates  -> pipeline_sandbox/out/silver/debates.parquet
    v_experimental_payments_full_psa    -> data/gold/parquet/payments_full_psa.parquet

REMOVAL CHECKLIST when graduating either fix to production:
  - Delete this file.
  - Delete utility/pages_code/experimental_preview.py.
  - Delete sql_views/experimental_*.sql.
  - Remove the experimental_preview_page entry from utility/app.py.
"""
from __future__ import annotations

import contextlib
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_SQL_VIEWS = Path(__file__).resolve().parents[2] / "sql_views"


@st.cache_resource
def get_experimental_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("experimental_*.sql")):
        with contextlib.suppress(Exception):
            conn.execute(sql_file.read_text(encoding="utf-8"))
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return get_experimental_conn().execute(sql, params or []).df()
    except Exception:
        return pd.DataFrame()


# ── Legislation (unscoped) ──────────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_experimental_legislation_index() -> pd.DataFrame:
    return _safe("SELECT * FROM v_experimental_legislation_index")


@st.cache_data(ttl=300)
def fetch_experimental_legislation_source_breakdown() -> pd.DataFrame:
    return _safe(
        "SELECT source, COUNT(*) AS bills "
        "FROM v_experimental_legislation_index "
        "GROUP BY source ORDER BY bills DESC"
    )


@st.cache_data(ttl=300)
def fetch_experimental_legislation_phase_crosstab() -> pd.DataFrame:
    return _safe(
        "SELECT bill_phase, source, COUNT(*) AS bills "
        "FROM v_experimental_legislation_index "
        "GROUP BY bill_phase, source ORDER BY bill_phase, source"
    )


@st.cache_data(ttl=300)
def fetch_experimental_government_bills_sample(limit: int = 25) -> pd.DataFrame:
    return _safe(
        "SELECT bill_id, bill_title, bill_status, sponsor, origin_house,"
        " current_stage, bill_phase, introduced_date "
        "FROM v_experimental_legislation_index "
        "WHERE source = 'Government' "
        "ORDER BY introduced_date DESC NULLS LAST "
        f"LIMIT {int(limit)}"
    )


@st.cache_data(ttl=300)
def fetch_experimental_seanad_origin_sample(limit: int = 25) -> pd.DataFrame:
    return _safe(
        "SELECT bill_id, bill_title, source, origin_house, stage_number,"
        " bill_phase, current_stage, introduced_date "
        "FROM v_experimental_legislation_index "
        "WHERE origin_house ILIKE '%Seanad%' "
        "ORDER BY introduced_date DESC NULLS LAST "
        f"LIMIT {int(limit)}"
    )


@st.cache_data(ttl=300)
def fetch_experimental_government_bills_with_debates(limit: int = 15) -> pd.DataFrame:
    return _safe(
        "SELECT i.bill_id, i.bill_title, i.sponsor, i.bill_phase,"
        " COUNT(d.bill_id) AS debate_count "
        "FROM v_experimental_legislation_index i "
        "LEFT JOIN v_experimental_legislation_debates d USING (bill_id) "
        "WHERE i.source = 'Government' "
        "GROUP BY i.bill_id, i.bill_title, i.sponsor, i.bill_phase "
        "HAVING debate_count > 0 "
        "ORDER BY debate_count DESC "
        f"LIMIT {int(limit)}"
    )


# ── Payments (full PSA) ─────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_experimental_payments_summary() -> pd.DataFrame:
    return _safe(
        "SELECT COUNT(*) AS rows,"
        " COUNT(DISTINCT member_name) AS members,"
        " SUM(amount) AS total_paid,"
        " MIN(date_paid) AS first_payment,"
        " MAX(date_paid) AS last_payment "
        "FROM v_experimental_payments_full_psa"
    )


@st.cache_data(ttl=300)
def fetch_experimental_payments_by_kind() -> pd.DataFrame:
    return _safe(
        "SELECT payment_kind, COUNT(*) AS rows,"
        " SUM(amount) AS total_paid,"
        " MIN(date_paid) AS first_seen,"
        " MAX(date_paid) AS last_seen "
        "FROM v_experimental_payments_full_psa "
        "GROUP BY payment_kind ORDER BY total_paid DESC"
    )


@st.cache_data(ttl=300)
def fetch_experimental_payments_by_year() -> pd.DataFrame:
    return _safe(
        "SELECT payment_year, COUNT(*) AS rows,"
        " SUM(amount) AS total_paid,"
        " SUM(CASE WHEN payment_kind = 'TAA' THEN amount END) AS taa_total,"
        " SUM(CASE WHEN payment_kind = 'PSA_DUBLIN' THEN amount END) AS dublin_total,"
        " SUM(CASE WHEN payment_kind IN ('PRA','PRA_MIN') THEN amount END) AS pra_total "
        "FROM v_experimental_payments_full_psa "
        "GROUP BY payment_year ORDER BY payment_year"
    )


@st.cache_data(ttl=300)
def fetch_experimental_payments_top_members(limit: int = 25) -> pd.DataFrame:
    return _safe(
        "SELECT member_name, position,"
        " COUNT(*) AS payment_count,"
        " SUM(amount) AS total_paid,"
        " SUM(CASE WHEN payment_kind = 'TAA' THEN amount END) AS taa_total,"
        " SUM(CASE WHEN payment_kind IN ('PRA','PRA_MIN') THEN amount END) AS pra_total "
        "FROM v_experimental_payments_full_psa "
        "GROUP BY member_name, position "
        "ORDER BY total_paid DESC "
        f"LIMIT {int(limit)}"
    )


# ── Production-vs-experimental comparison ───────────────────────────────────

@st.cache_data(ttl=300)
def fetch_production_legislation_count() -> int:
    """Count rows in the production v_legislation_index — for delta display."""
    try:
        from data_access.legislation_data import get_legislation_conn
        df = get_legislation_conn().execute(
            "SELECT COUNT(*) AS n FROM v_legislation_index"
        ).df()
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0


@st.cache_data(ttl=300)
def fetch_production_payments_summary() -> pd.DataFrame:
    """Production payments summary for delta display."""
    try:
        from data_access.payments_data import get_payments_conn
        return get_payments_conn().execute(
            "SELECT COUNT(*) AS rows,"
            " COUNT(DISTINCT member_name) AS members,"
            " SUM(amount_num) AS total_paid,"
            " MIN(date_paid) AS first_payment,"
            " MAX(date_paid) AS last_payment "
            "FROM v_payments_base"
        ).df()
    except Exception:
        return pd.DataFrame()
