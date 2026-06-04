"""
SIPO political-donations data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/sipo_*.sql
- Retrieval SQL for the Party-Donations lens of the Payments page
  (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as the page files):
- JOIN, GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL (rollups live in the views)
- CREATE VIEW / CREATE TABLE, read_parquet / read_csv, pandas groupby/merge/pivot,
  business-metric definitions.

Privacy: the gold parquet and these views carry NO donor address column. Donor
name + amount are the public SIPO record. No-inference: figures + source only.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views


@st.cache_resource
def get_donations_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["sipo_*.sql"], swallow_errors=False)
    return conn


@st.cache_data(ttl=300)
def fetch_donations_totals() -> dict[str, float | int]:
    """Headline totals across all parties (no GROUP BY — aggregates the rollup view)."""
    row = (
        get_donations_conn()
        .execute(
            "SELECT SUM(total_value), SUM(donation_count), COUNT(*)"
            " FROM v_sipo_donations_by_party"
        )
        .fetchone()
    )
    if not row:
        return {"total": 0.0, "donations": 0, "parties": 0}
    return {"total": float(row[0] or 0.0), "donations": int(row[1] or 0), "parties": int(row[2] or 0)}


@st.cache_data(ttl=300)
def fetch_donations_by_party() -> pd.DataFrame:
    """One row per party — drives the Party-Donations cards."""
    return (
        get_donations_conn()
        .execute(
            "SELECT party, donation_count, total_value, min_value, max_value, verify_count"
            " FROM v_sipo_donations_by_party"
            " ORDER BY total_value DESC"
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_party_donors(party: str) -> pd.DataFrame:
    """Donor receipts for one party — name, amount, date, method, verify flag."""
    return (
        get_donations_conn()
        .execute(
            "SELECT donor_name, value_eur, date_received_raw, nature,"
            " description_of_donor, needs_verify, source_page"
            " FROM v_sipo_donations"
            " WHERE party = ?"
            " ORDER BY value_eur DESC",
            [party],
        )
        .df()
    )
