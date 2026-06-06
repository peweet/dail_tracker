"""SIPO political-donations data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.sipo``; this file owns only the Streamlit caching and
the headline-totals dict shaping the Payments page expects.

Privacy: the gold parquet and these views carry NO donor address column. Donor
name + amount are the public SIPO record. No-inference: figures + source only.

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
def get_donations_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["sipo_*.sql"], swallow_errors=False)


@st.cache_data(ttl=300)
def fetch_donations_totals() -> dict[str, float | int]:
    """Headline totals across all parties (no GROUP BY — aggregates the rollup view)."""
    r = _q.donations_totals(get_donations_conn())
    if not r.ok or r.is_empty:
        return {"total": 0.0, "donations": 0, "parties": 0}
    row = r.data.iloc[0]
    return {
        "total": float(row["total_value"] or 0.0),
        "donations": int(row["donation_count"] or 0),
        "parties": int(row["parties"] or 0),
    }


@st.cache_data(ttl=300)
def fetch_donations_by_party() -> pd.DataFrame:
    """One row per party — drives the Party-Donations cards."""
    return _q.donations_by_party(get_donations_conn()).data


@st.cache_data(ttl=300)
def fetch_party_donors(party: str) -> pd.DataFrame:
    """Donor receipts for one party — name, amount, date, method, verify flag."""
    return _q.party_donors(get_donations_conn(), party).data
