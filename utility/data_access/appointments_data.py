"""Public Appointments data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL lives in ``dail_tracker_core.queries.appointments``; this file owns
only Streamlit caching and unwraps ``.data`` (empty on a source failure — same
contract as the old ``_safe``).

Forbidden here (unchanged): JOIN/multi-col GROUP BY/HAVING/WINDOW in SQL,
CREATE VIEW, read_parquet, pandas merge/pivot, business-metric definitions.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import appointments as _q


@st.cache_resource
def get_appointments_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["appointments_*.sql"], swallow_errors=True)


@st.cache_data(ttl=300)
def fetch_public_appointments() -> pd.DataFrame:
    """Every public-appointment notice as a row — the full v_public_appointments
    view. One registered analytical surface; the page does its filtering,
    faceting, and grouping in pandas off this frame."""
    return _q.public_appointments(get_appointments_conn()).data
