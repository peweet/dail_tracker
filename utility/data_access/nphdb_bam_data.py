"""BAM / National Children's Hospital disclosure data access — thin Streamlit wrapper.

Retrieval SQL lives in ``dail_tracker_core.queries.nphdb_bam``; this file owns only
the Streamlit connection cache and per-query memoisation. The dataset is a static,
hand-verified Dáil written-answer (PQ) disclosure — BAM's own reporting to the
Oireachtas on National Children's Hospital schedule slippage, and a 2017/2018
project-cost estimate table — not a scraped corpus.

Forbidden here (same contract as the other thin wrappers): JOIN/GROUP BY/HAVING/
WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions — all of which live in sql_views/ and dail_tracker_core.
"""

from __future__ import annotations

import duckdb
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import nphdb_bam as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_nphdb_bam_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["nphdb_bam_disclosures.sql"])


@st.cache_data(ttl=3600)
def fetch_nphdb_bam_disclosures_result() -> QueryResult:
    """The full BAM disclosure ledger (schedule-slippage + cost rows). Retrieval
    only — the view + query module own the ordering; the page renders from this."""
    return _q.disclosures(get_nphdb_bam_conn())
