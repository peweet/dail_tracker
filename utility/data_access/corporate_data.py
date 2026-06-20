"""Corporate notices data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.corporate``; this file owns only Streamlit caching
and unwraps ``.data`` to the DataFrame the page expects (empty on a source
failure — same contract as the old ``_safe``).

Forbidden here (unchanged): JOIN/multi-col GROUP BY/HAVING/WINDOW in SQL,
CREATE VIEW, read_parquet, pandas merge/pivot, business-metric definitions.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import corporate as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_corporate_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["corporate_*.sql"], swallow_errors=True)


@st.cache_data(ttl=300)
def fetch_corporate_notices() -> pd.DataFrame:
    """Every in-scope corporate notice as a row — the full v_corporate_notices
    view. Personal insolvency is excluded upstream by enrichment. The page
    does its faceting / search / aggregation in pandas off this frame."""
    return _q.corporate_notices(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_corporate_notices_for_company_result(company_num: int) -> QueryResult:
    """One CRO company's corporate register / distress notices (Iris Oifigiúil) for the
    company dossier's corporate-register panel, matched on CRO ``company_num``. A
    QueryResult (not the bare frame) so the panel can tell 'source unavailable' from
    'no notices' — display-only public-record annotation, never a civic claim beyond
    'this CRO entity appears on these statutory notices'."""
    return _q.corporate_notices_for_company(get_corporate_conn(), company_num)


@st.cache_data(ttl=300)
def fetch_cbi_notice_matches() -> pd.DataFrame:
    """Per-notice CBI authorisation lookup (EXPERIMENTAL — CBI register, gold)."""
    return _q.cbi_notice_matches(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_cbi_repeat_distress() -> pd.DataFrame:
    """Per-firm repeat-distress aggregate (EXPERIMENTAL — CBI register, gold)."""
    return _q.cbi_repeat_distress(get_corporate_conn()).data


@st.cache_data(ttl=600)
def fetch_brand_aliases() -> pd.DataFrame:
    """Brand → parent_fund → fund_type curated alias map. Falls back to a
    typed-empty frame if the view/source is absent, so the page's
    `if "notes" in aliases.columns` guard still holds."""
    df = _q.brand_aliases(get_corporate_conn()).data
    if df.empty:
        return pd.DataFrame(columns=["brand", "parent_fund", "fund_type", "notes"])
    return df


@st.cache_data(ttl=300)
def fetch_receiver_appointers() -> pd.DataFrame:
    """Receiver-appointer ranking (precomputed gold — corporate_receiver_enrich)."""
    return _q.receiver_appointers(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_receiver_bucket_mix() -> pd.DataFrame:
    """Appointer type-mix (mention-weighted) by bucket."""
    return _q.receiver_bucket_mix(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_receiver_firms() -> pd.DataFrame:
    """Operator-firm concentration (precomputed gold)."""
    return _q.receiver_firms(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_receiver_year_counts() -> pd.DataFrame:
    """Receivership-notices-by-year sparkline series (precomputed gold)."""
    return _q.receiver_year_counts(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_receiver_summary() -> pd.DataFrame:
    """Featured/operator headline scalar counts (one-row frame)."""
    return _q.receiver_summary(get_corporate_conn()).data
