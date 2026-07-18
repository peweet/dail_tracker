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

from dail_tracker_core.connections import domain_conn
from dail_tracker_core.queries import corporate as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_corporate_conn() -> duckdb.DuckDBPyConnection:
    return domain_conn("corporate")


@st.cache_data(ttl=300)
def fetch_corporate_notices() -> pd.DataFrame:
    """Every in-scope corporate notice as a row — the full v_corporate_notices
    view. Personal insolvency is excluded upstream by enrichment. The page
    does its faceting / search / aggregation in pandas off this frame."""
    return _q.corporate_notices(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_firm_notices(firm: str) -> pd.DataFrame:
    """Every notice naming one receiver / insolvency firm (the ?firm= landing).
    Curated firms match the precomputed receiver_firms tag; a free-text firm
    falls back to a word-bounded regexp over raw_text — both matches run in
    DuckDB (graduated out of the page's _firm_notice_mask)."""
    return _q.firm_notices(get_corporate_conn(), firm).data


@st.cache_data(ttl=300)
def fetch_firm_fund_counts(firm: str) -> pd.DataFrame:
    """Appointing parent funds/banks co-named on one curated firm's notices —
    n_recv (receivership-shaped) + n_all (every notice). Precomputed in
    v_corporate_firm_fund_counts; empty for non-curated (free-text) firms."""
    return _q.firm_fund_counts(get_corporate_conn(), firm).data


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


@st.cache_data(ttl=300)
def fetch_cbi_enforcement() -> pd.DataFrame:
    """CBI enforcement actions — settlements / sanctions with fines (gold). Not summed."""
    return _q.cbi_enforcement(get_corporate_conn()).data


@st.cache_data(ttl=300)
def fetch_isif_portfolio(limit: int | None = 12) -> pd.DataFrame:
    """ISIF sovereign-fund investment commitments (gold) — newest first, never summed."""
    return _q.isif_portfolio(get_corporate_conn(), limit=limit).data


@st.cache_data(ttl=600)
def fetch_brand_aliases() -> pd.DataFrame:
    """Brand → parent_fund → fund_type curated alias map. Falls back to a
    typed-empty frame if the view/source is absent, so the page's
    `if "notes" in aliases.columns` guard still holds."""
    df = _q.brand_aliases(get_corporate_conn()).data
    if df.empty:
        return pd.DataFrame(columns=["brand", "parent_fund", "fund_type", "notes"])
    return df


@st.cache_data(ttl=600)
def fetch_brand_alias_groups() -> pd.DataFrame:
    """Methodology-expander table: the curated alias map rolled up to one row per
    (parent_fund, fund_type) with its brand strings joined — precomputed in
    v_corporate_brand_alias_groups. Typed-empty fallback keeps the page's
    column access safe when the view/source is absent."""
    df = _q.brand_alias_groups(get_corporate_conn()).data
    if df.empty:
        return pd.DataFrame(columns=["parent_fund", "fund_type", "brands", "notes_concat"])
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
