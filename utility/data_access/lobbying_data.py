"""Lobbying data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.lobbying``; this file owns only the Streamlit caching
and the two list-shaping helpers the page expects (``fetch_all_*_names`` return
``list[str]``). Every other fetcher returns ``.data`` — the same DataFrame
contract the page consumed before, so the old ``_safe`` swallow-to-empty
behaviour is preserved (an unavailable QueryResult carries an empty DataFrame).

``get_lobbying_conn`` is still exported (the page imports fetchers, not the conn,
but keeping it avoids surprising any future caller).

Forbidden here (unchanged): read_parquet, parquet_scan, CREATE VIEW,
pandas groupby/merge/pivot business logic, multi-dim GROUP BY.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.connections import domain_conn
from dail_tracker_core.queries import lobbying as _q


@st.cache_resource
def get_lobbying_conn() -> duckdb.DuckDBPyConnection:
    # lobbying_*.sql registers loud (a missing core view is a real break);
    # charity_financials_by_year.sql registers soft so the org panel's "Charity
    # finances" tile degrades gracefully rather than taking the page down.
    return domain_conn("lobbying")


# ── Summary ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_summary() -> pd.DataFrame:
    return _q.summary(get_lobbying_conn()).data


# ── Return documents (embedded third-party PDFs) ───────────────────────────────


@st.cache_data(ttl=300)
def fetch_return_documents_for_org(org_name: str) -> pd.DataFrame:
    return _q.return_documents_for_org(get_lobbying_conn(), org_name).data


# ── Politician index ────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_politician_index(year: int | None = None) -> pd.DataFrame:
    return _q.politician_index(get_lobbying_conn(), year).data


@st.cache_data(ttl=300)
def fetch_all_politician_names() -> list[str]:
    r = _q.all_politician_names(get_lobbying_conn())
    if not r.ok or r.is_empty:
        return []
    return r.data["member_name"].dropna().tolist()


# ── Org index ──────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_org_index(
    exclude_state_adjacent: bool = False,
    funding_profile: str | None = None,
    income_trend: str | None = None,
    name_q: str | None = None,
) -> pd.DataFrame:
    return _q.org_index(
        get_lobbying_conn(),
        exclude_state_adjacent,
        funding_profile=funding_profile,
        income_trend=income_trend,
        name_q=name_q,
    ).data


@st.cache_data(ttl=300)
def fetch_all_org_names() -> list[str]:
    r = _q.all_org_names(get_lobbying_conn())
    if not r.ok or r.is_empty:
        return []
    return r.data["lobbyist_name"].dropna().tolist()


@st.cache_data(ttl=300)
def fetch_charity_financial_series(rcn: int) -> pd.DataFrame:
    return _q.charity_financial_series(get_lobbying_conn(), rcn).data


# ── Procurement footprint (eTenders cross-reference) ────────────────────────────


@st.cache_data(ttl=300)
def fetch_org_procurement(org_name: str) -> pd.DataFrame:
    return _q.org_procurement(get_lobbying_conn(), org_name).data


# ── Contact detail ─────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_contact_detail(member_name: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    return _q.contact_detail(get_lobbying_conn(), member_name, start, end).data


@st.cache_data(ttl=300)
def fetch_org_contact_detail(org_name: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    return _q.org_contact_detail(get_lobbying_conn(), org_name, start, end).data


@st.cache_data(ttl=300)
def fetch_politician_area_returns(
    member_name: str, area: str, start: str | None = None, end: str | None = None
) -> pd.DataFrame:
    return _q.politician_area_returns(get_lobbying_conn(), member_name, area, start, end).data


@st.cache_data(ttl=300)
def fetch_dpo_return_map() -> pd.DataFrame:
    """Map of return_id -> DPO individual_name (one row per match)."""
    return _q.dpo_return_map(get_lobbying_conn()).data


@st.cache_data(ttl=300)
def fetch_org_politician_returns(
    org_name: str, member_name: str, start: str | None = None, end: str | None = None
) -> pd.DataFrame:
    return _q.org_politician_returns(get_lobbying_conn(), org_name, member_name, start, end).data


@st.cache_data(ttl=300)
def fetch_dpo_politician_returns(
    individual_name: str, member_name: str, start: str | None = None, end: str | None = None
) -> pd.DataFrame:
    return _q.dpo_politician_returns(get_lobbying_conn(), individual_name, member_name, start, end).data


@st.cache_data(ttl=300)
def fetch_politician_area_returns_with_dpo(
    member_name: str, area: str, start: str | None = None, end: str | None = None
) -> pd.DataFrame:
    return _q.politician_area_returns_with_dpo(get_lobbying_conn(), member_name, area, start, end).data


@st.cache_data(ttl=300)
def fetch_area_contact_detail(area: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    return _q.area_contact_detail(get_lobbying_conn(), area, start, end).data


# ── Policy area summary ────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_policy_area_summary() -> pd.DataFrame:
    return _q.policy_area_summary(get_lobbying_conn()).data


# ── Topic keyword search ──────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_topic_returns(keywords: tuple[str, ...], start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Returns whose free-text description matches any keyword (case-insensitive
    substrings, no tokenisation). ``keywords`` is a tuple so the result caches."""
    return _q.topic_returns(get_lobbying_conn(), keywords, start, end).data


@st.cache_data(ttl=300)
def fetch_topic_summary(keywords: tuple[str, ...]) -> pd.DataFrame:
    """One-row aggregate for the Topic stage hero (counts, period span)."""
    return _q.topic_summary(get_lobbying_conn(), keywords).data


# ── Recent returns ─────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_recent_returns() -> pd.DataFrame:
    return _q.recent_returns(get_lobbying_conn()).data


# ── Revolving door ─────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_revolving_door(limit: int | None = 50) -> pd.DataFrame:
    """Ranked DPO list for the landing callout (limit=5..7) and Stage 2a index (limit=None)."""
    return _q.revolving_door(get_lobbying_conn(), limit).data


@st.cache_data(ttl=300)
def fetch_revolving_door_summary() -> pd.DataFrame:
    """Single-row aggregate for the landing callout headline number."""
    return _q.revolving_door_summary(get_lobbying_conn()).data


@st.cache_data(ttl=300)
def fetch_dpo_one(individual_name: str) -> pd.DataFrame:
    """Single DPO row for the Stage 2b individual hero."""
    return _q.dpo_one(get_lobbying_conn(), individual_name).data


@st.cache_data(ttl=300)
def fetch_dpo_firms(individual_name: str) -> pd.DataFrame:
    """Firms a given DPO has filed returns under (Stage 2b: Firms represented)."""
    return _q.dpo_firms(get_lobbying_conn(), individual_name).data


@st.cache_data(ttl=300)
def fetch_dpo_client_breakdown(individual_name: str) -> pd.DataFrame:
    """Client companies a given DPO has lobbied on behalf of (Stage 2b: Clients represented)."""
    return _q.dpo_client_breakdown(get_lobbying_conn(), individual_name).data


@st.cache_data(ttl=300)
def fetch_dpo_politicians_targeted(individual_name: str) -> pd.DataFrame:
    """Politicians targeted by returns this DPO filed (Stage 2b: Politicians targeted)."""
    return _q.dpo_politicians_targeted(get_lobbying_conn(), individual_name).data


# ── DPO individual profile ────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_dpo_returns(individual_name: str) -> pd.DataFrame:
    """Returns filed where the DPO appears directly as the lobbyist name."""
    return _q.dpo_returns(get_lobbying_conn(), individual_name).data


@st.cache_data(ttl=300)
def fetch_dpo_clients(individual_name: str) -> pd.DataFrame:
    """Client companies the DPO has lobbied on behalf of."""
    return _q.dpo_clients(get_lobbying_conn(), individual_name).data


@st.cache_data(ttl=300)
def fetch_dpo_returns_detail(individual_name: str) -> pd.DataFrame:
    """All returns attributed to this DPO from the revolving door register."""
    return _q.dpo_returns_detail(get_lobbying_conn(), individual_name).data


# ── Source links ───────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_sources_for_politician(member_name: str) -> pd.DataFrame:
    return _q.sources_for_politician(get_lobbying_conn(), member_name).data


@st.cache_data(ttl=300)
def fetch_sources_for_org(org_name: str) -> pd.DataFrame:
    return _q.sources_for_org(get_lobbying_conn(), org_name).data


# ── Org intensity (bilateral relationships) ────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_orgs_for_politician(member_name: str) -> pd.DataFrame:
    """Orgs that have lobbied a politician, ranked by intensity."""
    return _q.orgs_for_politician(get_lobbying_conn(), member_name).data


@st.cache_data(ttl=300)
def fetch_politicians_for_org(org_name: str) -> pd.DataFrame:
    """Politicians targeted by an org, ranked by intensity."""
    return _q.politicians_for_org(get_lobbying_conn(), org_name).data


# ── Persistence ────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_org_persistence(org_name: str) -> pd.DataFrame:
    return _q.org_persistence(get_lobbying_conn(), org_name).data


# ── Policy exposure ────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_policy_exposure_for_politician(member_name: str) -> pd.DataFrame:
    """Policy areas a politician has been lobbied on, ranked by volume."""
    return _q.policy_exposure_for_politician(get_lobbying_conn(), member_name).data


@st.cache_data(ttl=300)
def fetch_politicians_for_area(area: str) -> pd.DataFrame:
    """Politicians most exposed to a given policy area."""
    return _q.politicians_for_area(get_lobbying_conn(), area).data


# ── Clients ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_clients_for_org(org_name: str) -> pd.DataFrame:
    """Client companies represented by a lobbying firm."""
    return _q.clients_for_org(get_lobbying_conn(), org_name).data
