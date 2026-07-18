"""Legislation data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.legislation``; this file owns only the Streamlit
caching and the small list/dict shaping + the two ``v_bill_amendment_intensity``
column projections the page contract expects (the core fns return a richer
superset shared with the orphan most-contested helpers).

Backs both the legislation page and the statutory-instruments page.

Forbidden here (unchanged): read_parquet, parquet_scan, CREATE VIEW,
pandas groupby/merge/pivot business logic, multi-dim GROUP BY.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.connections import domain_conn
from dail_tracker_core.queries import legislation as _q

# Column contracts the page consumed from the old narrower SELECTs. The core
# fns return the full v_bill_amendment_intensity column set; we project to these
# so the page frames are byte-identical to before.
_MOST_CONTESTED_COLS = ["bill_id", "bill_title", "bill_status", "amendment_lists", "committee_lists", "report_lists"]
_INTENSITY_COLS = [
    "bill_id",
    "amendment_lists",
    "distinct_stages",
    "committee_lists",
    "report_lists",
    "cream_lists",
    "dail_lists",
    "seanad_lists",
    "first_amendment_date",
    "last_amendment_date",
]


@st.cache_resource
def get_legislation_conn() -> duckdb.DuckDBPyConnection:
    return domain_conn("legislation")


# ── Index ──────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_legislation_index_filtered(
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    title_search: str | None = None,
) -> pd.DataFrame:
    return _q.index_filtered(get_legislation_conn(), start_date, end_date, status, title_search).data


@st.cache_data(ttl=300)
def fetch_all_statuses() -> list[str]:
    df = _q.distinct_statuses(get_legislation_conn()).data
    return df["bill_status"].tolist() if not df.empty else []


# ── Detail ─────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_detail(bill_id: str) -> pd.DataFrame:
    return _q.bill_detail(get_legislation_conn(), bill_id).data


# ── Amendment intensity (contestation proxy) ────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_amendment_intensity(bill_id: str) -> pd.DataFrame:
    df = _q.amendment_intensity_for_bill(get_legislation_conn(), bill_id).data
    return df[_INTENSITY_COLS] if not df.empty else df


@st.cache_data(ttl=300)
def fetch_most_contested_bills(limit: int = 15) -> pd.DataFrame:
    df = _q.most_contested_bills(get_legislation_conn(), limit).data
    return df[_MOST_CONTESTED_COLS] if not df.empty else df


# ── Timeline ───────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_timeline(bill_id: str) -> pd.DataFrame:
    return _q.bill_timeline(get_legislation_conn(), bill_id).data


# ── Sources ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_sources(bill_id: str) -> pd.DataFrame:
    return _q.bill_sources(get_legislation_conn(), bill_id).data


# ── PDF documents ──────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_pdfs(bill_id: str) -> pd.DataFrame:
    """All Oireachtas-issued PDFs for a bill (versions → related → amendments)."""
    return _q.bill_pdfs(get_legislation_conn(), bill_id).data


# ── Debates ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_bill_debates(bill_id: str) -> pd.DataFrame:
    return _q.bill_debates(get_legislation_conn(), bill_id).data


# ── Pre-2014 primary Acts (curated table) ─────────────────────────────────────


@st.cache_data(ttl=3600)
def fetch_pre2014_act_detail(bill_id: str) -> dict:
    """Hero info for a synthetic 'act_<year>_<slug>' bill_id. {} on miss/non-act."""
    if not (isinstance(bill_id, str) and bill_id.startswith("act_")):
        return {}
    rows = _q.pre2014_act(get_legislation_conn(), bill_id).data
    if rows.empty:
        return {}
    r = rows.iloc[0]
    return {
        "act_short_title": str(r.get("act_short_title") or ""),
        "act_year": int(r.get("act_year") or 0),
        "policy_domain": str(r.get("policy_domain") or ""),
    }


# ── Statutory Instruments under a bill ────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_si_composition(bill_id: str) -> pd.DataFrame:
    """Operation-mix summary for the composition sentence (GROUP BY lives in view)."""
    return _q.si_composition(get_legislation_conn(), bill_id).data


@st.cache_data(ttl=300)
def fetch_si_freshness(bill_id: str) -> dict:
    """Total + first/last SI date + EU share for the freshness line."""
    df = _q.si_freshness(get_legislation_conn(), bill_id).data
    if df.empty or int(df.iloc[0]["total"] or 0) == 0:
        return {}
    r = df.iloc[0]
    return {
        "first_si": r["first_si"],
        "last_si": r["last_si"],
        "total": int(r["total"] or 0),
        "eu_count": int(r["eu_count"] or 0),
    }


@st.cache_data(ttl=300)
def fetch_si_years_for_bill(bill_id: str) -> list[int]:
    df = _q.si_years_for_bill(get_legislation_conn(), bill_id).data
    return [int(y) for y in df["si_year"].dropna().tolist()] if not df.empty else []


@st.cache_data(ttl=300)
def fetch_si_by_bill(
    bill_id: str,
    year: int | None = None,
    operation: str | None = None,
    eu_only: bool = False,
) -> pd.DataFrame:
    return _q.si_by_bill(get_legislation_conn(), bill_id, year, operation, eu_only).data


@st.cache_data(ttl=300)
def fetch_act_commencement(bill_id: str) -> pd.DataFrame:
    """Commencement-order timeline for an Act (empty when none / self-executing)."""
    return _q.act_commencement(get_legislation_conn(), bill_id).data


# ── Statutory Instruments — first-class entity (v_statutory_instruments) ──────


@st.cache_data(ttl=300)
def fetch_si_entity_index() -> pd.DataFrame:
    """Every Statutory Instrument as a row — the page facets/filters in pandas."""
    return _q.si_entity_index(get_legislation_conn()).data


def fetch_si_entity_index_classified() -> pd.DataFrame:
    """v_statutory_instruments + LRC subject classification (falls back to empty
    when the LRC gold table is absent). Uncached, matching prior behaviour."""
    return _q.si_entity_index_classified(get_legislation_conn()).data


@st.cache_data(ttl=300)
def fetch_si_amendments_made(si_year: int, si_number: int) -> pd.DataFrame:
    """The instruments THIS SI amends/revokes (forward direction of the SI→SI graph)."""
    return _q.si_amendments_made(get_legislation_conn(), si_year, si_number).data


@st.cache_data(ttl=300)
def fetch_circulars_for_si(si_year: int, si_number: int) -> pd.DataFrame:
    """Government circular(s) that operationalise THIS SI — the instruction layer atop the
    law. Resolved rows only (the citing circular refers to this SI). Empty for most SIs:
    only ~70 circulars in the 2020+ corpus cite an SI."""
    qr = _q.circular_si_crosswalk(get_legislation_conn(), si_year=si_year, si_number=si_number,
                                  resolved_only=True)
    return qr.data if qr.ok else pd.DataFrame()
