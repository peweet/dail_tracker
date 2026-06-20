"""Per-candidate SIPO election-EXPENSES data access — thin Streamlit wrapper.

Companion to sipo_expenses_data.py (party-tier) and sipo_donations_data.py. This is
the GRANULAR tier: each individual candidate's GE2024 Election Expenses Statement,
down to the Part-5 line items (e.g. Noel Grealish -> "Galway Advertiser" €2,799.48).
Retrieval SQL lives in ``dail_tracker_core.queries.sipo``; this file owns only the
Streamlit caching + light dict shaping.

Data caveats the page MUST surface (no-inference):
  * OCR-derived from the official scanned returns — rows carry a "verify vs SIPO PDF"
    mark (``needs_verify``); decimal-loss mis-reads are excluded from gold, not guessed.
  * OCR is INCREMENTAL — only the candidates extracted so far are loaded.
  * 'detail' is the form's free-text "Details of item" field — a MIX of supplier names
    and item descriptions, NOT a clean vendor list.
  * party is authoritative (registry) for elected TDs, else the OCR-declared party;
    unmatched is shown as unknown, never guessed.

Forbidden here (logic firewall, unchanged): read_parquet, parquet_scan, CREATE VIEW,
pandas groupby/merge/pivot business logic, multi-dim GROUP BY. All rollups are views.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import sipo as _q


@st.cache_resource
def _conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["sipo_*.sql"], swallow_errors=False)


@st.cache_data(ttl=300)
def fetch_totals() -> dict[str, float | int]:
    """Headline totals across all candidates currently loaded."""
    r = _q.candidate_totals(_conn())
    if not r.ok or r.is_empty:
        return {"candidates": 0, "total": 0.0, "median": 0.0, "elected": 0, "constituencies": 0, "verify": 0}
    row = r.data.iloc[0]

    def _num(col: str) -> float:
        return float(row[col]) if pd.notna(row[col]) else 0.0

    return {
        "candidates": int(_num("candidate_count")),
        "total": _num("total_spend"),
        "median": _num("median_spend"),
        "elected": int(_num("elected_count")),
        "constituencies": int(_num("constituencies")),
        "verify": int(_num("verify_count")),
    }


@st.cache_data(ttl=300)
def fetch_ranked(limit: int | None = None) -> pd.DataFrame:
    """Candidates ranked by total spend — the primary league table.

    Defaults to ALL loaded candidates (no limit) so the page's search box can
    surface any of them; the page itself caps how many cards it renders.
    """
    return _q.candidate_ranked(_conn(), limit).data


@st.cache_data(ttl=300)
def fetch_filed_unquantified() -> pd.DataFrame:
    """Candidates who filed a statement with no trustworthy total — searchable, NO amount.

    These are listed alongside the ranked spenders so every filed candidate is findable;
    the page shows them without a figure and links each to the official SIPO PDF.
    """
    return _q.candidate_filed_unquantified(_conn()).data


@st.cache_data(ttl=300)
def fetch_by_party() -> pd.DataFrame:
    """One row per canonical party."""
    return _q.candidate_by_party(_conn()).data


@st.cache_data(ttl=300)
def fetch_by_category() -> pd.DataFrame:
    """The 8 statutory categories (5A–5H) with totals."""
    return _q.candidate_by_category(_conn()).data


@st.cache_data(ttl=300)
def fetch_top_details(limit: int = 25) -> pd.DataFrame:
    """Top spend-detail lines (suppliers + descriptions — not a vendor list)."""
    return _q.candidate_top_details(_conn(), limit).data


@st.cache_data(ttl=300)
def fetch_line_items(candidate_name: str) -> pd.DataFrame:
    """One candidate's Part-5 line items (the drill-down)."""
    return _q.candidate_line_items(_conn(), candidate_name).data


@st.cache_data(ttl=300)
def fetch_candidate(candidate_name: str) -> pd.Series | None:
    """One candidate's headline row, or None if absent."""
    r = _q.candidate_one(_conn(), candidate_name)
    if not r.ok or r.is_empty:
        return None
    return r.data.iloc[0]


@st.cache_data(ttl=300)
def fetch_party_finance() -> pd.DataFrame:
    """One row per party: donations in / national-agent spend / candidate spend.

    Drives the Election-2024 overview "full picture" cards. The three money
    columns are DIFFERENT grains (see v_sipo_ge2024_party_finance) — the UI shows
    each on its own bar and NEVER sums them.
    """
    return _q.ge2024_party_finance(_conn()).data
