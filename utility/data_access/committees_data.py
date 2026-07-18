"""Committees data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.committees``; this file owns the Streamlit caching
and the one-shot party_seats JSON decode + DECIMAL->int casts on the small
summary frame (a presentation decode, not a rollup — unchanged from before).

Forbidden here (unchanged): read_parquet, duckdb.connect(":memory:"),
pandas groupby/merge/pivot/iterrows, CREATE VIEW, business-metric definitions.
"""

from __future__ import annotations

import json

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.connections import domain_conn
from dail_tracker_core.queries import committees as _q


@st.cache_resource
def get_committees_conn() -> duckdb.DuckDBPyConnection:
    return domain_conn("committees")


@st.cache_resource
def get_committee_evidence_conn() -> duckdb.DuckDBPyConnection:
    """Separate connection for the meeting-history view (reads gold).

    swallow_errors=True (unlike the membership conn) so a box without the
    committee-evidence gold parquets degrades to an empty meeting history rather
    than breaking the whole Committees page — the membership register is the
    page's core and must always render.
    """
    return get_committees_conn()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_committee_assignments(chamber: str) -> pd.DataFrame:
    """One row per (member × committee) for the chamber."""
    return _q.assignments(get_committees_conn(), chamber).data


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_office_holders(chamber: str) -> pd.DataFrame:
    """One row per (member × office)."""
    return _q.office_holders(get_committees_conn(), chamber).data


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_committee_summary(chamber: str) -> pd.DataFrame:
    """Per-committee rollup; decodes party_seats_json into a list of (party, seats)
    tuples and casts the DECIMAL counts to int (one-shot shaping on a ≤100-row
    frame — not a rollup). Shape matches the old _committee_summary()."""
    df = _q.member_detail(get_committees_conn(), chamber).data
    if df.empty:
        return df.assign(party_seats=[])

    df["chairs"] = df["chairs"].astype(int)
    df["members"] = df["members"].astype(int)
    df["parties"] = df["parties"].astype(int)
    df["party_seats"] = df["party_seats_json"].map(  # logic_firewall: display_only
        lambda s: [(d["party"], int(d["seats"])) for d in json.loads(s)] if s else []
    )
    return df.drop(columns=["party_seats_json"])


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_party_seats(chamber: str, committee: str | None = None) -> pd.DataFrame:
    """Long-format party seats per committee; optionally filtered to one committee."""
    return _q.party_seats(get_committees_conn(), chamber, committee).data


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_committee_meetings(committee: str, limit: int = 60) -> pd.DataFrame:
    """Reverse-chron meeting history for one committee (date · topics · witnesses ·
    transcript link). Empty frame when the committee has no extracted meetings yet
    (only a subset of committees is in scope) or the gold layer is absent — the
    page renders a "not yet available" state, never an error."""
    return _q.meetings(get_committee_evidence_conn(), committee, limit).data
