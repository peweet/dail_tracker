"""
Committees data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/committees_*.sql
  (v_committee_assignments, v_committee_office_holders,
   v_committee_member_detail, v_committee_party_seats)
- All retrieval SQL for the committees page (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as Streamlit page files):
- read_parquet / read_csv
- duckdb.connect(":memory:") + register frame pattern
- pandas groupby, merge, pivot, iterrows
- CREATE VIEW / CREATE TABLE
- Business metric definitions (the per-committee rollup lives in
  v_committee_member_detail, not here)

The wide-to-long unpivot of committee_*/office_* slot columns is now a
pipeline script (committees_long_format_etl.py)
whose output the four views read.

The page consumes:
- df_long  → fetch_committee_assignments(chamber)
- offices  → fetch_office_holders(chamber)
- summary  → fetch_committee_summary(chamber)
The shapes match what utility/pages_code/committees.py used to build in-page.
"""

from __future__ import annotations

import json

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views


@st.cache_resource
def get_committees_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["committees_*.sql"], swallow_errors=False)
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        conn = get_committees_conn()
        return conn.execute(sql, params or []).df()
    except Exception:
        return pd.DataFrame()


# ── Assignments (long-format member × committee) ──────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_committee_assignments(chamber: str) -> pd.DataFrame:
    """One row per (member × committee) for the chamber.

    Column shape matches the old _load() df_long return:
        name, party, constituency, dail_number, committee, committee_url,
        type, status, role, is_chair, start, end
    """
    return _safe(
        "SELECT name, party, constituency, dail_number, committee, committee_url,"
        ' type, status, role, is_chair, start, "end"'
        " FROM v_committee_assignments WHERE chamber = ?",
        [chamber],
    )


# ── Government office holders ─────────────────────────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_office_holders(chamber: str) -> pd.DataFrame:
    """One row per (member × office) — column shape matches the old
    _load() offices return: name, party, office, start, end."""
    return _safe(
        'SELECT name, party, office, start, "end" FROM v_committee_office_holders WHERE chamber = ?',
        [chamber],
    )


# ── Per-committee summary (members + parties + chair + party_seats) ──────────


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_committee_summary(chamber: str) -> pd.DataFrame:
    """Per-committee rollup matching the old _committee_summary() shape:
        committee, members, parties, chairs, status, type, url,
        chair_name, chair_party, party_seats
    where party_seats is a Python list of (party, seats) tuples.

    The view returns party_seats as JSON; we decode here so the page
    doesn't need to know the encoding. This is a one-shot decode on a
    small frame (≤100 rows) — not a rollup.
    """
    df = _safe(
        "SELECT committee, members, parties, chairs, status, type, url,"
        " chair_name, chair_party, party_seats_json"
        " FROM v_committee_member_detail WHERE chamber = ?",
        [chamber],
    )
    if df.empty:
        return df.assign(party_seats=[])

    # SUM(CASE ...) lands as DECIMAL; cast to int for parity with pandas .sum().
    df["chairs"] = df["chairs"].astype(int)
    df["members"] = df["members"].astype(int)
    df["parties"] = df["parties"].astype(int)
    df["party_seats"] = df["party_seats_json"].map(
        lambda s: [(d["party"], int(d["seats"])) for d in json.loads(s)] if s else []
    )
    return df.drop(columns=["party_seats_json"])


# ── Per-committee party seats (flat alternative) ──────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_party_seats(chamber: str, committee: str | None = None) -> pd.DataFrame:
    """Long-format party seats per committee. Useful for charts and exports.

    If committee is supplied, filter to just that committee."""
    if committee is not None:
        return _safe(
            "SELECT committee, party, seats FROM v_committee_party_seats"
            " WHERE chamber = ? AND committee = ? ORDER BY seats DESC, party",
            [chamber, committee],
        )
    return _safe(
        "SELECT committee, party, seats FROM v_committee_party_seats"
        " WHERE chamber = ? ORDER BY committee, seats DESC, party",
        [chamber],
    )
