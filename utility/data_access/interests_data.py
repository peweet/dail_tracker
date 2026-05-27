"""
Interests data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/member_interests_*.sql
  (v_member_interests_detail + v_member_interests_index)
- All retrieval SQL for the interests page (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as Streamlit page files):
- read_parquet / read_csv
- duckdb.connect(":memory:") + register frame pattern
- CREATE VIEW / CREATE TABLE
- pandas groupby, merge, pivot
- Business metric definitions (the leaderboard rollup lives in
  v_member_interests_index, not here)

Pre-existing /interests page behaviour preserved exactly: same filter
options, same column contract, same row ordering, same leaderboard rank.
The page-side _load_interests / _fetch_filter_options / _fetch_interests /
_fetch_td_data / _fetch_member_index_fallback patterns are now thin SQL
SELECTs against the two registered views.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SQL_VIEWS = _PROJECT_ROOT / "sql_views"


def _absolutize_data_paths(sql: str) -> str:
    return sql.replace("'data/", f"'{_PROJECT_ROOT.as_posix()}/data/")


@st.cache_resource
def get_interests_conn() -> duckdb.DuckDBPyConnection:
    """Open a connection and register v_member_interests_detail and
    v_member_interests_index. The detail view file sorts before the index
    file so dependency order works under the alphabetical glob."""
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("member_interests_*.sql")):
        conn.execute(_absolutize_data_paths(sql_file.read_text(encoding="utf-8")))
    for sql_file in sorted(_SQL_VIEWS.glob("member_zz_interests_*.sql")):
        conn.execute(_absolutize_data_paths(sql_file.read_text(encoding="utf-8")))
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        conn = get_interests_conn()
        return conn.execute(sql, params or []).df()
    except Exception:
        return pd.DataFrame()


# ── Availability guard ────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_interests_availability(house: str) -> bool:
    """Return True iff v_member_interests_detail has any row for this house.

    Replaces the in-page parquet/CSV existence check + empty-frame check.
    A single COUNT(*) > 0 is enough; we don't materialise the whole frame
    just to find out whether data exists.
    """
    df = _safe(
        "SELECT 1 FROM v_member_interests_detail WHERE house = ? LIMIT 1",
        [house],
    )
    return not df.empty


# ── Filter options ────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_interests_filter_options(house: str) -> dict[str, list]:
    """{"years": [int], "members": [str]} for the sidebar / leaderboard
    filters. Pure retrieval against v_member_interests_detail."""
    years_df = _safe(
        "SELECT DISTINCT declaration_year FROM v_member_interests_detail"
        " WHERE house = ? AND declaration_year IS NOT NULL"
        " ORDER BY declaration_year DESC",
        [house],
    )
    years = years_df["declaration_year"].dropna().astype(int).tolist() if not years_df.empty else []

    members_df = _safe(
        "SELECT DISTINCT member_name FROM v_member_interests_detail"
        " WHERE house = ? AND member_name IS NOT NULL"
        " ORDER BY member_name",
        [house],
    )
    members = members_df["member_name"].tolist() if not members_df.empty else []

    return {"years": years, "members": members}


# ── Detail retrieval ──────────────────────────────────────────────────────────


_DETAIL_COLS = (
    "member_name, party_name, constituency, declaration_year,"
    " interest_category, interest_text, landlord_flag, property_flag"
)


@st.cache_data(ttl=300)
def fetch_interests(
    house: str,
    name_q: str = "",
    years: tuple[int, ...] = (),
    landlord_only: bool = False,
) -> pd.DataFrame:
    """Browse-list rows. Filters AND together. Limit 1000 matches the
    previous in-page behaviour."""
    clauses: list[str] = ["house = ?"]
    params: list = [house]
    if name_q:
        clauses.append("member_name ILIKE ?")
        params.append(f"%{name_q}%")
    if years:
        placeholders = ", ".join("?" for _ in years)
        clauses.append(f"declaration_year IN ({placeholders})")
        params.extend(int(y) for y in years)
    if landlord_only:
        clauses.append("landlord_flag = ?")
        params.append(True)
    where = " WHERE " + " AND ".join(clauses)
    return _safe(
        f"SELECT {_DETAIL_COLS} FROM v_member_interests_detail"
        f"{where} ORDER BY declaration_year DESC, member_name LIMIT 1000",
        params,
    )


@st.cache_data(ttl=300)
def fetch_td_interests(house: str, td_name: str) -> pd.DataFrame:
    """Every declaration for one TD across all years."""
    return _safe(
        f"SELECT {_DETAIL_COLS} FROM v_member_interests_detail"
        " WHERE house = ? AND member_name = ?"
        " ORDER BY declaration_year DESC, interest_category",
        [house, td_name],
    )


# ── Member index (ranked leaderboard) ─────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_member_index(house: str, year: int) -> pd.DataFrame:
    """Ranked member index for a house × year. The rank, counts and flags
    are all produced by v_member_interests_index — retrieval-only here."""
    return _safe(
        "SELECT rank, member_name, party_name, constituency,"
        " total_declarations, directorship_count, property_count, share_count,"
        " is_landlord, is_property_owner"
        " FROM v_member_interests_index"
        " WHERE house = ? AND declaration_year = ?"
        " ORDER BY rank",
        [house, int(year)],
    )
