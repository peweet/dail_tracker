"""
Member Overview data-access layer — unified DuckDB connection.

Loads all per-domain views needed by member_overview.py in dependency order.

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_UTIL = _HERE.parent
if str(_UTIL) not in sys.path:
    sys.path.insert(0, str(_UTIL))

import duckdb  # noqa: E402 — sys.path mutation above is required before this import
import streamlit as st  # noqa: E402 — sys.path mutation above is required before streamlit import

from data_access._sql_registry import (  # noqa: E402 — sys.path mutation above
    SQL_VIEWS_DIR as _SQL_VIEWS,
)
from data_access._sql_registry import (  # noqa: E402
    absolutize_data_paths as _absolutize_data_paths,
)

_log = logging.getLogger(__name__)


# Ordered — payments_base must precede its dependents
_DOMAIN_FILES = [
    "attendance_member_year_summary.sql",
    "payments_base.sql",
    "payments_member_detail.sql",
    "payments_yearly_evolution.sql",
    "lobbying_revolving_door.sql",
    "legislation_index.sql",
    "legislation_si_index.sql",
    "v_debate_listings.sql",
    "member_debate_sections.sql",
    # Questions feature (added 2026-05-27 after the 1000-row API cap was lifted
    # in services/member_paginated.py). Three views: feed, per-TD aggregate
    # profile, and focus-shift detector. All read from
    # data/silver/parquet/questions.parquet which is 264k rows post-backfill.
    "member_questions.sql",
    "member_question_profile.sql",
    "member_question_focus_shift.sql",
    # Per-TD aggregates over v_member_questions. Must come AFTER member_questions.sql.
    # Without these the ministry-filter selectbox + Top Topics card silently fall
    # back to empty state — bug surfaced 2026-05-31 in the Member Overview crawl
    # via the server-side Catalog Error logs.
    "member_zz_question_ministries.sql",
    "member_zz_question_top_topics.sql",
    # Constituency civic context (added 2026-05-31).
    # Sourced from cso_fy005.parquet (CSO PxStat — Census 2022 population per
    # Dáil constituency, 2017 boundaries). Wide one row per constituency.
    # See sql_views/member_constituency_demographics.sql for the 36/43 vs
    # 7/43 boundary-split match coverage note.
    "member_constituency_demographics.sql",
]

# {MEMBER_PARQUET_PATH} substituted with absolute path from config
_REGISTRY_FILES = [
    "member_registry.sql",
]

# {EXTERNAL_LINKS_PARQUET_PATH} substituted with absolute path from config.
# Output of wikidata_socials_etl.py — optional: if the file is missing (first
# pipeline run, or Wikidata fetch failed), _load_sql swallows the error and
# the hero block falls back to "no chips" gracefully.
_EXTERNAL_LINKS_FILES = [
    "member_external_links.sql",
]

# {PARQUET_PATH} substituted with absolute path from config
_VOTE_FILES = [
    "vote_td_summary.sql",
    "vote_member_detail.sql",
]


def _load_sql(conn, fpath: Path, substitutions: dict[str, str]) -> None:
    if not fpath.exists():
        _log.warning("member_overview: SQL file not found: %s", fpath.name)
        return
    try:
        sql = fpath.read_text(encoding="utf-8")
        for key, val in substitutions.items():
            sql = sql.replace(key, val)
        conn.execute(_absolutize_data_paths(sql))
    except Exception as exc:
        _log.warning("member_overview view failed: %s | %s", fpath.name, exc)


@st.cache_resource
def get_member_overview_conn():
    conn = duckdb.connect()

    # Plain views — no path substitution needed
    for fname in _DOMAIN_FILES:
        _load_sql(conn, _SQL_VIEWS / fname, {})

    # Member registry — absolute path injected to avoid CWD ambiguity
    try:
        from config import SILVER_PARQUET_DIR

        member_parquet = (SILVER_PARQUET_DIR / "flattened_members.parquet").as_posix()
        for fname in _REGISTRY_FILES:
            _load_sql(conn, _SQL_VIEWS / fname, {"{MEMBER_PARQUET_PATH}": member_parquet})
    except Exception as exc:
        _log.warning("member_overview: could not load member registry: %s", exc)

    # External links — Wikidata-sourced socials + Wikipedia. Parquet may be
    # absent if wikidata_socials_etl.py hasn't run yet; _load_sql logs the
    # failure and the hero falls back to no chips.
    try:
        from config import SILVER_PARQUET_DIR

        ext_links_parquet = (SILVER_PARQUET_DIR / "member_external_links.parquet").as_posix()
        for fname in _EXTERNAL_LINKS_FILES:
            _load_sql(conn, _SQL_VIEWS / fname, {"{EXTERNAL_LINKS_PARQUET_PATH}": ext_links_parquet})
    except Exception as exc:
        _log.warning("member_overview: could not load external-links view: %s", exc)

    # Vote views — absolute path injected
    try:
        from config import GOLD_VOTE_HISTORY_PARQUET

        vote_parquet = GOLD_VOTE_HISTORY_PARQUET.as_posix()
        for fname in _VOTE_FILES:
            _load_sql(conn, _SQL_VIEWS / fname, {"{PARQUET_PATH}": vote_parquet})
    except Exception as exc:
        _log.warning("member_overview: could not load vote views: %s", exc)

    return conn
