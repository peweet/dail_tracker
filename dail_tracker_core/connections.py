"""Streamlit-free connection builders for dail_tracker_core.

The member-overview connection needs a bespoke 4-phase view registration (an
ordered domain glob with no substitution, plus registry / external-links / vote
phases that each inject absolute parquet paths from ``config``). That logic used
to live in the Streamlit wrapper ``utility/data_access/member_overview_data.py``;
it now lives here so BOTH the Streamlit wrapper (via ``@st.cache_resource``) and
the read-only API (via FastAPI ``lifespan``) build the identical connection
without either importing the other — and without the API importing Streamlit.

``config`` is import-safe here (it is Streamlit-free — just path constants).
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.db import register_views

_log = logging.getLogger(__name__)


# Ordered — payments_base must precede its dependents; legislation_si_current_state
# must precede legislation_si_index (LEFT JOIN dependency). The order is load-bearing.
DOMAIN_FILES = [
    "attendance_member_year_summary.sql",
    "payments_base.sql",
    "payments_member_detail.sql",
    "payments_yearly_evolution.sql",
    "lobbying_revolving_door.sql",
    "legislation_index.sql",
    "legislation_si_current_state.sql",
    "legislation_si_index.sql",
    "v_debate_listings.sql",
    "member_debate_sections.sql",
    "member_questions.sql",
    "member_question_profile.sql",
    "member_question_focus_shift.sql",
    "member_zz_question_ministries.sql",
    "member_zz_question_top_topics.sql",
    "member_constituency_demographics.sql",
    "member_ministerial_tenure.sql",
]

# {MEMBER_PARQUET_PATH} + {SEANAD_MEMBER_PARQUET_PATH} substituted from config.
REGISTRY_FILES = ["member_registry.sql"]

# {EXTERNAL_LINKS_PARQUET_PATH} — optional (parquet may be absent on a fresh run).
EXTERNAL_LINKS_FILES = ["member_external_links.sql"]

# {PARQUET_PATH} + {SEANAD_VOTE_PARQUET_PATH} — vote_base must precede its dependents.
VOTE_FILES = ["vote_base.sql", "vote_td_summary.sql", "vote_member_detail.sql"]


def register_member_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Register the full member-overview view set onto ``conn`` (4 phases).

    swallow_errors=True throughout: a missing optional view degrades its section
    to an empty state rather than taking the whole connection down — the prior
    Streamlit behaviour, preserved.
    """
    # Phase 1 — domain views, no substitution, exact-filename "patterns" keep order.
    register_views(conn, DOMAIN_FILES, swallow_errors=True)

    # Phase 2 — member registry (absolute parquet paths from config).
    try:
        from config import SILVER_PARQUET_DIR

        register_views(
            conn,
            REGISTRY_FILES,
            substitutions={
                "{MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_members.parquet").as_posix(),
                "{SEANAD_MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_seanad_members.parquet").as_posix(),
            },
            swallow_errors=True,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not load registry: %s", exc)

    # Phase 3 — Wikidata external links (optional parquet).
    try:
        from config import SILVER_PARQUET_DIR

        register_views(
            conn,
            EXTERNAL_LINKS_FILES,
            substitutions={
                "{EXTERNAL_LINKS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_external_links.parquet").as_posix()
            },
            swallow_errors=True,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not load external-links: %s", exc)

    # Phase 4 — vote views (both houses, explicit two-parquet union in vote_base).
    try:
        from config import GOLD_SEANAD_VOTE_HISTORY_PARQUET, GOLD_VOTE_HISTORY_PARQUET

        register_views(
            conn,
            VOTE_FILES,
            substitutions={
                "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
                "{SEANAD_VOTE_PARQUET_PATH}": GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix(),
            },
            swallow_errors=True,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not load vote views: %s", exc)


def member_overview_conn() -> duckdb.DuckDBPyConnection:
    """A fresh in-memory DuckDB connection with the member-overview view set.

    The Streamlit layer wraps this in ``@st.cache_resource`` (one per session);
    the API builds it once at startup and hands out ``conn.cursor()`` per request.
    """
    conn = duckdb.connect()
    register_member_views(conn)
    return conn


def legislation_conn() -> duckdb.DuckDBPyConnection:
    """A fresh connection with the legislation + statutory-instruments view set.

    Same registration the Streamlit legislation page uses (no substitutions —
    these views read parquet via absolutize). Built once at API startup.
    """
    conn = duckdb.connect()
    register_views(conn, ["legislation_*.sql"], swallow_errors=True)
    return conn


# Domains the API serves beyond the member set. Each is registered with its own
# Streamlit page's glob (within-domain alphabetical order is proven by the pages).
# Only member_registry / member_external_links / vote_base need substitutions, and
# those are handled by register_member_views first; we pass the same substitutions
# to the vote_*.sql glob so the idempotent CREATE OR REPLACE of vote_base re-applies
# them correctly rather than registering a literal-placeholder view.
_API_DOMAIN_GLOBS = [
    "legislation_*.sql",
    "lobbying_*.sql",
    "charity_*.sql",
    "payments_*.sql",
    "committees_*.sql",
    "procurement_*.sql",
    # interests: detail glob first, then the zz_ index/summary that JOIN it — the
    # proven order from utility/data_access/interests_data.py. Reads parquet directly.
    "member_interests_*.sql",
    "member_zz_interests_*.sql",
    "vote_*.sql",
]


def api_conn() -> duckdb.DuckDBPyConnection:
    """One read-only connection with EVERY view set the API exposes.

    Built once at FastAPI startup; requests get a ``conn.cursor()``. All 111 views
    are CREATE OR REPLACE (idempotent), so the member set (registered first, in its
    load-bearing order, with substitutions) and the per-domain globs coexist.
    """
    conn = duckdb.connect()
    register_member_views(conn)  # member/registry/external/vote views + substitutions, explicit order

    subs: dict[str, str] = {}
    try:
        from config import GOLD_SEANAD_VOTE_HISTORY_PARQUET, GOLD_VOTE_HISTORY_PARQUET, SILVER_PARQUET_DIR

        subs = {
            "{MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_members.parquet").as_posix(),
            "{SEANAD_MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_seanad_members.parquet").as_posix(),
            "{EXTERNAL_LINKS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_external_links.parquet").as_posix(),
            "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
            "{SEANAD_VOTE_PARQUET_PATH}": GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix(),
        }
    except Exception as exc:  # noqa: BLE001
        _log.warning("api_conn: could not load config substitutions: %s", exc)

    register_views(conn, _API_DOMAIN_GLOBS, substitutions=subs, swallow_errors=True)
    return conn
