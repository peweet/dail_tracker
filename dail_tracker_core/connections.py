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
    # v_attendance_year_rank ranks members within (year, house); it reads ONLY
    # v_attendance_member_year_summary (registered immediately above), so it must
    # follow it. Member-overview's hero stat-strip uses it for the "Rank X of Y
    # TDs" sub-label — omitting it silently blanked that line on every profile.
    "attendance_year_rank.sql",
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
# member_salary.sql joins v_member_registry and unpivots the office_N_name fields
# from {MEMBER_PARQUET_PATH}, so it must register AFTER member_registry.sql.
# member_registry_all.sql joins v_member_registry + the historic rosters, so it
# must also register AFTER member_registry.sql.
REGISTRY_FILES = ["member_registry.sql", "member_registry_all.sql", "member_salary.sql"]

# {EXTERNAL_LINKS_PARQUET_PATH} — optional (parquet may be absent on a fresh run).
EXTERNAL_LINKS_FILES = ["member_external_links.sql"]

# {CONTACT_DETAILS_PARQUET_PATH} — optional (parquet may be absent on a fresh run).
CONTACT_DETAILS_FILES = ["member_contact_details.sql"]

# {PARQUET_PATH} + {SEANAD_VOTE_PARQUET_PATH} — vote_base must precede its dependents.
VOTE_FILES = [
    "vote_base.sql",
    "vote_td_summary.sql",
    "vote_td_year_summary.sql",  # member-overview "Votes by year" chart
    "vote_member_detail.sql",
]

# {SPEECH_FACT_PARQUET_PATH} — speech_base (single unified parquet, carries house)
# must precede its dependents. Powers the member-overview Debates section.
SPEECH_FILES = [
    "speech_base.sql",
    "speech_member_detail.sql",
    "speech_member_summary.sql",
    "speech_member_business.sql",
]


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
                # Historic backfill (former members + member×term sidecar) for
                # v_member_registry_all. Extra keys are harmless to the other
                # REGISTRY_FILES, which don't reference them.
                "{HISTORIC_DAIL_PARQUET_PATH}": (SILVER_PARQUET_DIR / "historic_members_dail.parquet").as_posix(),
                "{HISTORIC_SEANAD_PARQUET_PATH}": (SILVER_PARQUET_DIR / "historic_members_seanad.parquet").as_posix(),
                "{MEMBER_TERMS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_terms.parquet").as_posix(),
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

    # Phase 3b — official contact details scraped from oireachtas.ie (optional parquet).
    try:
        from config import SILVER_PARQUET_DIR

        register_views(
            conn,
            CONTACT_DETAILS_FILES,
            substitutions={
                "{CONTACT_DETAILS_PARQUET_PATH}": (
                    SILVER_PARQUET_DIR / "member_contact_details.parquet"
                ).as_posix()
            },
            swallow_errors=True,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not load contact-details: %s", exc)

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

    # Phase 5 — speech views (unified gold parquet; speech_base precedes dependents).
    # Prefer the FULL fact (all years + full text, gitignored) when present — local
    # + API; fall back to the committed lite slice on a fresh Cloud clone.
    try:
        from config import GOLD_SPEECHES_FACT_FULL_PARQUET, GOLD_SPEECHES_FACT_PARQUET

        speech_path = (
            GOLD_SPEECHES_FACT_FULL_PARQUET if GOLD_SPEECHES_FACT_FULL_PARQUET.exists() else GOLD_SPEECHES_FACT_PARQUET
        )
        register_views(
            conn,
            SPEECH_FILES,
            substitutions={"{SPEECH_FACT_PARQUET_PATH}": speech_path.as_posix()},
            swallow_errors=True,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not load speech views: %s", exc)


def member_overview_conn() -> duckdb.DuckDBPyConnection:
    """A fresh in-memory DuckDB connection with the member-overview view set.

    The Streamlit layer wraps this in ``@st.cache_resource`` (one per session);
    the API builds it once at startup and hands out ``conn.cursor()`` per request.
    """
    conn = duckdb.connect()
    register_member_views(conn)
    return conn


# Per-constituency dossier views. Explicit order (not a glob) because
# constituency_council_context JOINs constituency_la_crosswalk, and the registry/
# members views JOIN the member + demographics views — all registered first.
CONSTITUENCY_FILES = [
    "constituency_la_crosswalk.sql",
    "constituency_members.sql",
    "constituency_party_breakdown.sql",
    "constituency_registry.sql",
    "constituency_house_work.sql",
    "constituency_map_layers.sql",  # choropleth layers; JOINs registry + house_work
    "constituency_housing_context.sql",
    "constituency_ssha_waiting_list.sql",
    "constituency_council_housing_performance.sql",
    "constituency_council_context.sql",
]


def constituency_conn() -> duckdb.DuckDBPyConnection:
    """A fresh connection for the per-constituency dossier page.

    Layers four view sets, in order: (1) the member set (registry + constituency
    demographics + questions/votes/speeches/attendance, with substitutions) via
    register_member_views; (2) the interests views the house-work view aggregates
    (landlord/property flags); (3) the procurement glob (council summary + AFS
    revenue/capital views the council-context view joins); (4) the constituency
    views themselves, in explicit dependency order. swallow_errors throughout so a
    missing optional fact degrades one section, not the whole page.
    """
    conn = duckdb.connect()
    register_member_views(conn)
    register_views(conn, ["member_interests_*.sql", "member_zz_interests_*.sql"], swallow_errors=True)
    register_views(conn, ["procurement_*.sql"], swallow_errors=True)
    register_views(conn, CONSTITUENCY_FILES, swallow_errors=True)
    return conn


def legislation_conn() -> duckdb.DuckDBPyConnection:
    """A fresh connection with the legislation + statutory-instruments view set.

    Same registration the Streamlit legislation page uses (no substitutions —
    these views read parquet via absolutize). Built once at API startup.
    """
    conn = duckdb.connect()
    register_views(conn, ["legislation_*.sql"], swallow_errors=True)
    return conn


def housing_conn() -> duckdb.DuckDBPyConnection:
    """A fresh connection for the national Housing screen — the SSHA waiting-list
    composition + totals views (self-contained: each reads gold parquet directly,
    no inter-view deps), so a plain glob in any order is safe.
    """
    conn = duckdb.connect()
    register_views(conn, ["housing_*.sql"], swallow_errors=True)
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
    "speech_*.sql",
    # SIPO political finance / judiciary bench / public appointments. These views
    # absolutize their own parquet paths (no substitution needed); the sipo glob's
    # alphabetical order is dependency-safe (ge2024_party_finance reads the
    # candidate/donations/expenses views, all earlier alphabetically). swallow_errors
    # degrades a missing optional parquet to one unavailable domain, not a dead conn.
    "sipo_*.sql",
    "judiciary_*.sql",
    "appointments_*.sql",
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
        from config import (
            GOLD_SEANAD_VOTE_HISTORY_PARQUET,
            GOLD_SPEECHES_FACT_FULL_PARQUET,
            GOLD_SPEECHES_FACT_PARQUET,
            GOLD_VOTE_HISTORY_PARQUET,
            SILVER_PARQUET_DIR,
        )

        _speech_path = (
            GOLD_SPEECHES_FACT_FULL_PARQUET if GOLD_SPEECHES_FACT_FULL_PARQUET.exists() else GOLD_SPEECHES_FACT_PARQUET
        )
        subs = {
            "{MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_members.parquet").as_posix(),
            "{SEANAD_MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_seanad_members.parquet").as_posix(),
            "{EXTERNAL_LINKS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_external_links.parquet").as_posix(),
            "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
            "{SEANAD_VOTE_PARQUET_PATH}": GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix(),
            "{SPEECH_FACT_PARQUET_PATH}": _speech_path.as_posix(),
        }
    except Exception as exc:  # noqa: BLE001
        _log.warning("api_conn: could not load config substitutions: %s", exc)

    register_views(conn, _API_DOMAIN_GLOBS, substitutions=subs, swallow_errors=True)
    return conn
