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
    # Per-(house, year) distinct plenary sitting dates — the denominator for the
    # hero stat-strip's plenary-attendance figure ("53 of 94 sitting days").
    # Standalone (reads the silver sitting tables), so order is unconstrained.
    "attendance_chamber_sitting_days.sql",
    # Participation & absence model (replaces the censored TAA "sitting days"
    # ranking). Each reads its gold parquet directly — no inter-view deps, so
    # order among them is unconstrained.
    "attendance_participation_turnout.sql",
    "attendance_participation_absences.sql",
    "attendance_participation_divergence.sql",
    "attendance_taa_compliance.sql",
    "payments_base.sql",
    "payments_member_detail.sql",
    "payments_yearly_evolution.sql",
    # v_lobbying_revolving_door LEFT JOINs v_lobbying_base_member_codes (the shared
    # normalised member-name → code resolver). The base view must register FIRST —
    # this connection registers revolving_door explicitly (not via the lobbying_*.sql
    # glob), so the base view has to be listed here too, immediately ahead of it.
    "lobbying_base_member_codes.sql",
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

# {NEWS_MENTIONS_PARQUET_PATH} — optional (parquet may be absent on a fresh run).
# member_news_feed.sql JOINs v_member_news_mentions + v_member_registry_all (the
# cross-member "In the News" feed), so it must register AFTER member_news_mentions.sql
# here AND after the registry phase (REGISTRY_FILES, registered first below).
NEWS_MENTIONS_FILES = ["member_news_mentions.sql", "member_news_feed.sql"]

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


def _member_view_substitutions() -> dict[str, str]:
    """Absolute parquet-path substitutions for the member view set.

    One source of truth shared by ``register_member_views`` (per phase) and
    ``api_conn`` (re-applied to the domain globs). Extra keys are harmless:
    ``register_views`` only substitutes the placeholders a given ``.sql`` actually
    references, so passing the full dict to every phase is safe. Returns ``{}`` if
    ``config`` can't be imported (e.g. a fresh Cloud clone); every caller passes
    ``swallow_errors=True``, so a missing path degrades its section to an empty
    state rather than taking the connection down.
    """
    try:
        from config import (
            GOLD_SEANAD_VOTE_HISTORY_PARQUET,
            GOLD_SPEECHES_FACT_FULL_PARQUET,
            GOLD_SPEECHES_FACT_PARQUET,
            GOLD_VOTE_HISTORY_PARQUET,
            SILVER_PARQUET_DIR,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not import config substitutions: %s", exc)
        return {}

    # Prefer the FULL speech fact (all years + full text, gitignored) when present —
    # local + API; fall back to the committed lite slice on a fresh Cloud clone.
    speech_path = (
        GOLD_SPEECHES_FACT_FULL_PARQUET if GOLD_SPEECHES_FACT_FULL_PARQUET.exists() else GOLD_SPEECHES_FACT_PARQUET
    )
    return {
        "{MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_members.parquet").as_posix(),
        "{SEANAD_MEMBER_PARQUET_PATH}": (SILVER_PARQUET_DIR / "flattened_seanad_members.parquet").as_posix(),
        # Historic backfill (former members + member×term sidecar) for v_member_registry_all.
        "{HISTORIC_DAIL_PARQUET_PATH}": (SILVER_PARQUET_DIR / "historic_members_dail.parquet").as_posix(),
        "{HISTORIC_SEANAD_PARQUET_PATH}": (SILVER_PARQUET_DIR / "historic_members_seanad.parquet").as_posix(),
        "{MEMBER_TERMS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_terms.parquet").as_posix(),
        "{EXTERNAL_LINKS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_external_links.parquet").as_posix(),
        "{CONTACT_DETAILS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "member_contact_details.parquet").as_posix(),
        "{NEWS_MENTIONS_PARQUET_PATH}": (SILVER_PARQUET_DIR / "news_mentions.parquet").as_posix(),
        "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
        "{SEANAD_VOTE_PARQUET_PATH}": GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix(),
        "{SPEECH_FACT_PARQUET_PATH}": speech_path.as_posix(),
    }


def _register_phase(conn: duckdb.DuckDBPyConnection, name: str, files: list[str], subs: dict[str, str]) -> None:
    """Register one ordered phase of the member view set, degrading on error.

    ``swallow_errors=True`` keeps a missing optional view from taking the whole
    connection down; an outright registration failure is logged and skipped.
    """
    try:
        register_views(conn, files, substitutions=subs, swallow_errors=True)
    except Exception as exc:  # noqa: BLE001
        _log.warning("member views: could not load %s: %s", name, exc)


def register_member_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Register the full member-overview view set onto ``conn``.

    swallow_errors=True throughout: a missing optional view degrades its section
    to an empty state rather than taking the whole connection down — the prior
    Streamlit behaviour, preserved.
    """
    # Phase 1 — domain views, no substitution, exact-filename "patterns" keep order.
    register_views(conn, DOMAIN_FILES, swallow_errors=True)

    # Phases 2-5 — registry / external-links / contact / news / votes / speeches,
    # each injecting absolute parquet paths from config. Order is load-bearing:
    # registry precedes member_salary + registry_all; vote_base / speech_base
    # precede their dependents. The shared subs dict carries every placeholder;
    # each .sql consumes only the keys it references.
    subs = _member_view_substitutions()
    _register_phase(conn, "registry", REGISTRY_FILES, subs)
    _register_phase(conn, "external-links", EXTERNAL_LINKS_FILES, subs)
    _register_phase(conn, "contact-details", CONTACT_DETAILS_FILES, subs)
    _register_phase(conn, "news-mentions", NEWS_MENTIONS_FILES, subs)
    _register_phase(conn, "vote views", VOTE_FILES, subs)
    _register_phase(conn, "speech views", SPEECH_FILES, subs)


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
    "constituency_la_chief_executives.sql",  # council-grain CE roster (reads _meta CSV; no deps)
    "constituency_la_lpt_adjustment.sql",  # council-grain LPT local-adjustment-factor votes (reads _meta CSV; no deps)
    # Your-Councillors gold views — each reads a data/_meta CSV directly (no inter-view deps).
    "constituency_la_councillors.sql",  # elected-member roster by LEA
    "constituency_la_council_meeting_coverage.sql",  # per-council data-state tier (honest degradation)
    "constituency_la_councillor_votes.sql",  # named roll-call votes (Carlow only)
    "constituency_la_meeting_agendas.sql",  # what each council tabled (agenda)
    "constituency_la_standing_orders.sql",  # how agendas are formulated + voting rule (~8 councils)
    "constituency_la_planning_overturn.sql",  # council ABP-overturn rate (reads silver parquet; no deps)
    "constituency_la_derelict_sites_levy.sql",  # council Derelict Sites Levy enforcement (reads gold parquet; no deps)
    "constituency_la_collection_rates.sql",  # council NOAC M2 collection rates (reads gold parquet; no deps)
    "constituency_la_noac_scorecard.sql",  # council NOAC scorecard (finance/workforce/roads/fire/litter; reads gold parquet; no deps)
    "constituency_la_noac_scorecard_history.sql",  # scorecard metrics 2022-2024 for the trend sparklines (reads gold parquet; no deps)
    "constituency_la_noac_indicators.sql",  # full NOAC indicator set (~125 series) for the All-indicators drill-down (reads gold parquet; no deps)
    "constituency_la_accountability_summary.sql",  # 1-row national headline; MUST follow the 3 LA views above
    "constituency_la_cash_signals.sql",  # 3 cash figures co-located (M1+M2+derelict); JOINs scorecard+collection_rates+derelict, so MUST follow them
    "constituency_members.sql",
    "constituency_party_breakdown.sql",
    "constituency_registry.sql",
    "constituency_house_work.sql",
    "constituency_map_layers.sql",  # choropleth layers; JOINs registry + house_work
    "constituency_housing_context.sql",
    "constituency_ssha_waiting_list.sql",
    "constituency_waiting_composition.sql",  # JOINs v_ssha_waiting_list_composition (registered below)
    "constituency_council_housing_performance.sql",
    "constituency_la_housing_performance.sql",  # council-grain DISTINCT of the above (for the LG page)
    "constituency_la_map_layers.sql",  # LG choropleth layers; JOINs the four v_la_* views above
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
    # The national SSHA composition view — constituency_waiting_composition JOINs it.
    register_views(conn, ["housing_ssha_waiting_list_composition.sql"], swallow_errors=True)
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
    # ministerial diaries (who ministers meet x lobbying register). Views absolutize their
    # own gold parquet paths; independent of the member set, so order-insensitive here.
    "ministerial_diary_*.sql",
    # corporate notices (Iris Oifigiúil gazette): distress/register notices + CRO/CBI xref +
    # receiver-appointer/operator-firm precomputed gold. Same single glob the Streamlit page
    # uses (utility/data_access/corporate_data.get_corporate_conn); alphabetical within-domain
    # order is proven there, and swallow_errors degrades a missing optional view, not the conn.
    "corporate_*.sql",
]


def api_conn() -> duckdb.DuckDBPyConnection:
    """One read-only connection with EVERY view set the API exposes.

    Built once at FastAPI startup; requests get a ``conn.cursor()``. All 111 views
    are CREATE OR REPLACE (idempotent), so the member set (registered first, in its
    load-bearing order, with substitutions) and the per-domain globs coexist.
    """
    conn = duckdb.connect()
    register_member_views(conn)  # member/registry/external/vote views + substitutions, explicit order

    # Re-apply the same substitutions to the domain globs: their idempotent
    # CREATE OR REPLACE of vote_base / member_registry must re-inject the paths
    # rather than register a literal-placeholder view.
    register_views(conn, _API_DOMAIN_GLOBS, substitutions=_member_view_substitutions(), swallow_errors=True)

    # Full attendance view set. The member set already registered the participation +
    # year-rank subset (in load-bearing order); this glob adds summary / member_summary /
    # missing_members for the attendance resource. All CREATE OR REPLACE = idempotent, and
    # the sorted glob keeps member_year_summary ahead of its dependent year_rank.
    register_views(conn, ["attendance_*.sql"], swallow_errors=True)
    # National housing screen — and the SSHA composition view the constituency set JOINs,
    # so this must precede CONSTITUENCY_FILES below. Each housing view reads gold directly.
    register_views(conn, ["housing_*.sql"], swallow_errors=True)
    # CSO general-government finance series (publicfinance resource). Self-contained.
    register_views(conn, ["publicfinance_*.sql"], swallow_errors=True)
    # Local-government / your-councillors / per-constituency dossier views, in the proven
    # dependency order (same list constituency_conn uses). They JOIN the member, interests,
    # procurement and housing view sets already registered above — no substitutions (these
    # read their parquet via absolutize). swallow_errors degrades a missing optional fact to
    # one empty section, not a dead connection.
    register_views(conn, CONSTITUENCY_FILES, swallow_errors=True)
    return conn
