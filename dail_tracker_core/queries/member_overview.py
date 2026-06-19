"""Member-overview retrieval — Streamlit-free.

Extracted verbatim from the ~25 inline ``@st.cache_data`` query helpers that
used to live in ``utility/pages_code/member_overview.py``. This is the single
largest retrieval surface in the app: 24 retrieval functions across 18 views
spanning attendance, votes, payments, lobbying, legislation, statutory
instruments, ministerial tenure, parliamentary questions, debates, constituency
demographics, external links and the member registry.

Each function takes an explicit ``conn`` and returns a ``QueryResult`` (the old
page helper ``_q`` swallowed every error into an empty DataFrame and also
returned empty on ``conn is None``; both now map to ``unavailable``). All the
dict / list / scalar / fallback *shaping* stays in the thin page wrappers — this
module is pure SELECT / WHERE / ORDER BY / LIMIT, with the same dynamic-WHERE
builders the page used (questions feed, debate filters).

Connection is built by ``utility/data_access/member_overview_data.py`` (a bespoke
4-phase ``register_views`` bootstrap with per-domain parquet-path substitutions);
that stays in the Streamlit ``@st.cache_resource`` wrapper because it needs
``config`` paths. These functions only consume the resulting connection.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection | None, sql: str, params: list | None = None) -> QueryResult:
    if conn is None:
        return QueryResult.unavailable("member_overview: no connection")
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.warning("member_overview | %s | %s", sql[:80], exc)
        return QueryResult.unavailable(f"member_overview query failed: {exc}")


# ── Registry / identity ───────────────────────────────────────────────────────


def member_list(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(
        conn,
        "SELECT unique_member_code, member_name, party_name, constituency, house"
        " FROM v_member_registry ORDER BY member_name",
    )


def join_key_by_name(conn: duckdb.DuckDBPyConnection, name: str, house: str | None = None) -> QueryResult:
    if house:
        return _run(
            conn,
            "SELECT unique_member_code FROM v_member_registry WHERE member_name = ? AND house = ? LIMIT 1",
            [name, house],
        )
    return _run(
        conn,
        "SELECT unique_member_code FROM v_member_registry WHERE member_name = ? LIMIT 1",
        [name],
    )


def member_house(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT house FROM v_member_registry WHERE unique_member_code = ? ORDER BY house DESC LIMIT 1",
        [join_key],
    )


def identity_attendance(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """Identity from the attendance summary (carries the latest year). The page
    tries this first, then falls back to identity_registry on miss."""
    return _run(
        conn,
        "SELECT member_name, party_name, constituency, is_minister, year"
        " FROM v_attendance_member_year_summary"
        " WHERE unique_member_code = ? ORDER BY year DESC LIMIT 1",
        [join_key],
    )


def identity_registry(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """Canonical identity fallback (no year column)."""
    return _run(
        conn,
        "SELECT member_name, party_name, constituency, is_minister"
        " FROM v_member_registry WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )


# ── Attendance ────────────────────────────────────────────────────────────────


def att_all_years(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT year, attended_count, is_minister"
        " FROM v_attendance_member_year_summary"
        " WHERE unique_member_code = ? ORDER BY year DESC LIMIT 20",
        [join_key],
    )


def att_rank(conn: duckdb.DuckDBPyConnection, join_key: str, year: int) -> QueryResult:
    """The member's rank_high for one year (the page pairs this with att_rank_total)."""
    return _run(
        conn,
        "SELECT rank_high FROM v_attendance_year_rank WHERE unique_member_code = ? AND year = ? LIMIT 1",
        [join_key, year],
    )


def att_rank_total(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """Size of the ranked field for a year+house (TDs ranked among TDs)."""
    return _run(
        conn,
        "SELECT COUNT(*) AS n FROM v_attendance_year_rank WHERE year = ? AND house = ?",
        [year, house],
    )


# ── External links / votes / payments ─────────────────────────────────────────


def external_links(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT wikipedia_url, twitter_url, bluesky_url, facebook_url,"
        " instagram_url, website_url"
        " FROM v_member_external_links WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )


def votes_summary(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        [join_key],
    )


def pay_overview(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT payment_year, total_paid, taa_band_label, payment_count"
        " FROM v_payments_yearly_evolution"
        " WHERE unique_member_code = ? ORDER BY payment_year DESC LIMIT 20",
        [join_key],
    )


def pay_grand_total(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """One-row SUM of all payments for the member (presentation-layer scalar —
    the page floats the single ``total`` cell with an NaN/empty guard)."""
    return _run(
        conn,
        "SELECT SUM(amount_num) AS total FROM v_payments_member_detail WHERE unique_member_code = ?",
        [join_key],
    )


def salary(conn: duckdb.DuckDBPyConnection, join_key: str, house: str) -> QueryResult:
    """Statutory salary RATE row for a member (basic + highest current office
    allowance). Keyed on (code, house) — codes are not globally unique across
    houses. This is a published set rate, NOT earned pay nor the PSA expense
    allowances (see v_member_salary header)."""
    return _run(
        conn,
        "SELECT basic_label, basic_rate, current_office, office_label, office_allowance,"
        " total_statutory_rate_eur, is_office_holder, effective_from, source_doc, source_url"
        " FROM v_member_salary WHERE unique_member_code = ? AND house = ? LIMIT 1",
        [join_key, house],
    )


def lobbying_rd(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT individual_name, former_position, return_count, distinct_firms"
        " FROM v_lobbying_revolving_door WHERE unique_member_code = ? LIMIT 5",
        [join_key],
    )


# ── Legislation / SIs / ministerial ───────────────────────────────────────────


def legislation(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT bill_title, bill_status, bill_year, oireachtas_url"
        " FROM v_legislation_index"
        " WHERE sponsor_join_key = ?"
        " ORDER BY introduced_date DESC NULLS LAST LIMIT 50",
        [join_key],
    )


def si_signed(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """SIs the member signed as a departmental minister (si_minister_member_code)."""
    return _run(
        conn,
        "SELECT si_id, si_year, si_title, si_signed_date, si_operation,"
        " si_department_label, si_is_eu, eisb_url"
        " FROM v_statutory_instruments"
        " WHERE si_minister_member_code = ?"
        " ORDER BY si_signed_date DESC NULLS LAST",
        [join_key],
    )


def ministerial_roles(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """Ministerial posts held (Wikidata tenure spine; 2011→present)."""
    return _run(
        conn,
        "SELECT department_label, minister_name, start_date, end_date,"
        " is_current, tenure_days"
        " FROM v_member_ministerial_tenure"
        " WHERE unique_member_code = ?"
        " ORDER BY start_date DESC",
        [join_key],
    )


# ── Constituency demographics ─────────────────────────────────────────────────


def constituency_context(conn: duckdb.DuckDBPyConnection, constituency: str) -> QueryResult:
    """One demographics row for a constituency name (page guards the empty name)."""
    return _run(
        conn,
        "SELECT population_2022, population_per_td, td_seats,"
        " boundaries_label, source_key"
        " FROM v_member_constituency_demographics"
        " WHERE constituency_name = ?",
        [constituency],
    )


# ── Parliamentary questions ───────────────────────────────────────────────────


def question_profile(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT total_qs, distinct_ministries, top_ministry, top_count, top_pct"
        " FROM v_member_question_profile WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )


def question_focus_shift(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT past_top, past_n, past_year_min, past_year_max,"
        " recent_top, recent_n, recent_year_min, recent_year_max"
        " FROM v_member_question_focus_shift WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )


def question_years(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT question_year FROM v_member_questions"
        " WHERE unique_member_code = ? AND question_year IS NOT NULL"
        " ORDER BY question_year DESC",
        [join_key],
    )


def question_ministries(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """Per-TD distinct ministries ordered by COUNT desc (rollup is in the view)."""
    return _run(
        conn,
        "SELECT ministry FROM v_member_question_ministries WHERE unique_member_code = ? ORDER BY n DESC, ministry ASC",
        [join_key],
    )


def question_top_topics(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """Top-3 topics for a TD (rollup is in v_member_question_top_topics)."""
    return _run(
        conn,
        "SELECT topic, n FROM v_member_question_top_topics"
        " WHERE unique_member_code = ?"
        " ORDER BY n DESC, topic ASC LIMIT 3",
        [join_key],
    )


def question_feed(
    conn: duckdb.DuckDBPyConnection,
    join_key: str,
    year: int | None = None,
    qtype: str | None = None,
    ministry: str | None = None,
    topic: str | None = None,
    search_text: str | None = None,
) -> QueryResult:
    """Question feed. Filters AND together; free-text is case-insensitive ILIKE
    with %wrap. LIMIT 10000 (page paginates client-side), matching prior behaviour."""
    clauses = ["unique_member_code = ?"]
    params: list = [join_key]
    if year is not None:
        clauses.append("question_year = ?")
        params.append(year)
    if qtype:
        clauses.append("question_type = ?")
        params.append(qtype)
    if ministry:
        clauses.append("ministry = ?")
        params.append(ministry)
    if topic:
        clauses.append("topic = ?")
        params.append(topic)
    if search_text:
        clauses.append("question_text ILIKE ?")
        params.append(f"%{search_text}%")
    return _run(
        conn,
        "SELECT question_date, question_type, ministry, topic, question_text,"
        " question_ref, oireachtas_url"
        f" FROM v_member_questions WHERE {' AND '.join(clauses)}"
        " ORDER BY question_date DESC LIMIT 10000",
        params,
    )


# ── Debates ───────────────────────────────────────────────────────────────────


def debate_years(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT debate_year FROM v_member_debate_sections"
        " WHERE unique_member_code = ? AND debate_year IS NOT NULL"
        " ORDER BY debate_year DESC LIMIT 30",
        [join_key],
    )


def debate_topics(conn: duckdb.DuckDBPyConnection, join_key: str, year: int | None = None) -> QueryResult:
    clauses = ["unique_member_code = ?", "topic IS NOT NULL"]
    params: list = [join_key]
    if year is not None:
        clauses.append("debate_year = ?")
        params.append(year)
    return _run(
        conn,
        f"SELECT DISTINCT topic FROM v_member_debate_sections WHERE {' AND '.join(clauses)} ORDER BY topic LIMIT 100",
        params,
    )


def debate_sections(
    conn: duckdb.DuckDBPyConnection,
    join_key: str,
    year: int | None = None,
    topic: str | None = None,
) -> QueryResult:
    """Debate sections a TD raised a question in (retrieval-only filter)."""
    clauses = ["unique_member_code = ?"]
    params: list = [join_key]
    if year is not None:
        clauses.append("debate_year = ?")
        params.append(year)
    if topic:
        clauses.append("topic = ?")
        params.append(topic)
    return _run(
        conn,
        "SELECT debate_date, debate_section_id, chamber, topic,"
        " question_count, oireachtas_url"
        " FROM v_member_debate_sections"
        f" WHERE {' AND '.join(clauses)}"
        " ORDER BY debate_date DESC LIMIT 1000",
        params,
    )


# ── Speeches (floor contributions) ─────────────────────────────────────────────


def speech_summary(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """One-row header aggregate for the Debates section (or empty if none)."""
    return _run(conn, "SELECT * FROM v_member_speech_summary WHERE unique_member_code = ?", [join_key])


def speech_years(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT year FROM v_member_speeches"
        " WHERE unique_member_code = ? AND year IS NOT NULL ORDER BY year DESC LIMIT 30",
        [join_key],
    )


def speech_business(conn: duckdb.DuckDBPyConnection, join_key: str) -> QueryResult:
    """Business groupings the member contributed to (selectbox source).

    Pre-aggregated in v_member_speech_business — retrieval-only SELECT here.
    """
    return _run(
        conn,
        "SELECT business, contribution_count AS n FROM v_member_speech_business"
        " WHERE unique_member_code = ? ORDER BY n DESC LIMIT 100",
        [join_key],
    )


def member_speeches(
    conn: duckdb.DuckDBPyConnection,
    join_key: str,
    year: int | None = None,
    contribution_type: str | None = None,
    business: str | None = None,
    irish_only: bool = False,
    search: str | None = None,
) -> QueryResult:
    """The paginated floor-contribution feed (retrieval-only dynamic WHERE)."""
    clauses = ["unique_member_code = ?"]
    params: list = [join_key]
    if year is not None:
        clauses.append("year = ?")
        params.append(year)
    if contribution_type:
        clauses.append("contribution_type = ?")
        params.append(contribution_type)
    if business:
        clauses.append("business = ?")
        params.append(business)
    if irish_only:
        clauses.append("is_irish")
    if search:
        clauses.append("speech_text ILIKE '%' || ? || '%'")
        params.append(search)
    return _run(
        conn,
        "SELECT speech_date, house, chamber, section_heading, business, contribution_type,"
        " speaker_raw, recorded_time, speech_text, word_count, is_irish, irish_score, debate_url"
        " FROM v_member_speeches"
        f" WHERE {' AND '.join(clauses)}"
        " ORDER BY speech_date DESC, contribution_order ASC LIMIT 2000",
        params,
    )
