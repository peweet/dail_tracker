"""
SQL view contract tests — member registry, interests, questions, debate and
constituency-demographics views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import pytest

from ._view_test_helpers import (
    _DATA_BASE,
    CONTACT_DETAILS_PARQUET,
    EXTERNAL_LINKS_PARQUET,
    MEMBER_PARQUET,
    SILVER_PARQUET_DIR,
    _assert_cols,
    _con,
    _load,
    _result,
    _skip_missing,
    _src,
)

# ---------------------------------------------------------------------------
# MEMBER REGISTRY
# ---------------------------------------------------------------------------


def test_v_member_registry_executes():
    """Runs against test/fixtures/sql_views/silver/parquet/flattened_members.parquet
    by default; set DAIL_INTEGRATION_TESTS=1 to run against real pipeline output.
    """
    _skip_missing(MEMBER_PARQUET)
    con = _con()
    con.execute(_load("member_registry.sql"))
    result = _result(con, "v_member_registry")
    assert "unique_member_code" in result.columns
    assert "member_name" in result.columns
    assert "house" in result.columns  # Dáil/Seanad union column
    assert len(result) > 0


def test_v_member_registry_all_executes():
    """Historic-inclusive registry: current + former members with is_current /
    dails_served / served-year span. Skips in CI (no historic fixtures); runs on
    real pipeline output (DAIL_INTEGRATION_TESTS=1 or a dev box with the parquets).
    """
    historic_dail = SILVER_PARQUET_DIR / "historic_members_dail.parquet"
    historic_seanad = SILVER_PARQUET_DIR / "historic_members_seanad.parquet"
    member_terms = SILVER_PARQUET_DIR / "member_terms.parquet"
    _skip_missing(MEMBER_PARQUET, historic_dail, historic_seanad, member_terms)
    con = _con()
    con.execute(_load("member_registry.sql"))  # v_member_registry_all builds on it
    con.execute(_load("member_registry_all.sql"))
    result = _result(con, "v_member_registry_all")
    _assert_cols(
        result,
        "unique_member_code",
        "member_name",
        "house",
        "is_current",
        "dails_served",
        "served_from_year",
        "served_to_year",
    )
    # Must carry BOTH sitting and former members…
    counts = con.execute(
        "SELECT COUNT(*) FILTER (WHERE is_current) AS cur,"
        " COUNT(*) FILTER (WHERE NOT is_current) AS former FROM v_member_registry_all"
    ).fetchone()
    assert counts[0] > 0 and counts[1] > 0, "expected both current and former members"
    # …and be strictly ADDITIVE: the sitting set must equal v_member_registry exactly.
    reg_n = con.execute("SELECT COUNT(*) FROM v_member_registry").fetchone()[0]
    assert counts[0] == reg_n, "current-member count must equal v_member_registry (additive only)"


def test_v_member_ministerial_tenure_executes():
    """Ministerial tenure timeline — reads data/silver/ministerial_tenure.parquet.
    The columns are the contract dail_tracker_core.queries.ministerial relies on.
    """
    _skip_missing(_DATA_BASE / "data" / "silver" / "ministerial_tenure.parquet")
    con = _con()
    con.execute(_load("member_ministerial_tenure.sql"))
    result = _result(con, "v_member_ministerial_tenure")
    expected = {
        "department_key",
        "department_label",
        "minister_name",
        "unique_member_code",
        "start_date",
        "end_date",
        "is_current",
        "tenure_days",
        "wikidata_person",
        "wikidata_position",
    }
    _assert_cols(result, *expected)
    assert len(result) > 0
    # is_current must be a real boolean and at least one post should be filled.
    full = con.execute(
        "SELECT COUNT(*) FILTER (WHERE is_current) AS cur, COUNT(*) AS n FROM v_member_ministerial_tenure"
    ).fetchone()
    assert full[0] >= 1, "no sitting minister flagged is_current"
    # minister_name is the display field — never null.
    nulls = con.execute("SELECT COUNT(*) FROM v_member_ministerial_tenure WHERE minister_name IS NULL").fetchone()[0]
    assert nulls == 0


def test_v_member_salary_executes():
    """Statutory salary RATE view — basic (by House) + highest current office
    allowance, joined to the curated data/_meta/oireachtas_salary_rates.csv. The
    total must reconcile exactly to basic + office allowance, and basic must be
    one of the two published House rates.
    """
    _skip_missing(MEMBER_PARQUET, *_src("data/_meta/oireachtas_salary_rates.csv"))
    con = _con()
    con.execute(_load("member_registry.sql"))  # v_member_salary JOINs v_member_registry
    con.execute(_load("member_salary.sql"))
    result = _result(con, "v_member_salary")
    _assert_cols(
        result,
        "unique_member_code",
        "house",
        "basic_rate",
        "current_office",
        "office_allowance",
        "total_statutory_rate_eur",
        "is_office_holder",
        "source_doc",
        "source_url",
    )
    assert len(result) > 0
    # Total reconciles to basic + office allowance (no stray arithmetic).
    bad = con.execute(
        "SELECT COUNT(*) FROM v_member_salary"
        " WHERE total_statutory_rate_eur <> basic_rate + COALESCE(office_allowance, 0)"
    ).fetchone()[0]
    assert bad == 0, "total_statutory_rate_eur must equal basic_rate + office_allowance"
    # Basic salary is always one of the two published House rates — never NULL.
    off_house = con.execute(
        "SELECT COUNT(*) FROM v_member_salary WHERE basic_rate NOT IN (113679, 79614) OR basic_rate IS NULL"
    ).fetchone()[0]
    assert off_house == 0, "basic_rate must be a published TD/Senator rate"
    # Office allowance only ever attaches to a Dáil row (Seanad offices unmapped).
    seanad_oh = con.execute(
        "SELECT COUNT(*) FROM v_member_salary WHERE house = 'Seanad' AND is_office_holder"
    ).fetchone()[0]
    assert seanad_oh == 0, "Seanad office allowances are not mapped — should never flag is_office_holder"


def test_v_charity_financials_by_year_executes():
    """Per-charity annual financial series — reads charities/annual_reports.parquet.
    Must be strictly one row per (rcn, period_year); the source has up to 3.
    """
    _skip_missing(_DATA_BASE / "data" / "silver" / "charities" / "annual_reports.parquet")
    con = _con()
    con.execute(_load("charity_financials_by_year.sql"))
    con.execute(_load("charity_sector_totals_by_year.sql"))
    result = _result(con, "v_charity_financials_by_year")
    _assert_cols(result, "rcn", "period_year", "gross_income", "gross_expenditure", "gov_share")
    assert len(result) > 0
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT rcn, period_year, COUNT(*) c"
        " FROM v_charity_financials_by_year GROUP BY rcn, period_year HAVING c > 1)"
    ).fetchone()[0]
    assert dup == 0, "view is not one-row-per-(rcn, period_year)"
    # Sector rollup (depends on the per-year view) must be one row per year.
    totals = _result(con, "v_charity_sector_totals_by_year")
    _assert_cols(totals, "period_year", "n_charities", "total_gross_income")
    assert len(totals) > 0


def test_v_bill_amendment_intensity_executes():
    """Per-bill amendment activity — reads parquet/bill_amendments.parquet.
    One row per bill_id (= v_legislation_index key); ranked by amendment_lists.
    """
    _skip_missing(_DATA_BASE / "data" / "silver" / "parquet" / "bill_amendments.parquet")
    con = _con()
    con.execute(_load("legislation_bill_amendment_intensity.sql"))
    result = _result(con, "v_bill_amendment_intensity")
    _assert_cols(result, "bill_id", "bill_title", "amendment_lists", "committee_lists", "report_lists")
    assert len(result) > 0
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT bill_id, COUNT(*) c"
        " FROM v_bill_amendment_intensity GROUP BY bill_id HAVING c > 1)"
    ).fetchone()[0]
    assert dup == 0, "view is not one-row-per-bill"


def test_v_member_external_links_executes():
    """Runs against the Wikidata-sourced external-links fixture by default.
    The view's columns are the contract the member-overview hero relies on
    when building chips — a rename here is a UI break, surface it loudly.
    """
    _skip_missing(EXTERNAL_LINKS_PARQUET)
    con = _con()
    con.execute(_load("member_external_links.sql"))
    result = _result(con, "v_member_external_links")
    expected = {
        "unique_member_code",
        "wikidata_qid",
        "wikipedia_url",
        "twitter_handle",
        "twitter_url",
        "bluesky_handle",
        "bluesky_url",
        "facebook_id",
        "facebook_url",
        "instagram_handle",
        "instagram_url",
        "website_url",
    }
    assert expected.issubset(set(result.columns))
    assert len(result) > 0


def test_v_member_contact_details_executes():
    """Official contact details scraped from oireachtas.ie member profiles.
    The column set is the contract the Member Overview "Contact" block relies
    on — a rename here is a UI break, surface it loudly. Every field except the
    join key is nullable (sparse coverage is expected and surfaced honestly)."""
    _skip_missing(CONTACT_DETAILS_PARQUET)
    con = _con()
    con.execute(_load("member_contact_details.sql"))
    result = _result(con, "v_member_contact_details")
    expected = {
        "unique_member_code",
        "address",
        "phone_primary",
        "phone_all",
        "email",
        "website_url",
        "profile_url",
        "source_url",
        "scraped_date",
    }
    assert expected.issubset(set(result.columns))
    assert len(result) > 0


# ---------------------------------------------------------------------------
# MEMBER INTERESTS VIEWS
# ---------------------------------------------------------------------------

_INTERESTS_SRC = "data/silver/parquet/dail_member_interests_combined.parquet"


@pytest.mark.sql
def test_v_member_interests_detail_executes():
    _skip_missing(*_src(_INTERESTS_SRC))
    con = _con()
    con.execute(_load("member_interests_detail.sql"))
    result = _result(con, "v_member_interests_detail")
    _assert_cols(result, "member_name", "declaration_year", "interest_category", "interest_text", "house")
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_interests_index_executes():
    """Ranking index reads v_member_interests_detail — load detail first."""
    _skip_missing(*_src(_INTERESTS_SRC))
    con = _con()
    con.execute(_load("member_interests_detail.sql"))
    con.execute(_load("member_zz_interests_index.sql"))
    result = _result(con, "v_member_interests_index")
    _assert_cols(
        result,
        "rank",
        "house",
        "member_name",
        "total_declarations",
        "directorship_count",
        "property_count",
        "is_landlord",
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_interests_index_alltime_executes():
    """All-time ranking pools every year per member and ranks within each house.
    Reads v_member_interests_detail — load detail first. Rank must restart at 1
    per house and each member appears at most once per house."""
    _skip_missing(*_src(_INTERESTS_SRC))
    con = _con()
    con.execute(_load("member_interests_detail.sql"))
    con.execute(_load("member_zz_interests_index_alltime.sql"))
    result = _result(con, "v_member_interests_index_alltime")
    _assert_cols(
        result,
        "rank",
        "house",
        "member_name",
        "total_declarations",
        "directorship_count",
        "property_count",
        "is_landlord",
    )
    assert len(result) > 0
    # Invariants over the FULL view (not the LIMIT-5 sample _result returns).
    # One row per (house, member) — the year is collapsed.
    dupes = con.execute(
        "SELECT COUNT(*) FROM ("
        " SELECT house, member_name FROM v_member_interests_index_alltime"
        " GROUP BY house, member_name HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dupes == 0
    # Rank is chamber-scoped: each house starts at rank 1.
    bad_houses = con.execute(
        "SELECT COUNT(*) FROM ("
        " SELECT house FROM v_member_interests_index_alltime"
        " GROUP BY house HAVING MIN(rank) <> 1)"
    ).fetchone()[0]
    assert bad_houses == 0


# ---------------------------------------------------------------------------
# MEMBER QUESTIONS / DEBATE / CONSTITUENCY VIEWS
# ---------------------------------------------------------------------------
#
# The questions feed + its two per-TD aggregates (ministries, top_topics) read
# v_member_questions; the aggregates must be created AFTER it. This ordering is
# the exact bug that surfaced 2026-05-31 (empty ministry filter) — pinned here.

_QUESTIONS_SRC = "data/silver/parquet/questions.parquet"


@pytest.mark.sql
def test_v_member_questions_executes():
    _skip_missing(*_src(_QUESTIONS_SRC))
    con = _con()
    con.execute(_load("member_questions.sql"))
    result = _result(con, "v_member_questions")
    _assert_cols(
        result, "unique_member_code", "td_name", "question_date", "ministry", "topic", "question_text", "oireachtas_url"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_question_profile_executes():
    _skip_missing(*_src(_QUESTIONS_SRC))
    con = _con()
    con.execute(_load("member_question_profile.sql"))
    result = _result(con, "v_member_question_profile")
    _assert_cols(result, "unique_member_code", "total_qs", "distinct_ministries", "top_ministry", "top_pct")
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_question_focus_shift_executes():
    _skip_missing(*_src(_QUESTIONS_SRC))
    con = _con()
    con.execute(_load("member_question_focus_shift.sql"))
    result = _result(con, "v_member_question_focus_shift")
    _assert_cols(result, "unique_member_code", "past_top", "recent_top")
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_question_ministries_executes():
    """Reads v_member_questions — load questions first (the 2026-05-31 ordering bug)."""
    _skip_missing(*_src(_QUESTIONS_SRC))
    con = _con()
    con.execute(_load("member_questions.sql"))
    con.execute(_load("member_zz_question_ministries.sql"))
    result = _result(con, "v_member_question_ministries")
    _assert_cols(result, "unique_member_code", "ministry", "n")
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_question_top_topics_executes():
    """Reads v_member_questions — load questions first."""
    _skip_missing(*_src(_QUESTIONS_SRC))
    con = _con()
    con.execute(_load("member_questions.sql"))
    con.execute(_load("member_zz_question_top_topics.sql"))
    result = _result(con, "v_member_question_top_topics")
    _assert_cols(result, "unique_member_code", "topic", "n")
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_debate_sections_executes():
    _skip_missing(*_src(_QUESTIONS_SRC))
    con = _con()
    con.execute(_load("member_debate_sections.sql"))
    result = _result(con, "v_member_debate_sections")
    _assert_cols(
        result,
        "unique_member_code",
        "td_name",
        "debate_section_id",
        "debate_date",
        "chamber",
        "question_count",
        "oireachtas_url",
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_member_constituency_demographics_executes():
    """Electoral Commission 2022 population on 2023 boundaries — population_per_td
    is the per-capita denominator the member-overview civic-context card reads."""
    _skip_missing(*_src("data/gold/parquet/ec_constituency_pop_2022.parquet"))
    con = _con()
    con.execute(_load("member_constituency_demographics.sql"))
    result = _result(con, "v_member_constituency_demographics")
    _assert_cols(result, "constituency_name", "population_2022", "population_per_td", "td_seats", "boundaries_label")
    assert len(result) > 0
