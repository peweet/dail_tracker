"""
SQL view contract tests.

Each test creates a fresh in-memory DuckDB connection, executes the view SQL,
and asserts the output has the expected columns and at least one row.

What these tests catch that silver Pandera tests cannot:
  - Column alias mismatches (silver has full_name, view aliases to member_name)
  - Cast failures at runtime (::DOUBLE, ::DATE on wrong types)
  - Zero-row joins (failed LEFT JOIN produces rows but all-NULL columns)
  - TRY_CAST silent nulls (if TRY_CAST fails for all rows, column is all-NULL)
  - Template path substitution errors ({PARQUET_PATH} unresolved → DuckDB parse error)

Run with:
    pytest test/test_sql_views.py -v -m sql
"""

import os
import re
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
# utility/ APPENDED (not inserted at front) so the project-root config.py still
# wins — utility/ has its own config.py that would shadow it. We only need
# utility/ on the path to import the real production view loader
# (data_access._sql_registry.register_views) for the registration smoke test.
sys.path.append(str(Path(__file__).parents[2] / "utility"))
from data_access._sql_registry import register_views

from config import (
    DATA_DIR,
    GOLD_PARQUET_DIR,
    GOLD_SPEECHES_FACT_PARQUET,
    GOLD_VOTE_HISTORY_PARQUET,
    LOBBY_PARQUET_DIR,
    SILVER_DIR,
    SILVER_PARQUET_DIR,
)

PROJECT_ROOT = Path(__file__).parents[2]
SQL_VIEWS_DIR = PROJECT_ROOT / "sql_views"

# Fixture parquets (committed under test/fixtures/sql_views/) let the view-template
# tests run in CI without needing real pipeline output. Set DAIL_INTEGRATION_TESTS=1
# to point at production paths instead — needed for the tests that don't yet have
# fixtures (lobbying, payments, attendance) and for end-to-end runs locally.
_USE_REAL_PATHS = os.environ.get("DAIL_INTEGRATION_TESTS") == "1"
_FIXTURES_DIR = Path(__file__).parents[1] / "fixtures" / "sql_views"

if _USE_REAL_PATHS:
    MEMBER_PARQUET = SILVER_PARQUET_DIR / "flattened_members.parquet"
    SEANAD_MEMBER_PARQUET = SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"
    VOTE_PARQUET = GOLD_PARQUET_DIR / "pretty_votes.parquet"
    EXTERNAL_LINKS_PARQUET = SILVER_PARQUET_DIR / "member_external_links.parquet"
    CONTACT_DETAILS_PARQUET = SILVER_PARQUET_DIR / "member_contact_details.parquet"
else:
    MEMBER_PARQUET = _FIXTURES_DIR / "silver" / "parquet" / "flattened_members.parquet"
    # The Seanad members parquet shares the Dáil schema, so the committed Dáil
    # fixture doubles as the Seanad source for the registry-union template test.
    SEANAD_MEMBER_PARQUET = MEMBER_PARQUET
    VOTE_PARQUET = _FIXTURES_DIR / "gold" / "parquet" / "pretty_votes.parquet"
    EXTERNAL_LINKS_PARQUET = _FIXTURES_DIR / "silver" / "parquet" / "member_external_links.parquet"
    CONTACT_DETAILS_PARQUET = _FIXTURES_DIR / "silver" / "parquet" / "member_contact_details.parquet"

# Base for resolving views' hardcoded read_parquet('data/...') literals. Many
# views embed 'data/...' paths with no template hook, so _load rewrites them to
# this base (mirroring production's absolutize_data_paths). In integration mode
# that is the real project root; in CI it is the committed fixture data-tree
# (test/fixtures/sql_views/data/...), built by _generate.py. Domains without a
# committed fixture simply have no file there, so their tests skip in CI.
_DATA_BASE = PROJECT_ROOT if _USE_REAL_PATHS else _FIXTURES_DIR

# In CI mode, point the imported data-dir constants at the fixture tree too, so a
# skip guard and _load resolve against the SAME base. Without this, a dev machine
# that has real pipeline output would NOT skip a non-fixtured domain (real file
# present) yet _load would target the absent fixture — a false failure. Domains
# we committed fixtures for run; the rest skip cleanly. (GOLD_VOTE_HISTORY_PARQUET
# is intentionally left real — the registration smoke test loads real data.)
if not _USE_REAL_PATHS:
    _FIX_DATA = _FIXTURES_DIR / "data"
    GOLD_PARQUET_DIR = _FIX_DATA / "gold" / "parquet"
    SILVER_PARQUET_DIR = _FIX_DATA / "silver" / "parquet"
    SILVER_DIR = _FIX_DATA / "silver"
    LOBBY_PARQUET_DIR = _FIX_DATA / "silver" / "lobbying" / "parquet"
    DATA_DIR = _FIX_DATA


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def _con():
    """Fresh in-memory DuckDB connection — no shared state between tests."""
    return duckdb.connect()


def _view_path(filename: str) -> Path:
    """Resolve a bare view filename to its path under sql_views/.

    sql_views/ is organised into per-domain subdirectories but every file keeps
    its unique domain-prefixed name, so a recursive search by bare name finds
    exactly one file — mirroring production's recursive ``glob('**/'+pattern)``.
    """
    matches = sorted(SQL_VIEWS_DIR.glob(f"**/{filename}"))
    if not matches:
        raise FileNotFoundError(f"No SQL view named {filename!r} under {SQL_VIEWS_DIR}")
    return matches[0]


def _load(filename: str, con=None) -> str:
    """Read a SQL view file and substitute known template paths."""
    sql = _view_path(filename).read_text(encoding="utf-8")
    sql = sql.replace("{MEMBER_PARQUET_PATH}", str(MEMBER_PARQUET).replace("\\", "/"))
    sql = sql.replace("{SEANAD_MEMBER_PARQUET_PATH}", str(SEANAD_MEMBER_PARQUET).replace("\\", "/"))
    sql = sql.replace("{PARQUET_PATH}", str(VOTE_PARQUET).replace("\\", "/"))
    # The Seanad vote gold shares the Dáil schema, so the committed Dáil fixture
    # doubles as the Seanad source for v_vote_base's chamber-union template.
    sql = sql.replace("{SEANAD_VOTE_PARQUET_PATH}", str(VOTE_PARQUET).replace("\\", "/"))
    sql = sql.replace("{EXTERNAL_LINKS_PARQUET_PATH}", str(EXTERNAL_LINKS_PARQUET).replace("\\", "/"))
    sql = sql.replace("{CONTACT_DETAILS_PARQUET_PATH}", str(CONTACT_DETAILS_PARQUET).replace("\\", "/"))
    # Historic-members backfill (former-member rosters + member×term sidecar) for
    # v_member_registry_all. Resolve against SILVER_PARQUET_DIR like the others —
    # absent in the CI fixture tree, so member_registry_all-dependent tests skip there.
    sql = sql.replace(
        "{HISTORIC_DAIL_PARQUET_PATH}", str(SILVER_PARQUET_DIR / "historic_members_dail.parquet").replace("\\", "/")
    )
    sql = sql.replace(
        "{HISTORIC_SEANAD_PARQUET_PATH}",
        str(SILVER_PARQUET_DIR / "historic_members_seanad.parquet").replace("\\", "/"),
    )
    sql = sql.replace(
        "{MEMBER_TERMS_PARQUET_PATH}", str(SILVER_PARQUET_DIR / "member_terms.parquet").replace("\\", "/")
    )
    # Rewrite hardcoded read_parquet/read_csv('data/...') literals to an absolute
    # base (mirrors production absolutize_data_paths). CWD-independent, and in CI
    # it points at the committed fixture tree.
    sql = sql.replace("'data/", f"'{_DATA_BASE.as_posix()}/data/")
    return sql


def _skip_missing(*paths):
    """Skip the test if any required data file is absent."""
    for p in paths:
        if not Path(p).exists():
            pytest.skip(f"Required data file not found: {p} — run pipeline.py first")


def _result(con, view_name: str, limit: int = 5):
    return con.execute(f"SELECT * FROM {view_name} LIMIT {limit}").pl()


def _src(*rel_paths: str):
    """Resolve a view's verbatim 'data/...' source literal to an absolute path.

    Views read literals like read_parquet('data/gold/parquet/x.parquet'); skip
    guards need the absolute path. Resolves against the SAME base _load rewrites
    to — real project root in integration mode, the fixture tree in CI — so a
    domain with a committed fixture runs in CI and one without skips. Pass the
    same 'data/...' string the SQL uses."""
    return [_DATA_BASE / p for p in rel_paths]


def _assert_cols(result, *cols):
    for col in cols:
        assert col in result.columns, f"Expected column '{col}' (have: {sorted(result.columns)})"


def _fixture_only():
    """Skip a test whose exact-value assertions are calibrated to the synthetic
    fixture — they don't hold against real pipeline output in integration mode."""
    if _USE_REAL_PATHS:
        pytest.skip("exact-value assertions are calibrated to the synthetic fixture (unset DAIL_INTEGRATION_TESTS)")


# ---------------------------------------------------------------------------
# ATTENDANCE VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_attendance_member_summary_executes():
    _skip_missing(
        SILVER_DIR / "aggregated_td_tables.csv",
        SILVER_DIR / "flattened_members.csv",
    )
    con = _con()
    con.execute(_load("attendance_member_summary.sql"))
    result = _result(con, "v_attendance_member_summary")
    assert "member_name" in result.columns
    assert "attendance_rate" in result.columns
    assert "party_name" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_summary_executes():
    _skip_missing(SILVER_DIR / "aggregated_td_tables.csv")
    con = _con()
    con.execute(_load("attendance_summary.sql"))
    result = _result(con, "v_attendance_summary")
    assert "members_count" in result.columns
    assert "sitting_count" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_member_year_summary_executes():
    _skip_missing(GOLD_PARQUET_DIR / "attendance_by_td_year.parquet")
    con = _con()
    con.execute(_load("attendance_member_year_summary.sql"))
    result = _result(con, "v_attendance_member_year_summary")
    assert "unique_member_code" in result.columns
    assert "year" in result.columns
    assert "attended_count" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_year_rank_executes():
    # v_attendance_year_rank reads v_attendance_member_year_summary —
    # both must be created in the same connection.
    _skip_missing(GOLD_PARQUET_DIR / "attendance_by_td_year.parquet")
    con = _con()
    con.execute(_load("attendance_member_year_summary.sql"))
    con.execute(_load("attendance_year_rank.sql"))
    result = _result(con, "v_attendance_year_rank")
    assert "unique_member_code" in result.columns
    assert "rank_high" in result.columns
    assert len(result) > 0


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
# LOBBYING VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_lobbying_index_executes():
    _skip_missing(
        GOLD_PARQUET_DIR / "most_lobbied_politicians.parquet",
        GOLD_PARQUET_DIR / "politician_policy_exposure.parquet",
        GOLD_PARQUET_DIR / "bilateral_relationships.parquet",
    )
    con = _con()
    con.execute(_load("lobbying_index.sql"))
    result = _result(con, "v_lobbying_index")
    assert "member_name" in result.columns
    assert "return_count" in result.columns
    assert "distinct_policy_areas" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_persistence_executes():
    _skip_missing(LOBBY_PARQUET_DIR / "lobbyist_persistence.parquet")
    con = _con()
    con.execute(_load("lobbying_persistence.sql"))
    result = _result(con, "v_lobbying_persistence")
    assert "lobbyist_name" in result.columns
    assert "first_return_date" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_org_intensity_executes():
    _skip_missing(LOBBY_PARQUET_DIR / "bilateral_relationships.parquet")
    con = _con()
    con.execute(_load("lobbying_org_intensity.sql"))
    result = _result(con, "v_lobbying_org_intensity")
    assert "lobbyist_name" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_org_index_exposes_register_fields():
    """website / profile_url come from the lobbying.ie org register via gold."""
    _skip_missing(
        GOLD_PARQUET_DIR / "top_lobbyist_organisations.parquet",
        GOLD_PARQUET_DIR / "lobbyist_persistence.parquet",
    )
    con = _con()
    con.execute(_load("lobbying_org_index.sql"))
    result = _result(con, "v_lobbying_org_index")
    _assert_cols(result, "lobbyist_name", "website", "profile_url", "main_activities")
    # At least one org must carry a real website — guards against the columns
    # silently reverting to the old hardcoded '' literals.
    populated = con.execute(
        "SELECT COUNT(*) FROM v_lobbying_org_index WHERE website IS NOT NULL AND website <> ''"
    ).fetchone()[0]
    assert populated > 0, "no website populated — gold join may have regressed"


@pytest.mark.sql
def test_v_experimental_org_index_enriched_exposes_website():
    """The org detail panel reads `website` from this view."""
    _skip_missing(
        GOLD_PARQUET_DIR / "top_lobbyist_organisations.parquet",
        GOLD_PARQUET_DIR / "lobbyist_persistence.parquet",
        SILVER_DIR / "charities" / "charity_resolved.parquet",
        SILVER_DIR / "cro" / "companies.parquet",
        SILVER_DIR / "cro" / "financial_statements.parquet",
    )
    con = _con()
    con.execute(_load("lobbying_experimental_org_index_enriched.sql"))
    result = _result(con, "v_experimental_lobbying_org_index_enriched")
    _assert_cols(result, "lobbyist_name", "website", "sector_label", "lobbying_profile_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_clients_executes():
    _skip_missing(SILVER_DIR / "lobbying" / "client_company_returns_detail.csv")
    con = _con()
    con.execute(_load("lobbying_clients.sql"))
    result = _result(con, "v_lobbying_clients")
    assert "client_name" in result.columns
    assert "period_start_date" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_revolving_door_executes():
    _skip_missing(GOLD_PARQUET_DIR / "revolving_door_dpos.parquet")
    con = _con()
    con.execute(_load("lobbying_revolving_door.sql"))
    result = _result(con, "v_lobbying_revolving_door")
    assert "return_count" in result.columns
    assert "chamber_display" in result.columns
    assert len(result) > 0


# ---------------------------------------------------------------------------
# LEGISLATION VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_legislation_index_executes():
    _skip_missing(SILVER_PARQUET_DIR / "sponsors.parquet")
    con = _con()
    con.execute(_load("legislation_index.sql"))
    result = _result(con, "v_legislation_index")
    assert "bill_title" in result.columns
    assert "introduced_date" in result.columns
    assert "stage_number" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_debates_executes():
    _skip_missing(
        SILVER_PARQUET_DIR / "debates.parquet",
        SILVER_PARQUET_DIR / "sponsors.parquet",
    )
    con = _con()
    con.execute(_load("legislation_debates.sql"))
    result = _result(con, "v_legislation_debates")
    assert "debate_date" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_debate_listings_executes():
    _skip_missing(SILVER_PARQUET_DIR / "debate_listings.parquet")
    con = _con()
    con.execute(_load("v_debate_listings.sql"))
    result = _result(con, "v_debate_listings")
    for col in (
        "debate_section_id",
        "debate_date",
        "chamber",
        "debate_type",
        "speaker_count",
        "speech_count",
        "debate_url_web",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_debate_listings"
    assert len(result) > 0


# ---------------------------------------------------------------------------
# STATUTORY INSTRUMENT VIEWS
# ---------------------------------------------------------------------------


_SI_STATE_ENUM = {
    "in_force_as_made",
    "amended",
    "partially_revoked",
    "amended_and_partially_revoked",
    "revoked",
    "other_affected",
}


@pytest.mark.sql
def test_v_statutory_instruments_executes():
    """The SI-as-entity view. Locks the signatory contract the SI detail panel
    reads — si_responsible_actor (printed signing office), si_signatory_name
    (printed signer name), and the tenure-inferred si_minister_name/member_code.
    A schema drift on any of these silently breaks 'who signed the SI'. Also
    locks the legal-state columns LEFT-JOINed from v_si_current_state."""
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
    )
    con = _con()
    # v_statutory_instruments LEFT-JOINs v_si_current_state, so register that
    # view first (production's register_views does this via alphabetical order).
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    result = _result(con, "v_statutory_instruments")
    for col in (
        "si_id",
        "si_signed_date",
        "si_responsible_actor",
        "si_signatory_name",
        "si_minister_name",
        "si_minister_member_code",
        # legal-state columns from the LEFT JOIN
        "current_state",
        "affecting_sis",
        "state_source_url",
        "directory_updated_to",
        "state_confidence",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_statutory_instruments"
    assert len(result) > 0


@pytest.mark.sql
def test_v_statutory_instruments_left_join_no_inflation():
    """The legal-state LEFT JOIN must be one-row-per-SI: the view row count must
    equal the base parquet row count (no fan-out), and SIs absent from the
    directory crawl must keep a NULL current_state ('status not checked'), never
    drop out."""
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
    )
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    base = con.execute(
        f"SELECT count(*) FROM read_parquet('{(_DATA_BASE / 'data/gold/parquet/statutory_instruments.parquet').as_posix()}')"
    ).fetchone()[0]
    view = con.execute("SELECT count(*) FROM v_statutory_instruments").fetchone()[0]
    assert view == base, f"LEFT JOIN inflated rows: base={base} view={view}"


@pytest.mark.sql
def test_v_si_current_state_executes():
    """The SI legal-state view (eISB Legislation Directory). Locks the enum and
    the provenance invariant the detail-panel chip + caveat depend on: every
    revoked / partially_revoked row must carry a confirm link (state_source_url),
    and current_state must stay within the agreed enum."""
    _skip_missing(GOLD_PARQUET_DIR / "si_current_state.parquet")
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    result = _result(con, "v_si_current_state")
    _assert_cols(
        result,
        "si_id",
        "current_state",
        "affecting_sis",
        "this_si_eli_url",
        "how_affected_raw",
        "state_source",
        "state_source_url",
        "directory_updated_to",
        "confidence",
    )
    assert len(result) > 0

    # Enum check across the whole view.
    states = con.execute("SELECT DISTINCT current_state FROM v_si_current_state").fetchall()
    for (s,) in states:
        assert s in _SI_STATE_ENUM, f"current_state '{s}' outside the agreed enum"

    # Provenance invariant: a negative legal state must always be sourced.
    unsourced = con.execute(
        "SELECT count(*) FROM v_si_current_state "
        "WHERE current_state IN ('revoked', 'partially_revoked') AND state_source_url IS NULL"
    ).fetchone()[0]
    assert unsourced == 0, f"{unsourced} revoked/partially_revoked rows missing state_source_url"


@pytest.mark.sql
def test_v_si_current_state_coverage_gate():
    """Join coverage vs gold ≥ 95% (the extractor measured 99.5%). Guards against
    an eISB layout change silently dropping the directory crawl to a stub. Runs
    only against real pipeline output — the CI fixture is a 2/3 stub by design."""
    if not _USE_REAL_PATHS:
        pytest.skip("coverage gate needs real pipeline output (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
    )
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    total, matched = con.execute("SELECT count(*), count(current_state) FROM v_statutory_instruments").fetchone()
    cov = matched / total if total else 0
    assert cov >= 0.95, f"SI legal-state coverage {cov:.1%} < 95% — directory crawl may be broken"


@pytest.mark.sql
def test_v_bill_statutory_instruments_executes():
    """The bill-gated SI view (SIs joined to their enabling Act)."""
    _skip_missing(GOLD_PARQUET_DIR / "bill_statutory_instruments.parquet")
    con = _con()
    con.execute(_load("legislation_statutory_instruments.sql"))
    result = _result(con, "v_bill_statutory_instruments")
    for col in ("bill_id", "si_id", "si_minister", "si_minister_named"):
        assert col in result.columns, f"Expected column '{col}' in v_bill_statutory_instruments"
    assert len(result) > 0


# --- v_si_amendments — the SI→SI amendment/revocation graph (edge inversion) ---

_SI_AMEND_EFFECTS = {"revokes", "amends", "partially revokes", "amends and partially revokes"}


def _write_si_amendments_fixture(root: Path) -> None:
    """Build a minimal si_current_state + statutory_instruments parquet pair under
    root/data/gold/parquet/ that exercises every derivation rule of v_si_amendments."""
    import polars as pl

    pdir = root / "data" / "gold" / "parquet"
    pdir.mkdir(parents=True, exist_ok=True)

    # affected-side rows. Lists are the eISB "affecting" instruments.
    state = pl.DataFrame(
        {
            "si_year": [2020, 2020, 2020, 2020, 2020],
            "si_number": [100, 101, 102, 103, 104],
            "current_state": [
                "revoked",  # -> 1 edge, effect 'revokes'
                "amended",  # -> 1 edge, effect 'amends', provision parsed
                "other_affected",  # EXCLUDED (indirect refs)
                "in_force_as_made",  # EXCLUDED (no affecting edge)
                "partially_revoked",  # -> 1 edge, effect 'partially revokes', amender out-of-gold
            ],
            "this_si_eli_url": ["eli100", "eli101", "eli102", "eli103", "eli104"],
            "how_affected_raw": [
                "Revoked || S.I. No. 200 of 2021 , reg. 5",
                "Reg. 3 amended || S.I. No. 201 of 2022 , reg. 2",
                "Rendered obsolete by revocation of S.I. No. 90 of 2019 || S.I. No. 202 of 2021 , reg. 1",
                None,
                "Reg. 4 revoked || S.I. No. 204 of 2023 , reg. 9",
            ],
            "confidence": [0.90, 0.88, 0.70, 0.95, 0.85],
            "affecting_sis": [
                ["200/2021"],
                ["201/2022"],
                ["90/2019", "202/2021"],  # would inflate if not excluded
                [],
                ["204/2023"],
            ],
            "affecting_si_urls": [
                ["u200"],
                ["u201"],
                ["u90", "u202"],
                [],
                ["u204"],
            ],
        }
    )
    state.write_parquet(pdir / "si_current_state.parquet")

    # titles: include both bases and the in-gold amenders; OMIT 204/2023 so its
    # amender_title must come back NULL (LEFT JOIN, not an inner-join drop).
    sis = pl.DataFrame(
        {
            "si_year": [2020, 2020, 2020, 2020, 2021, 2022],
            "si_number": [100, 101, 102, 104, 200, 201],
            "si_title": [
                "Base A Regs 2020",
                "Base B Regs 2020",
                "Base C Regs 2020",
                "Base E Regs 2020",
                "Revoker Regs 2021",
                "Amender Regs 2022",
            ],
        }
    )
    sis.write_parquet(pdir / "statutory_instruments.parquet")


def test_v_si_amendments_inversion_contract(tmp_path):
    """Precise derivation contract on a synthetic fixture (no real data needed):
    edge inversion, effect mapping, other_affected exclusion, no row inflation
    from multi-element lists, number/year parse, LEFT-JOIN title fill."""
    _write_si_amendments_fixture(tmp_path)
    sql = _view_path("legislation_si_amendments.sql").read_text(encoding="utf-8")
    sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
    con = _con()
    con.execute(sql)
    df = con.execute("SELECT * FROM v_si_amendments ORDER BY affected_number").pl()

    # exactly 3 edges: revoked(100), amended(101), partially_revoked(104).
    # other_affected(102) excluded -> its 2-element list does NOT inflate; 103 has no edge.
    assert df.height == 3, f"expected 3 clean edges, got {df.height}"
    assert set(df["current_state"]) == {"revoked", "amended", "partially_revoked"}
    assert set(df["effect"]).issubset(_SI_AMEND_EFFECTS)
    assert 102 not in set(df["affected_number"]), "other_affected must be excluded"
    assert 103 not in set(df["affected_number"]), "no-edge row must be excluded"

    # effect mapping + number/year parse + provision extraction (the amended row)
    amend = con.execute("SELECT * FROM v_si_amendments WHERE affected_number=101").pl().to_dicts()[0]
    assert amend["effect"] == "amends"
    assert (amend["amender_number"], amend["amender_year"]) == (201, 2022)
    assert amend["amender_title"] == "Amender Regs 2022"
    assert amend["provision_note"] == "Reg. 3 amended"

    # DIR2 inversion: the revoker 200/2021 points at the affected base 100/2020
    rev = con.execute("SELECT * FROM v_si_amendments WHERE amender_number=200 AND amender_year=2021").pl().to_dicts()[0]
    assert rev["effect"] == "revokes"
    assert (rev["affected_number"], rev["affected_year"]) == (100, 2020)

    # LEFT JOIN: amender 204/2023 is absent from gold -> title NULL, row still present
    part = con.execute("SELECT * FROM v_si_amendments WHERE effect='partially revokes'").pl().to_dicts()[0]
    assert (part["amender_number"], part["amender_year"]) == (204, 2023)
    assert part["amender_title"] is None


# --- v_si_lrc_enrichment + v_statutory_instruments_classified (LRC subject) ---

_LRC_STATUS_ENUM = {"matched_classified_list", "not_matched"}
_LRC_FORBIDDEN = {"in_force", "valid", "invalid", "official_status", "legally_current"}


@pytest.mark.sql
def test_v_si_lrc_enrichment_executes():
    """LRC subject-classification view. Locks the column contract the SI subject
    chip + topic facet read, the SAFE-status enum (never 'in force'), and one row
    per SI (no fan-out from the source summary)."""
    _skip_missing(GOLD_PARQUET_DIR / "si_lrc_enrichment_summary.parquet")
    con = _con()
    con.execute(_load("legislation_si_lrc_enrichment.sql"))
    result = _result(con, "v_si_lrc_enrichment")
    for col in (
        "si_year",
        "si_number",
        "has_lrc_classified_list_match",
        "lrc_primary_subject",
        "lrc_primary_leaf",
        "lrc_enrichment_status",
        "lrc_caveat",
        "lrc_list_updated_to",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_si_lrc_enrichment"

    # safe status vocabulary — the dangerous failure is a legal-status assertion
    states = {s for (s,) in con.execute("SELECT DISTINCT lrc_enrichment_status FROM v_si_lrc_enrichment").fetchall()}
    assert states <= _LRC_STATUS_ENUM, f"status outside safe enum: {states - _LRC_STATUS_ENUM}"
    joined = " ".join(states)
    for bad in _LRC_FORBIDDEN:
        assert bad not in joined, f"forbidden legal-status token {bad!r} in lrc_enrichment_status"

    # one row per SI
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (si_year, si_number)) FROM v_si_lrc_enrichment"
    ).fetchone()
    assert n == distinct, f"v_si_lrc_enrichment not one-row-per-SI: {n} rows, {distinct} distinct"
    # unmatched rows must carry no subject (never a fabricated classification)
    bad = con.execute(
        "SELECT count(*) FROM v_si_lrc_enrichment "
        "WHERE lrc_enrichment_status='not_matched' AND lrc_primary_subject IS NOT NULL"
    ).fetchone()[0]
    assert bad == 0, f"{bad} not_matched rows carry a subject"


@pytest.mark.sql
def test_v_statutory_instruments_classified_no_inflation():
    """The page's browse surface = v_statutory_instruments LEFT JOIN the LRC
    enrichment. Must stay one-row-per-SI (no fan-out) and expose the subject
    columns the facet/chip read."""
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
        GOLD_PARQUET_DIR / "si_lrc_enrichment_summary.parquet",
    )
    con = _con()
    # dependency order: current_state -> index (v_statutory_instruments) ->
    # lrc_enrichment -> zz_classified
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    con.execute(_load("legislation_si_lrc_enrichment.sql"))
    con.execute(_load("legislation_si_zz_classified.sql"))
    base = con.execute("SELECT count(*) FROM v_statutory_instruments").fetchone()[0]
    clf = con.execute("SELECT count(*) FROM v_statutory_instruments_classified").fetchone()[0]
    assert clf == base, f"LRC LEFT JOIN inflated rows: base={base} classified={clf}"
    result = _result(con, "v_statutory_instruments_classified")
    for col in ("si_id", "lrc_primary_subject", "lrc_primary_leaf", "lrc_enrichment_status"):
        assert col in result.columns, f"Expected column '{col}' in v_statutory_instruments_classified"


@pytest.mark.sql
def test_v_si_amendments_executes():
    """Real-data execute + contract: column shape, effect enum, other_affected
    excluded, and row count equals the clean-state edge count in the source
    parquet (guards the inversion against silent fan-out or scope drift)."""
    _skip_missing(GOLD_PARQUET_DIR / "si_current_state.parquet", GOLD_PARQUET_DIR / "statutory_instruments.parquet")
    con = _con()
    con.execute(_load("legislation_si_amendments.sql"))
    result = _result(con, "v_si_amendments")
    for col in (
        "amender_number",
        "amender_year",
        "amender_title",
        "amender_eli_url",
        "effect",
        "current_state",
        "provision_note",
        "confidence",
        "affected_number",
        "affected_year",
        "affected_title",
        "affected_eli_url",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_si_amendments"

    effects = con.execute("SELECT DISTINCT effect FROM v_si_amendments").fetchall()
    for (e,) in effects:
        assert e in _SI_AMEND_EFFECTS, f"effect '{e}' outside the agreed set"
    assert con.execute("SELECT count(*) FROM v_si_amendments WHERE current_state='other_affected'").fetchone()[0] == 0

    # row count must equal sum(len(affecting_sis)) over clean states — no inflation
    src = (_DATA_BASE / "data/gold/parquet/si_current_state.parquet").as_posix()
    expected = con.execute(
        f"SELECT coalesce(sum(len(affecting_sis)),0) FROM read_parquet('{src}') "
        "WHERE current_state IN ('revoked','partially_revoked','amended','amended_and_partially_revoked') "
        "AND affecting_sis IS NOT NULL"
    ).fetchone()[0]
    view_n = con.execute("SELECT count(*) FROM v_si_amendments").fetchone()[0]
    assert view_n == expected, f"edge count {view_n} != clean-state affecting count {expected}"


# ---------------------------------------------------------------------------
# PAYMENTS VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_payments_base_executes():
    _skip_missing(GOLD_PARQUET_DIR / "payments_fact.parquet")
    con = _con()
    con.execute(_load("payments_base.sql"))
    result = _result(con, "v_payments_base")
    assert len(result) > 0


# The payments page is a dependency chain rooted at v_payments_base:
#   base → member_detail
#   base → summary
#   base → yearly_evolution → alltime_ranking → alltime_summary
# Each view reads its parent view, not parquet, so the test must CREATE every
# ancestor on the same connection before the leaf. The real source parquet is
# payments_full_psa.parquet (+ the Seanad sibling) — note the existing
# test_v_payments_base skip guard names payments_fact.parquet, which is the
# pre-PSA file; these chain tests guard on the file the view actually reads.
_PAYMENTS_SOURCE = GOLD_PARQUET_DIR / "payments_full_psa.parquet"


def _payments_chain(con, *leaves: str) -> None:
    """CREATE v_payments_base then each named leaf file, in order."""
    con.execute(_load("payments_base.sql"))
    for leaf in leaves:
        con.execute(_load(leaf))


@pytest.mark.sql
def test_v_payments_member_detail_executes():
    """Per-transaction audit trail. Locks the columns the member panel and the
    member-overview payments card read; unique_member_code is the cross-page key."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_member_detail.sql")
    result = _result(con, "v_payments_member_detail")
    for col in (
        "unique_member_code",
        "member_name",
        "date_paid",
        "narrative",
        "amount_num",
        "payment_year",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_payments_member_detail"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_summary_executes():
    """Dataset-level hero row. A single row of dataset totals."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_summary.sql")
    result = _result(con, "v_payments_summary")
    for col in ("members_count", "payment_count", "total_paid", "first_year", "last_year"):
        assert col in result.columns, f"Expected column '{col}' in v_payments_summary"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_yearly_evolution_executes():
    """Per-(member, year) aggregate with the pre-computed window columns the
    contract forbids Streamlit from computing (rank_high, year_total_paid,
    member_alltime_total). A rename here silently empties the yearly cards."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_yearly_evolution.sql")
    result = _result(con, "v_payments_yearly_evolution")
    for col in (
        "member_name",
        "payment_year",
        "total_paid",
        "rank_high",
        "year_total_paid",
        "member_alltime_total",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_payments_yearly_evolution"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_alltime_ranking_executes():
    """All-time (since-2020) ranking. Reads yearly_evolution, so the full chain
    base → yearly_evolution → alltime_ranking must be created in order."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(con, "payments_yearly_evolution.sql", "payments_zz_alltime_ranking.sql")
    result = _result(con, "v_payments_alltime_ranking")
    for col in (
        "member_name",
        "unique_member_code",
        "total_paid_since_2020",
        "rank_high",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_payments_alltime_ranking"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_alltime_summary_executes():
    """Single-row hero totals for the Rankings view; the deepest leaf in the
    chain (base → yearly_evolution → alltime_ranking → alltime_summary)."""
    _skip_missing(_PAYMENTS_SOURCE)
    con = _con()
    _payments_chain(
        con,
        "payments_yearly_evolution.sql",
        "payments_zz_alltime_ranking.sql",
        "payments_zz_alltime_summary.sql",
    )
    result = _result(con, "v_payments_alltime_summary")
    for col in ("total_paid_since_2020", "member_count", "avg_per_td_since_2020"):
        assert col in result.columns, f"Expected column '{col}' in v_payments_alltime_summary"
    assert len(result) > 0


@pytest.mark.sql
def test_v_payments_sources_executes():
    """Source-link stub view. Reads no parquet (SELECT over a literal), so it
    needs no data and no skip — if it stops compiling, the page footer breaks."""
    con = _con()
    con.execute(_load("payments_sources.sql"))
    result = _result(con, "v_payments_sources")
    for col in ("source_url", "source_summary"):
        assert col in result.columns, f"Expected column '{col}' in v_payments_sources"
    assert len(result) > 0


# ---------------------------------------------------------------------------
# CORPORATE VIEWS  (Iris Oifigiúil distress / register notices)
# ---------------------------------------------------------------------------
#
# Privacy contract: personal insolvency (named-individual bankruptcies) is
# excluded upstream by policy. These tests lock the column shape the Corporate
# page reads — a drift that re-exposed a personal-bankruptcy field, or dropped
# the entity columns, must fail loudly. See [[feedback_personal_insolvency_privacy]].


@pytest.mark.sql
def test_v_corporate_notices_executes():
    _skip_missing(GOLD_PARQUET_DIR / "corporate_notices.parquet")
    con = _con()
    con.execute(_load("corporate_corporate_notices.sql"))
    result = _result(con, "v_corporate_notices")
    for col in (
        "notice_ref",
        "issue_date",
        "notice_category",
        "entity_name",
        "display_title",
        "brand_mentions",
        "parent_fund_mentions",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_corporate_notices"
    assert len(result) > 0


@pytest.mark.sql
def test_v_corporate_cbi_notice_match_executes():
    """Per-notice CBI-register badge lookup. Both CBI views read the sandbox
    cross-reference parquet (not v_corporate_notices), so they stand alone."""
    _skip_missing(DATA_DIR / "sandbox" / "parquet" / "cbi_xref_corporate_notices.parquet")
    con = _con()
    con.execute(_load("corporate_cbi_distress.sql"))
    result = _result(con, "v_corporate_cbi_notice_match")
    for col in (
        "notice_ref",
        "entity_name",
        "entity_norm",
        "registers",
        "ref_nos",
        "primary_register",
        "primary_ref_no",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_corporate_cbi_notice_match"
    assert len(result) > 0


@pytest.mark.sql
def test_v_corporate_cbi_repeat_distress_executes():
    """Per-firm repeat-distress aggregate. corporate_cbi_distress.sql creates
    BOTH CBI views; load it once and query the second. The HAVING gate can
    legitimately yield zero rows on a sparse dataset, so this asserts the column
    contract only — it does not require rows."""
    _skip_missing(DATA_DIR / "sandbox" / "parquet" / "cbi_xref_corporate_notices.parquet")
    con = _con()
    con.execute(_load("corporate_cbi_distress.sql"))
    result = _result(con, "v_corporate_cbi_repeat_distress")
    for col in (
        "entity_norm",
        "entity_name",
        "n_notices_total",
        "n_receivership",
        "n_distress",
        "n_routine",
        "primary_register",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_corporate_cbi_repeat_distress"


# ---------------------------------------------------------------------------
# VOTE VIEWS  ({PARQUET_PATH} substituted)
# ---------------------------------------------------------------------------

# NOTE: View names and column assertions match what each SQL file actually
# CREATEs. Some views use the `v_` prefix and some don't — this is an
# inconsistency in production SQL (td_vote_*, party_vote_breakdown vs the
# `v_vote_*` convention) that the old test parametrize didn't account for.
# Aliases (full_name → member_name, party → party_name) are reflected here.
VOTE_VIEWS = [
    ("vote_index.sql", "v_vote_index", ["vote_id", "vote_date", "vote_outcome"]),
    ("vote_member_detail.sql", "v_vote_member_detail", ["member_name", "vote_type"]),
    ("vote_party_breakdown.sql", "party_vote_breakdown", ["party_name", "vote_type", "member_count"]),
    ("vote_result_summary.sql", "v_vote_result_summary", ["division_count", "member_count"]),
    ("vote_sources.sql", "v_vote_sources", ["vote_id", "source_url"]),
    ("vote_td_summary.sql", "td_vote_summary", ["member_name", "yes_count"]),
    ("vote_td_year_summary.sql", "td_vote_year_summary", ["member_name", "year"]),
]


@pytest.mark.parametrize("filename,view_name,key_cols", VOTE_VIEWS)
def test_vote_view_executes(filename, view_name, key_cols):
    """Runs against test/fixtures/sql_views/gold/parquet/pretty_votes.parquet
    by default; set DAIL_INTEGRATION_TESTS=1 to run against real pipeline output.
    """
    _skip_missing(VOTE_PARQUET)
    con = _con()
    # All vote views read FROM v_vote_base (the chamber-union chokepoint) — it
    # must be created on the connection first.
    con.execute(_load("vote_base.sql"))
    con.execute(_load(filename))
    result = _result(con, view_name)
    for col in key_cols:
        assert col in result.columns, f"Expected column '{col}' in {view_name}"
    assert len(result) > 0


# ---------------------------------------------------------------------------
# REGISTRATION SMOKE TEST  (mirrors production view loading)
# ---------------------------------------------------------------------------
#
# Every Streamlit data-access module builds its DuckDB connection by calling
# register_views(conn, [glob...], swallow_errors=...). Five of those groups pass
# swallow_errors=True, which means a view that fails to PARSE or BIND is logged
# and silently skipped — the page just renders empty instead of erroring. No
# bespoke per-view test exists for the ~53 views without one, so a column rename
# or cast break in any of them ships unnoticed.
#
# This test re-runs each production glob-group through the *real* register_views
# loader with swallow_errors=False, so any SQL failure surfaces. It is the
# closest thing to "does the app's connection actually build". One test catches
# all ~78 views, including every untested one, in dependency (alphabetical) order
# on one connection per group — exactly how production loads them.
#
# Data gating: a view whose source parquet/CSV is absent raises duckdb.IOException
# ("No files found" / "Could not open"). That is a missing-DATA condition, not a
# contract break, so the group is skipped. Run with DAIL_INTEGRATION_TESTS=1
# against real pipeline output for full coverage; without data each group skips.
# (The member_overview connection uses a bespoke ordered file list rather than a
# glob group and is exercised separately — it is not covered here.)

# (group_id, glob patterns, substitutions) — one tuple per production
# register_views call site across utility/data_access/*_data.py.
_REGISTRATION_GROUPS = [
    ("appointments", ["appointments_*.sql"], {}),
    ("attendance", ["attendance_*.sql"], {}),
    ("charity", ["charity_*.sql"], {}),  # api_conn glob; only one charity file is loaded via lobbying
    ("committees", ["committees_*.sql"], {}),
    # committee_evidence: meeting-history view loaded by get_committee_evidence_conn
    # (swallow_errors=True so a missing gold layer renders an empty timeline, not an
    # error) — register it loud here to catch schema/cast drift.
    ("committee_evidence", ["committee_evidence_*.sql"], {}),
    ("corporate", ["corporate_*.sql"], {}),
    ("interests", ["member_interests_*.sql", "member_zz_interests_*.sql"], {}),
    ("judiciary", ["judiciary_*.sql"], {}),  # judiciary_data.py glob (also covered by test_judiciary_bench)
    ("legislation", ["legislation_*.sql"], {}),
    ("lobbying", ["lobbying_*.sql"], {}),
    ("payments", ["payments_*.sql"], {}),
    ("procurement", ["procurement_*.sql"], {}),
    # public_payments: the real public_payments_data.py call site loads only this one
    # self-contained file (it is also swept into the procurement_*.sql glob above).
    ("public_payments", ["procurement_public_payments.sql"], {}),
    # publicfinance: v_gov_finance_annual is intentionally unwired (no page yet — the
    # share-of-total denominator view is deferred), so no production connection loads
    # it. Register it here so the orphan view is still proven to build (schema drift).
    ("publicfinance", ["publicfinance_*.sql"], {}),
    ("sipo", ["sipo_*.sql"], {}),  # sipo_{donations,expenses}_data.py glob (also covered by test_core_sipo_queries)
    # speech: brand-new debates views, loaded by register_member_views with
    # swallow_errors=True (a break renders the member Debates section silently
    # empty). speech_base must precede its dependents — alphabetical order holds.
    ("speech", ["speech_*.sql"], {"{SPEECH_FACT_PARQUET_PATH}": GOLD_SPEECHES_FACT_PARQUET.as_posix()}),
    (
        "votes",
        ["vote*.sql"],
        {
            "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
            "{SEANAD_VOTE_PARQUET_PATH}": (
                GOLD_VOTE_HISTORY_PARQUET.parent / "current_seanad_vote_history.parquet"
            ).as_posix(),
        },
    ),
]


@pytest.mark.sql
@pytest.mark.parametrize("group_id,patterns,subs", _REGISTRATION_GROUPS, ids=[g[0] for g in _REGISTRATION_GROUPS])
def test_view_group_registers(group_id, patterns, subs):
    """Load a whole production glob-group through the real register_views loader,
    failing loud (swallow_errors=False). Skips if the group's source data is
    absent. A parse/bind/cast failure in ANY view in the group fails the test."""
    con = _con()
    try:
        register_views(con, patterns, substitutions=subs, swallow_errors=False)
    except duckdb.IOException as exc:
        pytest.skip(f"[{group_id}] source data not present: {exc}")
    except Exception as exc:  # noqa: BLE001 — surface the offending group + error
        pytest.fail(f"[{group_id}] view registration failed: {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# COMMITTEES VIEWS
# ---------------------------------------------------------------------------
#
# committees_data registers with swallow_errors=False, so a break fails the app
# loudly — but no test pinned the columns. v_committee_member_detail and
# v_committee_party_seats both read v_committee_assignments, so that file must be
# created first on the same connection.


@pytest.mark.sql
def test_v_committee_assignments_executes():
    _skip_missing(*_src("data/silver/committees/committee_assignments.parquet"))
    con = _con()
    con.execute(_load("committees_assignments.sql"))
    result = _result(con, "v_committee_assignments")
    _assert_cols(result, "chamber", "name", "party", "committee", "role", "is_chair", "start", "end")
    assert len(result) > 0


@pytest.mark.sql
def test_v_committee_office_holders_executes():
    _skip_missing(*_src("data/silver/committees/office_holders.parquet"))
    con = _con()
    con.execute(_load("committees_offices.sql"))
    result = _result(con, "v_committee_office_holders")
    _assert_cols(result, "chamber", "name", "party", "office", "start", "end")
    assert len(result) > 0


@pytest.mark.sql
def test_v_committee_member_detail_executes():
    """Reads v_committee_assignments — load assignments first. party_seats_json
    is the column the composition stacked-bar card parses."""
    _skip_missing(*_src("data/silver/committees/committee_assignments.parquet"))
    con = _con()
    con.execute(_load("committees_assignments.sql"))
    con.execute(_load("committees_zz_member_detail.sql"))
    result = _result(con, "v_committee_member_detail")
    _assert_cols(result, "chamber", "committee", "members", "parties", "chair_name", "party_seats_json")
    assert len(result) > 0


@pytest.mark.sql
def test_v_committee_party_seats_executes():
    _skip_missing(*_src("data/silver/committees/committee_assignments.parquet"))
    con = _con()
    con.execute(_load("committees_assignments.sql"))
    con.execute(_load("committees_zz_party_seats.sql"))
    result = _result(con, "v_committee_party_seats")
    _assert_cols(result, "chamber", "committee", "party", "seats")
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


# ---------------------------------------------------------------------------
# LOBBYING VIEWS (backfill — only 5 of ~21 were previously tested)
# ---------------------------------------------------------------------------

_LOB_SILVER = "data/silver/lobbying/parquet"


@pytest.mark.sql
def test_v_lobbying_summary_executes():
    _skip_missing(*_src("data/gold/parquet/policy_area_breakdown.parquet"))
    con = _con()
    con.execute(_load("lobbying_summary.sql"))
    result = _result(con, "v_lobbying_summary")
    _assert_cols(result, "total_returns", "total_orgs", "total_politicians", "first_period", "last_period")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_org_index_executes():
    _skip_missing(*_src("data/gold/parquet/top_lobbyist_organisations.parquet"))
    con = _con()
    con.execute(_load("lobbying_org_index.sql"))
    result = _result(con, "v_lobbying_org_index")
    _assert_cols(result, "lobbyist_name", "return_count", "politicians_targeted", "distinct_policy_areas")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_clients_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_clients.sql"))
    result = _result(con, "v_lobbying_dpo_clients")
    _assert_cols(result, "individual_name", "client_name", "return_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_firms_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_firms.sql"))
    result = _result(con, "v_lobbying_dpo_firms")
    _assert_cols(result, "individual_name", "lobbyist_name", "return_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_politicians_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_politicians.sql"))
    result = _result(con, "v_lobbying_dpo_politicians")
    _assert_cols(result, "individual_name", "member_name", "chamber", "return_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_returns_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_returns.sql"))
    result = _result(con, "v_lobbying_dpo_returns")
    _assert_cols(
        result,
        "individual_name",
        "return_id",
        "lobbyist_name",
        "client_name",
        "public_policy_area",
        "period_start_date",
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_contact_detail_executes():
    """The per-contact return view; unique_member_code is the cross-page key the
    member-overview lobbying card joins on."""
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_contact_detail.sql"))
    result = _result(con, "v_lobbying_contact_detail")
    _assert_cols(
        result,
        "return_id",
        "member_name",
        "unique_member_code",
        "chamber",
        "lobbyist_name",
        "public_policy_area",
        "period_start_date",
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_contact_detail_with_dpo_executes():
    """Reads v_lobbying_dpo_returns + v_lobbying_contact_detail — both first."""
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_returns.sql"))
    con.execute(_load("lobbying_contact_detail.sql"))
    con.execute(_load("lobbying_zz_contact_detail_with_dpo.sql"))
    result = _result(con, "v_lobbying_contact_detail_with_dpo")
    _assert_cols(result, "return_id", "member_name", "dpo_individuals", "dpo_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_politician_returns_executes():
    """Reads v_lobbying_dpo_returns + v_lobbying_contact_detail — both first."""
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_returns.sql"))
    con.execute(_load("lobbying_contact_detail.sql"))
    con.execute(_load("lobbying_zz_dpo_politician_returns.sql"))
    result = _result(con, "v_lobbying_dpo_politician_returns")
    _assert_cols(result, "individual_name", "member_name", "unique_member_code", "return_id", "lobbyist_name")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_policy_area_summary_executes():
    _skip_missing(*_src("data/gold/parquet/policy_area_breakdown.parquet"))
    con = _con()
    con.execute(_load("lobbying_policy_area_summary.sql"))
    result = _result(con, "v_lobbying_policy_area_summary")
    _assert_cols(result, "public_policy_area", "return_count", "distinct_orgs", "distinct_politicians")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_policy_exposure_executes():
    _skip_missing(*_src("data/gold/parquet/politician_policy_exposure.parquet"))
    con = _con()
    con.execute(_load("lobbying_policy_exposure.sql"))
    result = _result(con, "v_lobbying_policy_exposure")
    _assert_cols(
        result, "member_name", "unique_member_code", "public_policy_area", "returns_targeting", "distinct_lobbyists"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_recent_returns_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/returns_master.parquet"))
    con = _con()
    con.execute(_load("lobbying_recent_returns.sql"))
    result = _result(con, "v_lobbying_recent_returns")
    _assert_cols(result, "period_start_date", "lobbyist_name", "member_name", "public_policy_area", "source_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_return_documents_executes():
    _skip_missing(*_src("data/silver/parquet/lobbying_return_documents.parquet"))
    con = _con()
    con.execute(_load("lobbying_return_documents.sql"))
    result = _result(con, "v_lobbying_return_documents")
    _assert_cols(result, "return_id", "lobbyist_name", "pdf_url", "public_policy_area")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_sources_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_sources.sql"))
    result = _result(con, "v_lobbying_sources")
    _assert_cols(result, "return_id", "member_name", "lobbyist_name", "source_url", "official_pdf_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_topic_search_executes():
    """searchable_text is the concatenated column the topic-search box scans."""
    _skip_missing(*_src(f"{_LOB_SILVER}/returns_master.parquet"))
    con = _con()
    con.execute(_load("lobbying_topic_search.sql"))
    result = _result(con, "v_lobbying_topic_search")
    _assert_cols(result, "return_id", "lobbyist_name", "public_policy_area", "searchable_text")
    assert len(result) > 0


@pytest.mark.sql
def test_v_experimental_lobbying_org_index_enriched_executes():
    """Experimental CRO/charity-enriched org index — depends on CRO + charities
    silver tables that may not be present even in a full local build."""
    _skip_missing(
        *_src(
            "data/gold/parquet/top_lobbyist_organisations.parquet",
            "data/silver/cro/companies.parquet",
            "data/silver/charities/charity_resolved.parquet",
        )
    )
    con = _con()
    con.execute(_load("lobbying_experimental_org_index_enriched.sql"))
    result = _result(con, "v_experimental_lobbying_org_index_enriched")
    _assert_cols(result, "lobbyist_name", "return_count", "sector_label", "funding_profile", "match_method", "flags")
    assert len(result) > 0


# ---------------------------------------------------------------------------
# ATTENDANCE VIEWS (gap backfill)
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_attendance_timeline_executes():
    _skip_missing(*_src("data/silver/aggregated_td_tables.csv", "data/silver/flattened_members.csv"))
    con = _con()
    con.execute(_load("attendance_timeline.sql"))
    result = _result(con, "v_attendance_timeline")
    _assert_cols(result, "sitting_date", "member_name", "present_flag", "attendance_status", "party_name", "house")
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_missing_members_executes():
    _skip_missing(*_src("data/silver/flattened_members.csv", "data/gold/parquet/attendance_by_td_year.parquet"))
    con = _con()
    con.execute(_load("attendance_missing_members.sql"))
    result = _result(con, "v_attendance_missing_members")
    _assert_cols(result, "member_name", "party_name", "missing_reason")
    # May legitimately be empty if every elected member appears in attendance —
    # this is a coverage gap detector, so 0 rows is a valid (good) outcome.


@pytest.mark.sql
def test_v_attendance_chamber_sitting_days_executes():
    _skip_missing(*_src("data/silver/aggregated_td_tables.csv"))
    con = _con()
    con.execute(_load("attendance_chamber_sitting_days.sql"))
    result = _result(con, "v_attendance_chamber_sitting_days")
    _assert_cols(result, "house", "year", "sitting_days")
    assert len(result) > 0


# ---------------------------------------------------------------------------
# LEGISLATION VIEWS (gap backfill)
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_legislation_detail_executes():
    _skip_missing(*_src("data/silver/parquet/sponsors.parquet"))
    con = _con()
    con.execute(_load("legislation_detail.sql"))
    result = _result(con, "v_legislation_detail")
    _assert_cols(
        result, "bill_id", "bill_title", "bill_status", "sponsor", "introduced_date", "current_stage", "oireachtas_url"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_pdfs_executes():
    _skip_missing(*_src("data/silver/parquet/versions.parquet"))
    con = _con()
    con.execute(_load("legislation_pdfs.sql"))
    result = _result(con, "v_legislation_pdfs")
    _assert_cols(result, "bill_id", "pdf_category", "pdf_label", "pdf_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_sources_executes():
    _skip_missing(*_src("data/silver/parquet/sponsors.parquet"))
    con = _con()
    con.execute(_load("legislation_sources.sql"))
    result = _result(con, "v_legislation_sources")
    _assert_cols(result, "bill_id", "oireachtas_url", "source_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_timeline_executes():
    _skip_missing(*_src("data/silver/parquet/stages.parquet"))
    con = _con()
    con.execute(_load("legislation_timeline.sql"))
    result = _result(con, "v_legislation_timeline")
    _assert_cols(result, "bill_id", "stage_name", "stage_date", "stage_number", "chamber")
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_pre2014_acts_executes():
    """Curated pre-2014 Acts crosswalk (data/_meta/pre2014_acts.csv). The meta CSV
    is hand-maintained and may be absent on a fresh checkout."""
    _skip_missing(*_src("data/_meta/pre2014_acts.csv"))
    con = _con()
    con.execute(_load("legislation_pre2014_acts.sql"))
    result = _result(con, "v_legislation_pre2014_acts")
    _assert_cols(result, "canonical_bill_id", "act_short_title", "act_year", "policy_domain")
    assert len(result) > 0


@pytest.mark.sql
def test_v_bill_si_operation_mix_executes():
    """Reads v_bill_statutory_instruments — load that first."""
    _skip_missing(*_src("data/gold/parquet/bill_statutory_instruments.parquet"))
    con = _con()
    con.execute(_load("legislation_statutory_instruments.sql"))
    con.execute(_load("legislation_zz_bill_si_operation_mix.sql"))
    result = _result(con, "v_bill_si_operation_mix")
    _assert_cols(result, "bill_id", "si_operation", "n")
    assert len(result) > 0


# ---------------------------------------------------------------------------
# APPOINTMENTS VIEW
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_public_appointments_executes():
    _skip_missing(*_src("data/gold/parquet/public_appointments.parquet"))
    con = _con()
    con.execute(_load("appointments_public_appointments.sql"))
    result = _result(con, "v_public_appointments")
    _assert_cols(
        result, "notice_ref", "issue_date", "appointing_authority", "body", "appointee", "role", "english_summary"
    )
    assert len(result) > 0


# ---------------------------------------------------------------------------
# PROCUREMENT VIEWS
# ---------------------------------------------------------------------------
#
# These assert the VALUE-IS-NOT-SPEND semantics, not just that the views run.
# The fixture (test/fixtures/sql_views/_generate.py) plants rows whose aggregates
# are known exactly, so a regression in the value_safe_to_sum filter, the privacy/
# truncation exclusions, the CRO join, or the lobbying-overlap dedup fails loudly.

_PROC_AWARDS = "data/gold/parquet/procurement_awards.parquet"
_PROC_CRO = "data/gold/parquet/procurement_supplier_cro_match.parquet"
_PROC_OVERLAP = "data/gold/parquet/procurement_lobbying_overlap.parquet"


@pytest.mark.sql
def test_v_procurement_awards_executes():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS))
    con = _con()
    con.execute(_load("procurement_awards.sql"))
    df = con.execute("SELECT * FROM v_procurement_awards").pl()
    _assert_cols(
        df,
        "tender_id",
        "supplier",
        "supplier_norm",
        "supplier_class",
        "name_truncated",
        "contracting_authority",
        "cpv_code",
        "cpv_description",
        "award_date",
        "value_eur",
        "value_kind",
        "is_framework_or_dps",
        "value_shared_across_suppliers",
        "value_safe_to_sum",
        "is_call_off",
        "parent_agreement_id",
        # 2026-06-12 detail widening: title, classification fallback, competition detail,
        # pre-award estimate and the EU Official Journal deep links.
        "tender_title",
        "spend_category",
        "category_label",
        "contract_type",
        "procedure_type",
        "contract_duration_months",
        "n_bids_received",
        "n_sme_bids_received",
        "n_awarded_smes",
        "estimated_value_eur",
        "additional_cpv_codes",
        "ted_notice_link",
        "ted_can_link",
    )
    assert len(df) == 10  # raw passthrough — every award×supplier row, nothing filtered

    by_supplier = {r["supplier"]: r for r in df.to_dicts()}

    # DD/MM/YYYY parsed to a real DATE (TRY_STRPTIME)
    from datetime import date as _date

    assert by_supplier["Mason & Sons Ltd"]["award_date"] == _date(2023, 4, 4)

    # Detail fields: source strings TRY_CAST to honest ints; title/links/estimate carried.
    t001 = next(r for r in df.to_dicts() if r["tender_id"] == "T001")
    assert t001["tender_title"] == "N4 Road Improvement Works – Phase 2"
    assert t001["procedure_type"] == "Open Procedure"
    assert t001["contract_duration_months"] == 24
    assert t001["n_bids_received"] == 5
    assert t001["n_sme_bids_received"] == 3
    assert t001["n_awarded_smes"] == 1
    assert t001["estimated_value_eur"] == 120000.0
    assert t001["ted_can_link"].startswith("https://ted.europa.eu/")
    # category_label: CPV description wins when present…
    assert t001["category_label"] == "Construction work"
    # …and falls back to the OGP Spend Category when the row has no CPV (~70% of corpus).
    nullid = by_supplier["Nullid Co Ltd"]
    assert nullid["cpv_code"] is None
    assert nullid["category_label"] == "Information and Communication Technology"

    # Entity-split fix CONTRACT: a name with '&' survives whole — never fragmented
    # into "Mason" + "Sons Ltd". Guards against an ETL regression reaching gold.
    assert "Mason & Sons Ltd" in by_supplier
    assert "&" in by_supplier["Mason & Sons Ltd"]["supplier"]
    assert not any(r["supplier"] in {"Sons Ltd", "Company", "Co. Limited"} for r in df.to_dicts())

    # Tender ID literal "NULL" is now an honest null (2026-06-03 fix)
    assert by_supplier["Nullid Co Ltd"]["tender_id"] is None

    # A framework/DPS row is carried but flagged unsummable
    bigco = by_supplier["Bigco Services Ltd"]
    assert bigco["is_framework_or_dps"] is True
    assert bigco["value_safe_to_sum"] is False


@pytest.mark.sql
def test_v_procurement_supplier_summary_value_semantics():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS, _PROC_CRO, _PROC_OVERLAP))
    con = _con()
    con.execute(_load("procurement_supplier_summary.sql"))
    df = con.execute("SELECT * FROM v_procurement_supplier_summary").pl()
    _assert_cols(
        df,
        "supplier",
        "supplier_norm",
        "n_awards",
        "n_authorities",
        "awarded_value_safe_eur",
        "company_num",
        "company_status",
        "cro_match_method",
        "on_lobbying_register",
        "lobbying_returns",
        "is_lobbying_registrant",
        "is_lobbying_client",
        "has_epa_licence",
    )
    by = {r["supplier_norm"]: r for r in df.to_dicts()}

    # Privacy + quality exclusions: sole-trader and name_truncated never rank.
    assert "joemurphy" not in by, "sole trader leaked into supplier ranking"
    assert "eloittetruncnorm" not in by, "name_truncated supplier leaked into ranking"
    assert len(df) == 7

    # Clean multi-award supplier sums only its safe rows.
    acme = by["acmeconstructionltd"]
    assert acme["n_awards"] == 2
    assert acme["n_authorities"] == 2
    assert acme["awarded_value_safe_eur"] == 300000.0
    assert acme["company_num"] == 123456
    assert acme["company_status"] == "Normal"
    assert acme["on_lobbying_register"] is False
    # EPA flag (PR 4): the EPA fixture licenses ONLY company_num 123456 (acme), with
    # a second row at n_licences=0 that must NOT count. So exactly acme is flagged.
    assert acme["has_epa_licence"] is True, "CRO-matched EPA-licensed company must be flagged"
    assert sum(1 for r in df.to_dicts() if r["has_epa_licence"]) == 1, "only the n_licences>0 company is flagged"

    # KEY: a framework CEILING is counted but contributes ZERO to the value sum.
    bigco = by["bigcoservicesltd"]
    assert bigco["n_awards"] == 1
    assert bigco["awarded_value_safe_eur"] == 0.0

    # KEY: a value shared across co-suppliers on one tender is NOT summed.
    assert by["sharedcoaltd"]["awarded_value_safe_eur"] == 0.0
    # KEY: a NULL-Tender-ID row can't be verified unshared → not summed.
    assert by["nullidcoltd"]["awarded_value_safe_eur"] == 0.0

    # Lobbying overlap folded in: variant rows (registrant+client) aggregate per
    # supplier_norm — returns SUM to 8, both side flags true, value still 400k.
    lob = by["lobbycoltd"]
    assert lob["awarded_value_safe_eur"] == 400000.0
    assert lob["on_lobbying_register"] is True
    assert lob["lobbying_returns"] == 8  # 5 (registrant) + 3 (client)
    assert lob["is_lobbying_registrant"] is True
    assert lob["is_lobbying_client"] is True

    # Ordered by n_awards DESC → Acme (2 awards) leads.
    assert df["n_awards"].to_list()[0] == 2


@pytest.mark.sql
def test_v_procurement_authority_summary_value_semantics():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS))
    con = _con()
    con.execute(_load("procurement_authority_summary.sql"))
    df = con.execute("SELECT * FROM v_procurement_authority_summary").pl()
    _assert_cols(df, "contracting_authority", "n_awards", "n_suppliers", "awarded_value_safe_eur")
    by = {r["contracting_authority"]: r for r in df.to_dicts()}
    assert len(df) == 8

    # Two safe awards (Acme 100k + Lobbyco 400k) to two distinct suppliers.
    dcc = by["Dublin City Council"]
    assert dcc["n_awards"] == 2
    assert dcc["n_suppliers"] == 2
    assert dcc["awarded_value_safe_eur"] == 500000.0

    # Framework-only and shared-value-only authorities sum to ZERO.
    assert by["Health Service Executive"]["awarded_value_safe_eur"] == 0.0
    assert by["Office of Public Works (OPW)"]["awarded_value_safe_eur"] == 0.0


@pytest.mark.sql
def test_v_procurement_cpv_summary_value_semantics():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS))
    con = _con()
    con.execute(_load("procurement_cpv_summary.sql"))
    df = con.execute("SELECT * FROM v_procurement_cpv_summary").pl()
    _assert_cols(df, "cpv_code", "cpv_description", "n_awards", "n_suppliers", "awarded_value_safe_eur")
    by = {r["cpv_code"]: r for r in df.to_dicts()}
    # 4 real CPV groups; the CPV-less Nullid row (spend-category-only, like ~70% of the
    # corpus) must NOT grow a null/"NULL" bucket here.
    assert len(df) == 4
    assert None not in by

    construction = by["45000000"]
    assert construction["cpv_description"] == "Construction work"
    assert construction["n_awards"] == 3  # Acme×2 + Mason
    assert construction["awarded_value_safe_eur"] == 450000.0  # 100k + 200k + 150k

    # Business services: Bigco framework (5m) excluded; only eloitte 75k + Lobbyco 400k.
    assert by["79000000"]["awarded_value_safe_eur"] == 475000.0


@pytest.mark.sql
def test_v_procurement_lobbying_overlap_executes():
    _fixture_only()
    _skip_missing(*_src(_PROC_OVERLAP))
    con = _con()
    con.execute(_load("procurement_lobbying_overlap.sql"))
    df = con.execute("SELECT * FROM v_procurement_lobbying_overlap").pl()
    _assert_cols(
        df,
        "lobby_name",
        "lobby_side",
        "supplier",
        "supplier_norm",
        "n_lobby_returns",
        "n_award_rows",
        "n_authorities",
        "awarded_value_safe_eur",
    )
    # Passthrough: one row per matched lobbying entity (registrant + client variant).
    assert len(df) == 2
    assert set(df["supplier"].to_list()) == {"Lobbyco Ltd"}
    # Anomaly #3 is INTENTIONAL in this two-keyed table: a naive row-sum
    # double-counts the same supplier's awarded value. Lock that so a consumer
    # never SUM()s this column without deduping by supplier first.
    assert df["awarded_value_safe_eur"].sum() == 800000.0  # 2 × 400k — NOT the true 400k


@pytest.mark.sql
def test_v_lobbying_org_procurement_dedups_to_registrant():
    _fixture_only()
    _skip_missing(*_src(_PROC_OVERLAP))
    con = _con()
    con.execute(_load("lobbying_org_procurement.sql"))
    df = con.execute("SELECT * FROM v_lobbying_org_procurement").pl()
    _assert_cols(df, "lobbyist_name", "supplier", "n_awards", "n_authorities", "awarded_value_safe_eur")
    # Registrant-side only (the client variant is filtered out), grouped per name.
    assert len(df) == 1
    row = df.to_dicts()[0]
    assert row["lobbyist_name"] == "Lobbyco Limited"
    assert row["n_awards"] == 1
    assert row["n_authorities"] == 1
    assert row["awarded_value_safe_eur"] == 400000.0


@pytest.mark.sql
def test_v_procurement_charity_overlap_grain_and_value_firewall():
    """Charity ↔ procurement co-occurrence, linked by a HARD CRO company number
    (charity_resolved.cro_number == supplier_cro_match.company_num). Locks the
    column contract the linkage surface reads, the one-row-per-(rcn, supplier_norm)
    grain (no fan-out), and the money-grain firewall: awarded_value_safe_eur is
    never negative and never sums more rows than the summable-award subset, so a
    framework/DPS ceiling can never inflate a charity's apparent award value.

    Integration-data only — this cross-domain join has no synthetic fixture (the
    registration smoke test already proves it parses/binds in the CI fixture run)."""
    if not _USE_REAL_PATHS:
        pytest.skip("charity×procurement overlap has no fixture (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(
        *_src(_PROC_AWARDS, _PROC_CRO),
        SILVER_DIR / "charities" / "charity_resolved.parquet",
    )
    con = _con()
    con.execute(_load("procurement_charity_overlap.sql"))
    df = con.execute("SELECT * FROM v_procurement_charity_overlap").pl()
    _assert_cols(
        df,
        "rcn",
        "registered_charity_name",
        "company_num",
        "supplier_norm",
        "matched_supplier_name",
        "n_awards",
        "n_authorities",
        "awarded_value_safe_eur",
        "n_value_safe_awards",
        "n_ceiling_notices",
        "gov_funded_share_latest",
        "state_adjacent_flag",
    )

    # Grain: strictly one row per (rcn, supplier_norm) — a name-variant fan-out
    # would silently double-count a charity's award footprint.
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (rcn, supplier_norm)) FROM v_procurement_charity_overlap"
    ).fetchone()
    assert n == distinct, f"not one-row-per-(rcn, supplier_norm): {n} rows, {distinct} distinct"

    # Money-grain firewall: safe value never negative; the summable-award count
    # never exceeds the total award count (a ceiling notice can't be summed).
    bad = con.execute(
        "SELECT count(*) FROM v_procurement_charity_overlap "
        "WHERE awarded_value_safe_eur < 0 OR n_value_safe_awards > n_awards"
    ).fetchone()[0]
    assert bad == 0, "value firewall violated (negative safe value or safe>total awards)"

    # The link is a hard CRO identifier — company_num must always be present.
    assert df["company_num"].null_count() == 0


_TED_AWARDS_SILVER = "data/silver/parquet/ted_ie_awards.parquet"
_TED_TENDERS_SILVER = "data/silver/parquet/ted_ie_tenders.parquet"
_TED_WINNER_HISTORY_SILVER = "data/silver/parquet/ted_ie_winner_history.parquet"


@pytest.mark.sql
def test_v_procurement_ted_awards_competition_columns():
    """The TED award view must expose the eForms competition-intensity columns, and they must
    be internally consistent: is_single_bid is exactly (n_tenders_received == 1), tender counts
    are never < 1, and the flags stay boolean/null. Integration-data only (silver is gitignored,
    eForms-only so populated from ~2024)."""
    if not _USE_REAL_PATHS:
        pytest.skip("TED silver is gitignored and unfixtured (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_TED_AWARDS_SILVER))
    con = _con()
    con.execute(_load("procurement_ted_awards.sql"))
    df = con.execute("SELECT * FROM v_procurement_ted_awards LIMIT 5").pl()
    _assert_cols(
        df,
        "procedure_type",
        "is_uncompetitive_procedure",
        "n_tenders_received",
        "is_single_bid",
        "award_criteria_kind",
        "is_price_only",
    )
    # no nonsensical tender counts
    bad = con.execute(
        "SELECT count(*) FROM v_procurement_ted_awards WHERE n_tenders_received IS NOT NULL AND n_tenders_received < 1"
    ).fetchone()[0]
    assert bad == 0, "tenders-received below 1 — taxonomy/aggregation bug"
    # single-bid is exactly (min tenders == 1), wherever a count exists
    inconsistent = con.execute(
        "SELECT count(*) FROM v_procurement_ted_awards WHERE n_tenders_received IS NOT NULL "
        "AND is_single_bid <> (n_tenders_received = 1)"
    ).fetchone()[0]
    assert inconsistent == 0, "is_single_bid does not match (n_tenders_received = 1)"


@pytest.mark.sql
def test_v_procurement_ted_winner_history_union():
    """The full winner-history view UNIONs the 2024+ API lane and the 2016-2023 per-notice-XML
    backfill into one (notice x winner) feed. Both silvers gitignored → integration-data only."""
    if not _USE_REAL_PATHS:
        pytest.skip("TED silver is gitignored and unfixtured (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_TED_AWARDS_SILVER), *_src(_TED_WINNER_HISTORY_SILVER))
    con = _con()
    con.execute(_load("procurement_ted_awards_history.sql"))
    df = con.execute("SELECT * FROM v_procurement_ted_winner_history LIMIT 5").pl()
    _assert_cols(df, "source_lane", "winner_join_norm", "winner_name", "value_safe_to_sum", "procedure_type")
    # both ingestion lanes are present
    lanes = {r[0] for r in con.execute("SELECT DISTINCT source_lane FROM v_procurement_ted_winner_history").fetchall()}
    assert lanes == {"api", "per_notice_xml"}, f"unexpected lanes: {lanes}"
    # boundary dedupe: no publication_number may appear in BOTH lanes
    dup = con.execute(
        "SELECT count(*) FROM (SELECT publication_number FROM v_procurement_ted_winner_history "
        "GROUP BY 1 HAVING count(DISTINCT source_lane) > 1)"
    ).fetchone()[0]
    assert dup == 0, "publication_number present in both lanes — boundary dedupe failed"
    # the eForms competition fields exist only on the 2024+ API lane
    leaked = con.execute(
        "SELECT count(*) FROM v_procurement_ted_winner_history "
        "WHERE source_lane = 'per_notice_xml' AND procedure_type IS NOT NULL"
    ).fetchone()[0]
    assert leaked == 0, "competition field populated on a pre-2024 (legacy) row"
    # winner_name _NNNNN eForms suffix is stripped for display
    suffix = con.execute(
        r"SELECT count(*) FROM v_procurement_ted_winner_history WHERE regexp_matches(winner_name, '_[0-9]+$')"
    ).fetchone()[0]
    assert suffix == 0, "winner_name _NNNNN suffix not stripped"


@pytest.mark.sql
def test_v_procurement_ted_tenders_pre_award_grain():
    """The TED tender-pipeline view (cn-standard) is a pre-award grain: value_safe_to_sum must
    be FALSE on every row (estimates are never summable across grains), value_kind is the
    pre-award marker, and the contract columns the tab reads are present. Integration-data only."""
    if not _USE_REAL_PATHS:
        pytest.skip("TED tenders silver is gitignored and unfixtured (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_TED_TENDERS_SILVER))
    con = _con()
    con.execute(_load("procurement_ted_tenders.sql"))
    df = con.execute("SELECT * FROM v_procurement_ted_tenders LIMIT 5").pl()
    _assert_cols(
        df,
        "publication_number",
        "buyer_name",
        "cpv_division",
        "procedure_type",
        "submission_deadline",
        "is_still_open",
        "estimated_value_eur",
        "value_safe_to_sum",
    )
    assert con.execute("SELECT count(*) FROM v_procurement_ted_tenders").fetchone()[0] > 0
    # FIREWALL: a pre-award estimate is never summable — not one row may be value_safe_to_sum.
    summable = con.execute("SELECT count(*) FROM v_procurement_ted_tenders WHERE value_safe_to_sum").fetchone()[0]
    assert summable == 0, "a tender estimate was marked value_safe_to_sum — three-grain firewall breach"
    # one row per notice (no fan-out)
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT publication_number) FROM v_procurement_ted_tenders"
    ).fetchone()
    assert n == distinct, f"tenders view not one-row-per-notice: {n} rows, {distinct} distinct"


# ---------------------------------------------------------------------------
# VIEW-NAMING LINT  (no data needed — always runs)
# ---------------------------------------------------------------------------
#
# The project convention is a `v_` prefix on every view. Three production views
# predate it and are NOT prefixed; they are an accepted, documented exception.
# This test locks that set: it fails if a NEW non-prefixed view is added (drift)
# OR if one of the known exceptions is finally renamed (update the allowlist).

# Views that intentionally lack the `v_` prefix (legacy; see the votes section).
_KNOWN_UNPREFIXED_VIEWS = {
    "party_vote_breakdown",
    "td_vote_summary",
    "td_vote_year_summary",
}

_CREATE_VIEW_RE = re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)", re.IGNORECASE)


def test_view_names_follow_v_prefix_convention():
    """Every view name should start with `v_`, except the documented legacy set."""
    unprefixed = set()
    for sql_file in sorted(SQL_VIEWS_DIR.glob("**/*.sql")):
        text = sql_file.read_text(encoding="utf-8")
        for name in _CREATE_VIEW_RE.findall(text):
            if not name.startswith("v_"):
                unprefixed.add(name)

    new_offenders = unprefixed - _KNOWN_UNPREFIXED_VIEWS
    assert not new_offenders, (
        f"New view(s) without the 'v_' prefix: {sorted(new_offenders)}. Add the prefix to match the convention."
    )

    fixed = _KNOWN_UNPREFIXED_VIEWS - unprefixed
    assert not fixed, (
        f"These views were renamed to the 'v_' convention: {sorted(fixed)}. "
        "Remove them from _KNOWN_UNPREFIXED_VIEWS in this test."
    )


# ---------------------------------------------------------------------------
# MEMBER-OVERVIEW CONNECTION SMOKE TEST
# ---------------------------------------------------------------------------
#
# The member-overview page builds its connection from a bespoke ORDERED file
# list (not a glob group), spanning views from many domains plus the questions
# chain whose ordering bug bit on 2026-05-31. Production loads it through a
# helper that ALWAYS swallows errors, so a break renders an empty hero silently.
# This re-runs that exact ordered list failing-loud. It imports the production
# file lists so it can't drift; if the import fails (e.g. Streamlit unavailable)
# the test skips rather than errors.


@pytest.mark.sql
def test_member_overview_connection_builds():
    try:
        from data_access.member_overview_data import (
            _CONTACT_DETAILS_FILES,
            _DOMAIN_FILES,
            _EXTERNAL_LINKS_FILES,
            _REGISTRY_FILES,
            _VOTE_FILES,
        )
    except Exception as exc:  # noqa: BLE001 — import side-effects (streamlit/config)
        pytest.skip(f"member_overview_data not importable in this env: {exc}")

    # _load() already substitutes {MEMBER_PARQUET_PATH}, {SEANAD_MEMBER_PARQUET_PATH},
    # {EXTERNAL_LINKS_PARQUET_PATH}, {CONTACT_DETAILS_PARQUET_PATH} and {PARQUET_PATH}
    # — the full set these files use.
    ordered_files = [
        *_DOMAIN_FILES,
        *_REGISTRY_FILES,
        *_EXTERNAL_LINKS_FILES,
        *_CONTACT_DETAILS_FILES,
        *_VOTE_FILES,
    ]

    con = _con()
    for fname in ordered_files:
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"member_overview: source data not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001 — surface the offending file
            pytest.fail(f"member_overview: {fname} failed to register: {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# NULL / EMPTY-STRING REGRESSION GUARDS (2026-06-11 audit)
# Synthetic tmp_path fixtures — no real pipeline data needed. Each test locks a
# bug class found in the null/empty-string sweep: display sentinels papering
# over recoverable data, and sibling views drifting on dirty-value filters.
# ---------------------------------------------------------------------------


def _write_sponsors_fixture(tmp_path):
    """Minimal sponsors.parquet feeding BOTH v_legislation_index and v_legislation_detail."""
    import polars as pl

    pdir = tmp_path / "data" / "silver" / "parquet"
    pdir.mkdir(parents=True)
    n = 3
    df = pl.DataFrame(
        {
            "bill_year": ["2024", "2024", "2023"],
            "bill_no": ["10", "11", "12"],
            "short_title_en": ["PMB Bill 2024", "Govt Bill 2024", "Orphan Bill 2023"],
            "long_title_en": ["An Act A", "An Act B", "An Act C"],
            "status": ["Current", "Enacted", "Lapsed"],
            "bill_type": ["Private Member", "Government", "Government"],
            "source": ["Private Member"] * n,
            "origin_house": ["Dáil Éireann"] * n,
            # The three sponsor shapes in the silver data:
            #   PMB    -> sponsor_by_show_as set
            #   Govt   -> ONLY sponsor_as_show_as set (the 557-bill em-dash regression)
            #   Orphan -> both NULL (must be excluded by the WHERE, not shown as '—')
            "sponsor_by_show_as": ["Jane Doe", None, None],
            "sponsor_as_show_as": [None, "Minister for Health", None],
            "sponsor_is_primary": [True, None, None],
            "unique_member_code": ["JaneDoe.D.2020", None, None],
            "context_date": ["2024-01-15", "2024-02-20", "2023-03-01"],
            "last_updated": ["2024-06-01"] * n,
            "method": ["api"] * n,
            "most_recent_stage_event_show_as": ["Second Stage"] * n,
            "most_recent_stage_event_progress_stage": ["3", "11", "2"],
            "most_recent_stage_event_house_show_as": ["Dáil Éireann"] * n,
            "most_recent_stage_event_stage_completed": ["false", "true", "false"],
            "bill_url": ["u10", "u11", "u12"],
        }
    )
    df.write_parquet(pdir / "sponsors.parquet")


@pytest.mark.sql
def test_legislation_index_sponsor_falls_back_like_detail(tmp_path):
    """Government bills carry the sponsor ONLY in sponsor_as_show_as. The index
    once coalesced sponsor_by_show_as straight to '—', so 557 bills (34%)
    rendered an em-dash the detail panel resolved fine. Lock the fallback AND
    index↔detail parity so the two COALESCE chains can't drift again."""
    _write_sponsors_fixture(tmp_path)
    con = _con()
    for fname in ("legislation_index.sql", "legislation_detail.sql"):
        sql = _view_path(fname).read_text(encoding="utf-8")
        sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
        con.execute(sql)

    idx = con.execute("SELECT bill_id, sponsor FROM v_legislation_index").pl()
    # Orphan (both sponsor fields NULL) is excluded by the WHERE — so a '—'
    # sponsor can only mean a skipped fallback, never genuinely-missing data.
    assert idx.height == 2
    assert "2023_12" not in set(idx["bill_id"])
    assert "—" not in set(idx["sponsor"]), "index dropped to '—' despite sponsor_as_show_as being populated"
    by_id = dict(idx.iter_rows())
    assert by_id["2024_10"] == "Jane Doe"
    assert by_id["2024_11"] == "Minister for Health"

    # Parity: every bill must show the SAME sponsor on index and detail.
    mismatch = con.execute(
        """
        SELECT i.bill_id, i.sponsor, d.sponsor
        FROM v_legislation_index i JOIN v_legislation_detail d USING (bill_id)
        WHERE i.sponsor IS DISTINCT FROM d.sponsor
        """
    ).fetchall()
    assert mismatch == [], f"index/detail sponsor drift: {mismatch}"


def _write_dirty_awards_fixture(tmp_path):
    """procurement_awards.parquet with the literal dirty authority values the
    eTenders source emits ('NULL', ''), plus an undated row."""
    import polars as pl

    pdir = tmp_path / "data" / "gold" / "parquet"
    pdir.mkdir(parents=True)
    df = pl.DataFrame(
        {
            "Contracting Authority": [
                "Dublin City Council",
                "Dublin City Council",
                "NULL",  # literal string — must be excluded by BOTH views
                "",  # empty string — must be excluded by BOTH views
                None,  # honest null — must be excluded by BOTH views
                "Health Service Executive",  # undated — summary only
            ],
            "Notice Published Date/Contract Created Date": [
                "01/02/2023",
                "15/03/2024",
                "01/02/2023",
                "01/02/2023",
                "01/02/2023",
                None,
            ],
            "supplier_norm": ["acme", "beta", "ghost", "ghost2", "ghost3", "gamma"],
            "value_eur": [100000.0, 50000.0, 1.0, 1.0, 1.0, 75000.0],
            "value_safe_to_sum": [True, True, True, True, True, True],
        }
    )
    df.write_parquet(pdir / "procurement_awards.parquet")


@pytest.mark.sql
def test_procurement_authority_views_agree_on_dirty_value_filters(tmp_path):
    """v_procurement_authority_summary and its per-year sibling must exclude the
    SAME dirty authority values ('', 'NULL', NULL). The year view once dropped
    only '' — harmless while gold is coerced upstream, but a silent universe
    split the moment a literal-NULL regresses. Dirty rows here make any future
    one-sided filter edit fail loudly."""
    _write_dirty_awards_fixture(tmp_path)
    con = _con()
    for fname in ("procurement_authority_summary.sql", "procurement_authority_year_summary.sql"):
        sql = _view_path(fname).read_text(encoding="utf-8")
        sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
        con.execute(sql)

    alltime = set(
        con.execute("SELECT contracting_authority FROM v_procurement_authority_summary").pl()["contracting_authority"]
    )
    yearly = con.execute("SELECT contracting_authority, year FROM v_procurement_authority_year_summary").pl()

    dirty = {"", "NULL", None}
    assert not (alltime & dirty), f"all-time view leaked dirty authorities: {alltime & dirty}"
    assert not (set(yearly["contracting_authority"]) & dirty), "year view leaked dirty authorities"

    # Same universe, modulo the documented difference: undated rows only exist all-time.
    assert alltime == {"Dublin City Council", "Health Service Executive"}
    assert set(yearly["contracting_authority"]) == {"Dublin City Council"}
    assert set(yearly["year"]) == {2023, 2024}


# --- v_procurement_expiring_contracts (TED advertised-term projection) ---


def _write_ted_awards_term_fixture(tmp_path):
    """Minimal ted_ie_awards.parquet rows covering every expiring-contracts rule."""
    import polars as pl

    pdir = tmp_path / "data" / "silver" / "parquet"
    pdir.mkdir(parents=True)
    base = {
        "notice_url": "u",
        "cpv_code": "45000000",
        "cpv_division": "Construction",
        "award_value_eur": 100000.0,
        "value_kind": "contract_award_value",
        "is_multi_supplier_framework": False,
        "is_pan_eu_outlier": False,
        "contract_conclusion_date": "2025-01-01",
        "contract_duration_months": 24.0,
        "renewal_max": None,
        "contract_end_date_est": "2027-01-01",
        "contract_end_basis": "conclusion_plus_duration",
        "dispatch_date": "2025-01-10",
        "year": 2025,
        "n_winners": 1,
        "supplier_class": "company",
    }
    rows = [
        # 1. plain company award with an estimate
        {**base, "publication_number": "1-2025", "buyer_name": "Dublin City Council_123", "winner_name": "Acme Ltd"},
        # 2. two winner-rows of ONE notice -> must collapse to one view row, names joined
        {**base, "publication_number": "2-2025", "buyer_name": "HSE", "winner_name": "Alpha Ltd", "n_winners": 2},
        {**base, "publication_number": "2-2025", "buyer_name": "HSE", "winner_name": "Beta Ltd", "n_winners": 2},
        # 3. sole-trader winner -> notice listed, name WITHHELD
        {
            **base,
            "publication_number": "3-2025",
            "buyer_name": "OPW",
            "winner_name": "Jane Bloggs",
            "supplier_class": "sole_trader_or_individual",
        },
        # 4. pan-EU outlier -> EXCLUDED entirely
        {
            **base,
            "publication_number": "4-2025",
            "buyer_name": "GÉANT",
            "winner_name": "MegaCo",
            "is_pan_eu_outlier": True,
        },
        # 5. no end estimate -> EXCLUDED
        {
            **base,
            "publication_number": "5-2025",
            "buyer_name": "Revenue",
            "winner_name": "NoTerm Ltd",
            "contract_end_date_est": None,
            "contract_end_basis": None,
        },
    ]
    pl.DataFrame(rows).write_parquet(pdir / "ted_ie_awards.parquet")


@pytest.mark.sql
def test_v_procurement_expiring_contracts_contract(tmp_path):
    """Locks the signal's honesty rules: notice grain (no winner-row inflation),
    sole-trader names withheld but notice kept, pan-EU outliers and no-estimate
    rows excluded, buyer-name artefact cleanup, basis carried for display."""
    _write_ted_awards_term_fixture(tmp_path)
    sql = _view_path("procurement_expiring_contracts.sql").read_text(encoding="utf-8")
    sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
    con = _con()
    con.execute(sql)
    df = con.execute("SELECT * FROM v_procurement_expiring_contracts ORDER BY publication_number").pl()

    assert df.height == 3, "expected notices 1,2,3 only (pan-EU + no-estimate excluded)"
    by = {r["publication_number"]: r for r in df.to_dicts()}
    assert set(by) == {"1-2025", "2-2025", "3-2025"}

    # buyer artefact suffix stripped; winners aggregated to one row per notice
    assert by["1-2025"]["buyer_name"] == "Dublin City Council"
    assert sorted(by["2-2025"]["winners_display"].split("; ")) == ["Alpha Ltd", "Beta Ltd"]

    # privacy: the sole trader's name never appears; the notice itself survives
    assert by["3-2025"]["winners_display"] is None
    assert "Jane Bloggs" not in str(df.to_dicts())

    # the estimate's provenance is carried for honest display
    assert by["1-2025"]["contract_end_basis"] == "conclusion_plus_duration"
    assert str(by["1-2025"]["contract_end_date_est"]) == "2027-01-01"


# ---------------------------------------------------------------------------
# CONSTITUENCY HOUSING-ENRICHMENT TRIPWIRE (2026-06-19)
# These two views register with swallow_errors=True in constituency_conn(), so a
# break (renamed column, dropped parquet, mis-edited la_map) fails SILENTLY — the
# page section just disappears. This test fails LOUD instead. It also asserts the
# explicit la_map produces ZERO mis-joins (every serving council resolves to data).
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_constituency_housing_enrichment_views_build():
    _skip_missing(
        _DATA_BASE / "data" / "_meta" / "constituency_la_crosswalk.csv",
        GOLD_PARQUET_DIR / "ssha_a1_8_time_on_list_wide.parquet",
        GOLD_PARQUET_DIR / "noac_h2_vacancies_wide.parquet",
        GOLD_PARQUET_DIR / "noac_h1_stock_wide.parquet",
        GOLD_PARQUET_DIR / "noac_h7_retrofit_wide.parquet",
        GOLD_PARQUET_DIR / "noac_m2_collection_wide.parquet",
        GOLD_PARQUET_DIR / "derelict_sites_levy_wide.parquet",
    )
    con = _con()
    for fname in (
        "constituency_la_crosswalk.sql",
        "constituency_ssha_waiting_list.sql",
        "constituency_council_housing_performance.sql",
    ):
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"constituency housing: source not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001 — surface the offending file
            pytest.fail(f"constituency housing: {fname} failed to register: {type(exc).__name__}: {exc}")

    # SSHA waiting list — demand layer
    ssha = _result(con, "v_constituency_ssha_waiting_list", limit=100)
    assert ssha.height > 0
    for c in ("constituency_name", "local_authority", "waiting_total_2025", "long_wait_pct"):
        assert c in ssha.columns, f"v_constituency_ssha_waiting_list missing {c}"
    # explicit la_map => every serving council resolves (no NULL = no mis-join)
    assert ssha["waiting_total_2025"].null_count() == 0

    # NOAC council performance — operations layer, value + national-median benchmark
    perf = _result(con, "v_constituency_council_housing_performance", limit=100)
    assert perf.height > 0
    for c in ("vacancy_pct", "reletting_weeks", "retrofit_pct_of_stock", "nat_vacancy_pct"):
        assert c in perf.columns, f"v_constituency_council_housing_performance missing {c}"
    assert perf["vacancy_pct"].null_count() == 0
    # benchmark column is the national median (constant across all rows)
    assert perf["nat_vacancy_pct"].n_unique() == 1
    # collection + enforcement layer (NOAC M2 + Derelict Sites Levy) joins cleanly —
    # M2 shares NOAC naming, derelict has its own explicit map; both must resolve.
    for c in ("rent_collection_pct", "derelict_outstanding_eur", "nat_rent_collection_pct"):
        assert c in perf.columns, f"v_constituency_council_housing_performance missing {c}"
    assert perf["rent_collection_pct"].null_count() == 0
    assert perf["derelict_outstanding_eur"].null_count() == 0


@pytest.mark.sql
def test_ssha_waiting_list_national_views_build():
    """National Housing-screen views: composition (who's waiting) + totals (league
    table w/ per-capita). Guards the UNPIVOT labelling, the 3-grain rollup, and the
    LA->county->PEA08 maps (a name drift silently drops a county from the rollup)."""
    import polars as pl

    _skip_missing(
        GOLD_PARQUET_DIR / "ssha_a1_8_time_on_list_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_7_tenure_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_2_employment_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_4_household_size_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_9_citizenship_wide.parquet",
        GOLD_PARQUET_DIR / "cso_pea08.parquet",
        GOLD_PARQUET_DIR / "cso_vac14.parquet",
        GOLD_PARQUET_DIR / "cso_f2023b.parquet",
        GOLD_PARQUET_DIR / "cso_hap01.parquet",
        GOLD_PARQUET_DIR / "cso_ndq09.parquet",
        GOLD_PARQUET_DIR / "cso_hap17.parquet",
        GOLD_PARQUET_DIR / "cso_hap20.parquet",
        GOLD_PARQUET_DIR / "cso_hap32.parquet",
    )
    con = _con()
    for fname in (
        "housing_ssha_waiting_list_composition.sql",
        "housing_ssha_waiting_list_totals.sql",
        "housing_supply_national.sql",
        "housing_completions_trend.sql",
        "housing_rent_by_county.sql",
        "housing_hap_national.sql",
    ):
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"ssha national: source not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"ssha national: {fname} failed to register: {type(exc).__name__}: {exc}")

    comp = con.execute("SELECT * FROM v_ssha_waiting_list_composition").pl()
    assert set(comp["grain"].unique()) == {"national", "county", "la"}
    assert set(comp["dimension"].unique()) == {
        "time_on_list", "tenure", "employment", "household", "citizenship",
        "age", "income", "main_need", "accom_need",
    }
    # every category is labelled — no SSHA column-slug leaked through as a category in ANY
    # dimension (a slug looks like lowercase_with_underscores; labels are Title Case)
    leaked = comp.filter(pl.col("category").str.contains("^[a-z0-9_]+$"))
    assert leaked.height == 0, f"unlabelled slugs leaked: {set(leaked['category'].unique())}"
    # ord set on every row (drives bar ordering)
    assert comp.filter(pl.col("ord").is_null()).height == 0
    # citizenship is exactly the 4 source categories (sensitivity: no surprise buckets)
    cit = set(comp.filter(pl.col("dimension") == "citizenship")["category"].unique())
    assert cit == {"Irish", "EEA", "Non-EEA", "UK"}
    # main_need: the 5 disability sub-types are rolled into one "Disability (any)" (legibility)
    needs = set(comp.filter(pl.col("dimension") == "main_need")["category"].unique())
    assert "Disability (any)" in needs
    assert not any("disability" in n.lower() and n != "Disability (any)" for n in needs)
    # a national distribution sums to ~100%
    nat_time = comp.filter(
        (pl.col("grain") == "national") & (pl.col("dimension") == "time_on_list") & (pl.col("year") == 2025)
    )
    assert abs(nat_time["pct"].sum() - 100.0) < 0.5

    tot = con.execute("SELECT * FROM v_ssha_waiting_list_totals").pl()
    nat = tot.filter(pl.col("grain") == "national")
    cty = tot.filter(pl.col("grain") == "county")
    la = tot.filter(pl.col("grain") == "la")
    assert cty.height == 26 and la.height == 31 and nat.height == 1
    # rollup integrity: county sum == LA sum == national (a dropped LA breaks this)
    national_total = nat["waiting_total"][0]
    assert cty["waiting_total"].sum() == national_total
    assert la["waiting_total"].sum() == national_total
    # per-capita present for every county + national, never faked at LA grain
    assert cty["waiters_per_1000"].null_count() == 0
    assert la["waiters_per_1000"].null_count() == la.height

    # supply & affordability — single national row, the three CSO metrics present
    sup = con.execute("SELECT * FROM v_housing_supply_national").pl()
    assert sup.height == 1
    s = sup.row(0, named=True)
    assert (s["vacant_dwellings"] or 0) > 0 and 0 < (s["vacancy_rate"] or 0) < 100
    assert (s["avg_weekly_private_rent"] or 0) > 0
    assert (s["hap_households"] or 0) > 0

    # completions trend — only complete years (no part-reported "drop"); ascending
    ct = con.execute("SELECT * FROM v_housing_completions_trend ORDER BY year").pl()
    assert ct.height >= 5 and (ct["completions"] > 0).all()
    assert ct["year"].is_sorted()

    # rent by county — 24 of 26 (Dublin + Galway split in F2023B, deliberately absent)
    rent = con.execute("SELECT * FROM v_housing_rent_by_county").pl()
    assert rent.height == 24
    assert "Dublin" not in rent["county"].to_list() and "Galway" not in rent["county"].to_list()
    assert (rent["avg_weekly_private_rent"] > 0).all()


@pytest.mark.sql
def test_accommodation_spend_views_build():
    """Asylum/Ukraine accommodation spend views — the precise spend-category filter must
    NOT pull in Homeless/Student/Conference accommodation or Coastal/Data Protection, and
    the Ukraine stream only appears once it exists in the data (2025+)."""
    import polars as pl

    _skip_missing(
        GOLD_PARQUET_DIR / "procurement_payments_fact.parquet",
        GOLD_PARQUET_DIR / "dceidy_ipas_legacy_spend.parquet",
    )
    con = _con()
    for fname in ("housing_accommodation_spend_by_year.sql", "housing_accommodation_spend_providers.sql"):
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"accommodation spend: source not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"accommodation spend: {fname} failed: {type(exc).__name__}: {exc}")

    yr = con.execute("SELECT * FROM v_accommodation_spend_by_year").pl()
    assert yr.height > 0
    assert (yr["total_eur"] > 0).all()
    # category filter is tight: no homeless/student/coastal leakage. A single year (IP +
    # Ukraine combined, incl. the 2023-2024 DCEDIY surge) tops out ~€1.8bn; a leak would
    # balloon it past ~€2.5bn (well over the C&AG IP+Ukraine envelope).
    assert yr["total_eur"].max() < 2_500_000_000
    # Ukraine stream only from 2023+ (the war began 2022; no Ukraine accommodation before)
    pre = yr.filter(pl.col("year") < 2023)
    assert pre["ukraine_eur"].fill_null(0).sum() == 0

    prov = con.execute("SELECT * FROM v_accommodation_spend_providers").pl()
    assert prov.height > 50 and (prov["total_eur"] > 0).all()
    assert prov["total_eur"].is_sorted(descending=True)


# ---------------------------------------------------------------------------
# CONSTITUENCY CHOROPLETH TRIPWIRE (2026-06-19)
# v_constituency_map_layers feeds the national index choropleth. It JOINs
# v_constituency_registry + v_constituency_house_work and registers with
# swallow_errors=True in constituency_conn(), so a break (renamed source column,
# NTILE typo) fails SILENTLY — the map just disappears. This fails LOUD instead,
# and pins the quintile buckets to 1..5 (the page indexes a 5-colour palette with
# them — an out-of-range bucket would IndexError or mis-colour). Skips cleanly when
# the member/registry sources aren't present on this box.
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_constituency_map_layers_view_builds():
    try:
        from dail_tracker_core.connections import constituency_conn
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"constituency_conn import unavailable: {exc}")
    con = constituency_conn()
    try:
        df = con.execute("SELECT * FROM v_constituency_map_layers").pl()
    except duckdb.CatalogException:
        pytest.skip("v_constituency_map_layers absent — member/registry sources not on this box")

    assert df.height == 43, f"expected all 43 constituencies, got {df.height}"
    for c in (
        "constituency_name",
        "population_2022",
        "population_per_td",
        "pct_landlord_tds",
        "questions_per_td",
        "q_population",
        "q_population_per_td",
        "q_pct_landlord_tds",
        "q_questions_per_td",
    ):
        assert c in df.columns, f"v_constituency_map_layers missing {c}"

    # quintile buckets always land in 1..5 — the page maps them onto a 5-colour ramp.
    for qcol in ("q_population", "q_population_per_td", "q_pct_landlord_tds", "q_questions_per_td"):
        vals = set(df[qcol].drop_nulls().to_list())
        assert vals <= {1, 2, 3, 4, 5}, f"{qcol} out-of-range quintile(s): {vals - {1, 2, 3, 4, 5}}"

    # population is the Census-2022 spine: present for every constituency, fully bucketed.
    assert df["population_2022"].null_count() == 0
    assert df["q_population"].null_count() == 0


# ---------------------------------------------------------------------------
# COMMITTEE EVIDENCE / MEETING HISTORY
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_committee_meetings_executes():
    """Committee meeting-history spine — one row per (committee, date) with the
    session topics, witness orgs/people, and the transcript link. Locks the
    column contract the Committees page meeting-history section reads, the
    casefold crosswalk key the page filters on, and the LEFT-JOIN no-inflation
    invariant (orgs/persons aggregated to lists, never fanning out the spine)."""
    _skip_missing(
        GOLD_PARQUET_DIR / "committee_meetings.parquet",
        GOLD_PARQUET_DIR / "committee_witnesses.parquet",
        GOLD_PARQUET_DIR / "committee_witness_persons.parquet",
    )
    con = _con()
    con.execute(_load("committee_evidence_meetings.sql"))
    result = _result(con, "v_committee_meetings")
    for col in (
        "committee_code",
        "committee_name",
        "committee_key",
        "date",
        "source_xml",
        "transcript_url",
        "topics",
        "n_topics",
        "n_orgs",
        "n_persons",
        "witness_orgs",
        "witness_persons",
    ):
        assert col in result.columns, f"Expected column {col!r} in v_committee_meetings"
    assert len(result) > 0

    # transcript_url must be re-homed onto the citizen-facing debates site, never
    # left pointing at the raw AKN XML.
    bad_url = con.execute(
        "SELECT count(*) FROM v_committee_meetings"
        " WHERE transcript_url NOT LIKE 'https://www.oireachtas.ie/en/debates/debate/%'"
    ).fetchone()[0]
    assert bad_url == 0, "transcript_url must point at oireachtas.ie/en/debates/debate/"

    # crosswalk key is the lower-cased committee name (page filters on it).
    bad_key = con.execute(
        "SELECT count(*) FROM v_committee_meetings WHERE committee_key <> lower(committee_name)"
    ).fetchone()[0]
    assert bad_key == 0, "committee_key must equal lower(committee_name)"

    # spine is one row per (committee, date) — the witness LEFT JOINs aggregate
    # to lists and must never inflate it.
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (committee_code, date)) FROM v_committee_meetings"
    ).fetchone()
    assert n == distinct, f"v_committee_meetings not one-row-per-(committee, date): {n} rows, {distinct} distinct"
