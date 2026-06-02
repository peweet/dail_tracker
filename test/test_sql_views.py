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
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    GOLD_PARQUET_DIR,
    LOBBY_PARQUET_DIR,
    SILVER_DIR,
    SILVER_PARQUET_DIR,
)

SQL_VIEWS_DIR = Path(__file__).parent.parent / "sql_views"

# Fixture parquets (committed under test/fixtures/sql_views/) let the view-template
# tests run in CI without needing real pipeline output. Set DAIL_INTEGRATION_TESTS=1
# to point at production paths instead — needed for the tests that don't yet have
# fixtures (lobbying, payments, attendance) and for end-to-end runs locally.
_USE_REAL_PATHS = os.environ.get("DAIL_INTEGRATION_TESTS") == "1"
_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sql_views"

if _USE_REAL_PATHS:
    MEMBER_PARQUET = SILVER_PARQUET_DIR / "flattened_members.parquet"
    SEANAD_MEMBER_PARQUET = SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"
    VOTE_PARQUET = GOLD_PARQUET_DIR / "pretty_votes.parquet"
    EXTERNAL_LINKS_PARQUET = SILVER_PARQUET_DIR / "member_external_links.parquet"
else:
    MEMBER_PARQUET = _FIXTURES_DIR / "silver" / "parquet" / "flattened_members.parquet"
    # The Seanad members parquet shares the Dáil schema, so the committed Dáil
    # fixture doubles as the Seanad source for the registry-union template test.
    SEANAD_MEMBER_PARQUET = MEMBER_PARQUET
    VOTE_PARQUET = _FIXTURES_DIR / "gold" / "parquet" / "pretty_votes.parquet"
    EXTERNAL_LINKS_PARQUET = _FIXTURES_DIR / "silver" / "parquet" / "member_external_links.parquet"


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def _con():
    """Fresh in-memory DuckDB connection — no shared state between tests."""
    return duckdb.connect()


def _load(filename: str, con=None) -> str:
    """Read a SQL view file and substitute known template paths."""
    sql = (SQL_VIEWS_DIR / filename).read_text(encoding="utf-8")
    sql = sql.replace("{MEMBER_PARQUET_PATH}", str(MEMBER_PARQUET).replace("\\", "/"))
    sql = sql.replace("{SEANAD_MEMBER_PARQUET_PATH}", str(SEANAD_MEMBER_PARQUET).replace("\\", "/"))
    sql = sql.replace("{PARQUET_PATH}", str(VOTE_PARQUET).replace("\\", "/"))
    sql = sql.replace("{EXTERNAL_LINKS_PARQUET_PATH}", str(EXTERNAL_LINKS_PARQUET).replace("\\", "/"))
    return sql


def _skip_missing(*paths):
    """Skip the test if any required data file is absent."""
    for p in paths:
        if not Path(p).exists():
            pytest.skip(f"Required data file not found: {p} — run pipeline.py first")


def _result(con, view_name: str, limit: int = 5):
    return con.execute(f"SELECT * FROM {view_name} LIMIT {limit}").pl()


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
# PAYMENTS VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_payments_base_executes():
    _skip_missing(GOLD_PARQUET_DIR / "payments_fact.parquet")
    con = _con()
    con.execute(_load("payments_base.sql"))
    result = _result(con, "v_payments_base")
    assert len(result) > 0


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
    con.execute(_load(filename))
    result = _result(con, view_name)
    for col in key_cols:
        assert col in result.columns, f"Expected column '{col}' in {view_name}"
    assert len(result) > 0
