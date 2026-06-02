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

sys.path.insert(0, str(Path(__file__).parent.parent))
# utility/ APPENDED (not inserted at front) so the project-root config.py still
# wins — utility/ has its own config.py that would shadow it. We only need
# utility/ on the path to import the real production view loader
# (data_access._sql_registry.register_views) for the registration smoke test.
sys.path.append(str(Path(__file__).parent.parent / "utility"))
from data_access._sql_registry import register_views

from config import (
    DATA_DIR,
    GOLD_PARQUET_DIR,
    GOLD_VOTE_HISTORY_PARQUET,
    LOBBY_PARQUET_DIR,
    SILVER_DIR,
    SILVER_PARQUET_DIR,
)

PROJECT_ROOT = Path(__file__).parent.parent
SQL_VIEWS_DIR = PROJECT_ROOT / "sql_views"

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


def _load(filename: str, con=None) -> str:
    """Read a SQL view file and substitute known template paths."""
    sql = (SQL_VIEWS_DIR / filename).read_text(encoding="utf-8")
    sql = sql.replace("{MEMBER_PARQUET_PATH}", str(MEMBER_PARQUET).replace("\\", "/"))
    sql = sql.replace("{SEANAD_MEMBER_PARQUET_PATH}", str(SEANAD_MEMBER_PARQUET).replace("\\", "/"))
    sql = sql.replace("{PARQUET_PATH}", str(VOTE_PARQUET).replace("\\", "/"))
    # The Seanad vote gold shares the Dáil schema, so the committed Dáil fixture
    # doubles as the Seanad source for v_vote_base's chamber-union template.
    sql = sql.replace("{SEANAD_VOTE_PARQUET_PATH}", str(VOTE_PARQUET).replace("\\", "/"))
    sql = sql.replace("{EXTERNAL_LINKS_PARQUET_PATH}", str(EXTERNAL_LINKS_PARQUET).replace("\\", "/"))
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
# STATUTORY INSTRUMENT VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_statutory_instruments_executes():
    """The SI-as-entity view. Locks the signatory contract the SI detail panel
    reads — si_responsible_actor (printed signing office), si_signatory_name
    (printed signer name), and the tenure-inferred si_minister_name/member_code.
    A schema drift on any of these silently breaks 'who signed the SI'."""
    _skip_missing(GOLD_PARQUET_DIR / "statutory_instruments.parquet")
    con = _con()
    con.execute(_load("legislation_si_index.sql"))
    result = _result(con, "v_statutory_instruments")
    for col in (
        "si_id",
        "si_signed_date",
        "si_responsible_actor",
        "si_signatory_name",
        "si_minister_name",
        "si_minister_member_code",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_statutory_instruments"
    assert len(result) > 0


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
    ("committees", ["committees_*.sql"], {}),
    ("corporate", ["corporate_*.sql"], {}),
    ("interests", ["member_interests_*.sql", "member_zz_interests_*.sql"], {}),
    ("legislation", ["legislation_*.sql"], {}),
    ("lobbying", ["lobbying_*.sql"], {}),
    ("payments", ["payments_*.sql"], {}),
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
def test_member_interests_views_execute():
    """member_interests_views.sql creates FOUR views off one parquet — assert the
    column contract of each in one pass (they share a source, so one load)."""
    _skip_missing(*_src(_INTERESTS_SRC))
    con = _con()
    con.execute(_load("member_interests_views.sql"))

    detail = _result(con, "v_member_interests")
    _assert_cols(detail, "member_id", "member_name", "interest_category", "interest_text", "declaration_year", "house")
    assert len(detail) > 0

    summary = _result(con, "v_member_interests_summary")
    _assert_cols(summary, "members_with_interests_count", "declarations_count", "latest_declaration_year")
    assert len(summary) > 0

    cats = _result(con, "v_member_interests_category_summary")
    _assert_cols(cats, "interest_category", "declarations_count")
    assert len(cats) > 0

    ranking = _result(con, "v_member_interests_ranking")
    _assert_cols(ranking, "member_id", "member_name", "interest_count", "rank")
    assert len(ranking) > 0


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
def test_v_attendance_year_member_counts_executes():
    """Reads v_attendance_member_year_summary — load it first."""
    _skip_missing(*_src("data/gold/parquet/attendance_by_td_year.parquet"))
    con = _con()
    con.execute(_load("attendance_member_year_summary.sql"))
    con.execute(_load("attendance_year_member_counts.sql"))
    result = _result(con, "v_attendance_year_member_counts")
    _assert_cols(result, "year", "house", "members_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_attendance_chamber_sitting_days_executes():
    _skip_missing(*_src("data/silver/aggregated_td_tables.csv"))
    con = _con()
    con.execute(_load("attendance_chamber_sitting_days.sql"))
    result = _result(con, "v_attendance_chamber_sitting_days")
    _assert_cols(result, "house", "year", "sitting_days")
    assert len(result) > 0


@pytest.mark.sql
def test_v_sitting_days_by_year_executes():
    """Hardcoded VALUES reference table — no source file, so no skip. If it stops
    compiling, the attendance-rate denominator breaks."""
    con = _con()
    con.execute(_load("v_sitting_days_by_year.sql"))
    result = _result(con, "v_sitting_days_by_year")
    _assert_cols(result, "year", "total_sitting_days")
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
    for sql_file in sorted(SQL_VIEWS_DIR.glob("*.sql")):
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
            _DOMAIN_FILES,
            _EXTERNAL_LINKS_FILES,
            _REGISTRY_FILES,
            _VOTE_FILES,
        )
    except Exception as exc:  # noqa: BLE001 — import side-effects (streamlit/config)
        pytest.skip(f"member_overview_data not importable in this env: {exc}")

    # _load() already substitutes {MEMBER_PARQUET_PATH}, {SEANAD_MEMBER_PARQUET_PATH},
    # {EXTERNAL_LINKS_PARQUET_PATH} and {PARQUET_PATH} — the full set these files use.
    ordered_files = [*_DOMAIN_FILES, *_REGISTRY_FILES, *_EXTERNAL_LINKS_FILES, *_VOTE_FILES]

    con = _con()
    for fname in ordered_files:
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"member_overview: source data not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001 — surface the offending file
            pytest.fail(f"member_overview: {fname} failed to register: {type(exc).__name__}: {exc}")
