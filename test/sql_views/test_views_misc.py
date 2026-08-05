"""
SQL view contract tests — cross-cutting smoke/lint tests that don't belong to
a single view family: vote views, the production registration-loader smoke
test, the view-naming lint, the member-overview connection smoke test, and the
public-appointments view.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import re

import duckdb
import pytest

from dail_tracker_core.db import register_views

from ._view_test_helpers import (
    GOLD_SPEECHES_FACT_PARQUET,
    GOLD_VOTE_HISTORY_PARQUET,
    SQL_VIEWS_DIR,
    VOTE_PARQUET,
    _assert_cols,
    _con,
    _load,
    _result,
    _skip_missing,
    _src,
)

# ---------------------------------------------------------------------------
# VOTE VIEWS  ({PARQUET_PATH} substituted)
# ---------------------------------------------------------------------------

# NOTE: View names and column assertions match what each SQL file actually
# CREATEs. Some views use the `v_` prefix and some don't — this is an
# inconsistency in production SQL (td_vote_*, v_party_vote_breakdown vs the
# `v_vote_*` convention) that the old test parametrize didn't account for.
# Aliases (full_name → member_name, party → party_name) are reflected here.
VOTE_VIEWS = [
    ("vote_index.sql", "v_vote_index", ["vote_id", "vote_date", "vote_outcome"]),
    ("vote_member_detail.sql", "v_vote_member_detail", ["member_name", "vote_type"]),
    ("vote_party_breakdown.sql", "v_party_vote_breakdown", ["party_name", "vote_type", "member_count"]),
    ("vote_result_summary.sql", "v_vote_result_summary", ["division_count", "member_count"]),
    ("vote_sources.sql", "v_vote_sources", ["vote_id", "source_url"]),
    ("vote_td_summary.sql", "v_td_vote_summary", ["member_name", "yes_count"]),
    ("vote_td_year_summary.sql", "v_td_vote_year_summary", ["member_name", "year"]),
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
    # committees_*.sql views LEFT JOIN v_lobbying_base_member_codes for
    # unique_member_code, so it must load first — mirrors the DOMAIN_REGISTRATIONS
    # "committees" phase in dail_tracker_core/connections.py.
    ("committees", ["lobbying_base_member_codes.sql", "committees_*.sql"], {}),
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
# The project convention is a `v_` prefix on every view. The last three legacy
# exceptions (td_vote_summary / td_vote_year_summary / party_vote_breakdown)
# were renamed to the convention 2026-07-18 — the allowlist is now EMPTY and
# every view must conform.
_KNOWN_UNPREFIXED_VIEWS: set[str] = set()

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
