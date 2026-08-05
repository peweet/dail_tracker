"""
Shared fixtures/config for test/sql_views/test_views_*.py.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6) —
a plain module, not a conftest, imported explicitly by each test_views_*.py file so
test/conftest.py machinery stays undisturbed.
"""

import os
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
# utility/ APPENDED (not inserted at front) so the project-root config.py still
# wins — utility/ has its own config.py that would shadow it.
sys.path.append(str(Path(__file__).parents[2] / "utility"))
from config import (
    DATA_DIR,
    GOLD_PARQUET_DIR,
    GOLD_SPEECHES_FACT_PARQUET,  # noqa: F401 -- re-exported for test_views_misc.py
    GOLD_VOTE_HISTORY_PARQUET,  # noqa: F401 -- re-exported for test_views_misc.py
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
