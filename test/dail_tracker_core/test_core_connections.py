"""Tests for dail_tracker_core/connections._member_view_substitutions.

This helper is the single source of truth (shared by register_member_views and
api_conn) for the absolute parquet paths injected into the member-view SQL. The
refactor that introduced it could regress in exactly two ways, both covered here:

  - a member-view SQL file references a {PLACEHOLDER} the dict doesn't supply,
    which would leave a literal placeholder in the SQL and silently drop that view
    (swallow_errors hides it); and
  - a path is emitted with OS-native backslashes instead of posix, which DuckDB's
    read_parquet rejects on Windows.

It also pins the speech-fact fallback (FULL when present, else the committed lite
slice) that previously lived — copied verbatim — in two functions.
"""

from __future__ import annotations

import re

from dail_tracker_core import connections
from dail_tracker_core.db import SQL_VIEWS_DIR

# The phases that receive substitutions (phase 1 / DOMAIN_FILES gets none).
_SUBSTITUTED_FILES = (
    connections.REGISTRY_FILES
    + connections.EXTERNAL_LINKS_FILES
    + connections.CONTACT_DETAILS_FILES
    + connections.NEWS_MENTIONS_FILES
    + connections.VOTE_FILES
    + connections.SPEECH_FILES
)

_PLACEHOLDER_RE = re.compile(r"\{[A-Z_]+\}")


def _placeholders_in_member_view_sql() -> set[str]:
    found: set[str] = set()
    for name in _SUBSTITUTED_FILES:
        matches = list(SQL_VIEWS_DIR.glob(f"**/{name}"))
        assert matches, f"member-view SQL not found on disk: {name}"
        for sql_file in matches:
            found.update(_PLACEHOLDER_RE.findall(sql_file.read_text(encoding="utf-8")))
    return found


def test_substitutions_cover_every_member_view_placeholder():
    """Every {PLACEHOLDER} the member-view SQL references must have a substitution.

    Subset, not equality: the helper deliberately returns a superset (a phase is
    handed the full dict and consumes only the keys its SQL uses), so extra keys
    are allowed but a missing one is the regression we guard against.
    """
    subs = connections._member_view_substitutions()
    missing = _placeholders_in_member_view_sql() - set(subs)
    assert not missing, f"member-view SQL placeholders with no substitution: {sorted(missing)}"


def test_substitution_paths_are_posix():
    """DuckDB read_parquet needs forward slashes; a stray str(Path) on Windows
    would inject backslashes and fail the read."""
    subs = connections._member_view_substitutions()
    assert subs, "expected config to be importable in the test environment"
    backslashed = {k: v for k, v in subs.items() if "\\" in v}
    assert not backslashed, f"non-posix substitution paths: {backslashed}"


def test_speech_path_prefers_full_when_present(monkeypatch, tmp_path):
    """The full speech fact (all years + full text) wins when it exists on disk."""
    import config

    full = tmp_path / "speeches_fact_full.parquet"
    full.write_bytes(b"")  # make it exist
    lite = tmp_path / "speeches_fact.parquet"
    monkeypatch.setattr(config, "GOLD_SPEECHES_FACT_FULL_PARQUET", full)
    monkeypatch.setattr(config, "GOLD_SPEECHES_FACT_PARQUET", lite)

    subs = connections._member_view_substitutions()
    assert subs["{SPEECH_FACT_PARQUET_PATH}"] == full.as_posix()


def test_speech_path_falls_back_to_lite_when_full_absent(monkeypatch, tmp_path):
    """On a fresh Cloud clone the full fact is gitignored/absent, so the committed
    lite slice is used instead."""
    import config

    full = tmp_path / "speeches_fact_full.parquet"  # never created -> does not exist
    lite = tmp_path / "speeches_fact.parquet"
    lite.write_bytes(b"")
    monkeypatch.setattr(config, "GOLD_SPEECHES_FACT_FULL_PARQUET", full)
    monkeypatch.setattr(config, "GOLD_SPEECHES_FACT_PARQUET", lite)

    subs = connections._member_view_substitutions()
    assert subs["{SPEECH_FACT_PARQUET_PATH}"] == lite.as_posix()


def test_returns_empty_dict_when_config_unimportable(monkeypatch):
    """If config can't be imported the helper degrades to {} rather than raising —
    every caller passes swallow_errors=True, so views just degrade to empty."""
    import builtins

    real_import = builtins.__import__

    def _fail_config(name, *args, **kwargs):
        if name == "config":
            raise ImportError("simulated: config unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fail_config)
    assert connections._member_view_substitutions() == {}
