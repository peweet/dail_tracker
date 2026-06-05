"""DuckDB connection + SQL-view registration for dail_tracker_core.

This is the Streamlit-free home of the connection bootstrap. A core query
function takes a ``duckdb.DuckDBPyConnection`` as an argument (so it is unit-
testable and interface-agnostic); the caller builds that connection here via
``connect_with_views``.

TRANSITIONAL NOTE: the registration loop below intentionally duplicates
``utility/data_access/_sql_registry.py`` (~25 lines). That shared module is
imported by ~14 live Streamlit data-access modules; rewriting it to re-export
from here would touch all of them at once. To keep each migration PR small and
reversible, core carries its own copy for now. Once every data-access module is
a thin wrapper over core, ``_sql_registry.py`` collapses to a re-export shim (or
is deleted) — tracked as a later consolidation step. The two copies are each
covered by tests, and the path-rewrite rule is identical, so drift is caught.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path

import duckdb

# dail_tracker_core/db.py -> parents[0] = dail_tracker_core, parents[1] = repo root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_VIEWS_DIR = PROJECT_ROOT / "sql_views"

_log = logging.getLogger(__name__)


def absolutize_data_paths(sql: str) -> str:
    """Rewrite ``read_parquet('data/...')`` literals to absolute project paths.

    DuckDB resolves relative literals against the process CWD, which is wrong
    whenever the app is launched from a subdirectory. Anchoring to PROJECT_ROOT
    makes view registration CWD-independent. Identical rule to _sql_registry.
    """
    return sql.replace("'data/", f"'{PROJECT_ROOT.as_posix()}/data/")


def register_views(
    conn: duckdb.DuckDBPyConnection,
    patterns: Iterable[str],
    *,
    substitutions: Mapping[str, str] | None = None,
    swallow_errors: bool,
) -> None:
    """Register SQL views onto ``conn``.

    For each glob pattern (in order), every matching ``sql_views/*.sql`` file is
    read in alphabetical order — preserving the implicit dependency ordering the
    views rely on — then has its ``{KEY}`` substitutions applied, its data paths
    absolutised, and is executed. A failing file is logged and skipped when
    ``swallow_errors`` is True, else the exception propagates.
    """
    subs = substitutions or {}
    for pattern in patterns:
        for sql_file in sorted(SQL_VIEWS_DIR.glob(pattern)):
            try:
                sql = sql_file.read_text(encoding="utf-8")
                for key, val in subs.items():
                    sql = sql.replace(key, val)
                conn.execute(absolutize_data_paths(sql))
            except Exception as exc:
                if not swallow_errors:
                    raise
                _log.warning("view registration failed: %s | %s", sql_file.name, exc)


def connect_with_views(
    patterns: Iterable[str],
    *,
    substitutions: Mapping[str, str] | None = None,
    swallow_errors: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Build a fresh in-memory DuckDB connection with the matching views registered.

    ``patterns`` is a list of globs against ``sql_views/`` (e.g.
    ``["procurement_*.sql"]``). The Streamlit layer wraps this in
    ``@st.cache_resource`` so one connection is reused per session; a future API
    layer would instead build one per request (read-only). Either way the core
    function signature is the same.
    """
    conn = duckdb.connect()
    register_views(conn, patterns, substitutions=substitutions, swallow_errors=swallow_errors)
    return conn
