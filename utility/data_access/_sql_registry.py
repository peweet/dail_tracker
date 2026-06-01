"""
Shared SQL-view registration for the data-access layer.

Single source of truth for:
- the project root and sql_views/ directory
- the `'data/...'` → absolute-path rewrite DuckDB needs (it resolves
  read_parquet/read_csv literals against CWD, which is wrong when Streamlit
  is launched from utility/)
- the glob → substitute → absolutize → execute registration loop

Every per-domain data-access module (payments_data, votes_data, …) builds its
DuckDB connection by calling register_views() instead of carrying its own copy
of this boilerplate.

Note on `swallow_errors`: call sites pass their *existing* behaviour explicitly.
Some modules historically raise on a bad view (failing loud) and some log and
continue. That inconsistency is intentional drift left untouched by the dedup
refactor — standardising it is a deliberate behaviour change tracked separately.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_VIEWS_DIR = PROJECT_ROOT / "sql_views"

_log = logging.getLogger(__name__)


def absolutize_data_paths(sql: str) -> str:
    # SQL views use literals like read_parquet('data/silver/...'). DuckDB
    # resolves those against CWD, so a Streamlit launch from utility/ breaks
    # queries. Rewrite to absolute project paths at registration time.
    return sql.replace("'data/", f"'{PROJECT_ROOT.as_posix()}/data/")


def register_views(
    conn: duckdb.DuckDBPyConnection,
    patterns: Iterable[str],
    *,
    substitutions: Mapping[str, str] | None = None,
    swallow_errors: bool,
) -> None:
    """Register SQL views onto `conn`.

    For each glob pattern (in order), every matching sql_views/*.sql file is
    read in alphabetical order — preserving the implicit dependency ordering
    the views rely on — then has its {KEY} substitutions applied, its data
    paths absolutized, and is executed.

    If `swallow_errors` is True a failing file is logged and skipped; otherwise
    the exception propagates.
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
