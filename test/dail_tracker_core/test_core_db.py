"""Unit + integration tests for dail_tracker_core.db.

absolutize_data_paths is a pure-string unit test (always runs). connect_with_views
is exercised against the real procurement views: registration must succeed even
when the underlying gold parquet is absent (DuckDB creates views lazily), which
is the property the data-access layer relies on.
"""

from __future__ import annotations

from dail_tracker_core.db import (
    PROJECT_ROOT,
    absolutize_data_paths,
    connect_with_views,
)


def test_absolutize_rewrites_data_literals():
    sql = "SELECT * FROM read_parquet('data/gold/parquet/x.parquet')"
    out = absolutize_data_paths(sql)
    assert "'data/" not in out
    assert f"'{PROJECT_ROOT.as_posix()}/data/" in out


def test_absolutize_leaves_other_strings_untouched():
    sql = "SELECT 'data is fine' AS note"  # not a read_parquet literal
    # only the read_parquet-style "'data/" prefix is rewritten; a space follows
    # 'data here, so nothing should change.
    assert absolutize_data_paths(sql) == sql


def test_connect_registers_procurement_views_even_without_parquet():
    # View registration is lazy: CREATE VIEW referencing a missing parquet
    # succeeds; the error would only surface on SELECT. So the connection must
    # build and the views must appear in the catalog regardless of gold state.
    conn = connect_with_views(["procurement_*.sql"], swallow_errors=True)
    try:
        views = {
            row[0]
            for row in conn.execute(
                "SELECT view_name FROM duckdb_views() WHERE view_name LIKE 'v_procurement%'"
            ).fetchall()
        }
        assert "v_procurement_supplier_summary" in views
    finally:
        conn.close()
