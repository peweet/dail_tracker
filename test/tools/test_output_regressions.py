"""Unit tests for tools/check_output_regressions.py — the completeness guard.

Lock the pure `find_regressions` verdict: silent thinning (row drop / emptied), a removed
column (schema drift), and a missing output are regressions; row growth and new columns are
not. This is the 'is the data COMPLETE' guard that catches a PDF/layout parse silently
shipping partial data behind a green pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.check_output_regressions import find_regressions


def _b(rows, cols):
    return {"rows": rows, "columns": list(cols)}


def test_no_regression_when_identical():
    base = {"a.parquet": _b(100, ["x", "y"])}
    assert find_regressions(base, base) == []


def test_row_growth_and_new_columns_are_not_regressions():
    base = {"a.parquet": _b(100, ["x"])}
    cur = {"a.parquet": _b(250, ["x", "z"])}  # more rows + a new column
    assert find_regressions(cur, base) == []


def test_row_drop_beyond_tolerance_flags():
    base = {"a.parquet": _b(1000, ["x"])}
    cur = {"a.parquet": _b(400, ["x"])}  # -60% > 50% tolerance
    out = find_regressions(cur, base, tolerance=0.5)
    assert len(out) == 1 and out[0]["kind"] == "ROW_DROP" and out[0]["rows"] == 400


def test_row_drop_within_tolerance_is_ok():
    base = {"a.parquet": _b(1000, ["x"])}
    cur = {"a.parquet": _b(600, ["x"])}  # -40% < 50%
    assert find_regressions(cur, base, tolerance=0.5) == []


def test_emptied_table_flags_even_with_huge_tolerance():
    base = {"a.parquet": _b(50, ["x"])}
    cur = {"a.parquet": _b(0, ["x"])}
    out = find_regressions(cur, base, tolerance=0.99)
    assert len(out) == 1 and out[0]["kind"] == "EMPTIED"


def test_removed_column_flags_even_if_rows_grew():
    base = {"a.parquet": _b(100, ["x", "y"])}
    cur = {"a.parquet": _b(500, ["x"])}  # y dropped (schema drift)
    out = find_regressions(cur, base)
    kinds = {r["kind"] for r in out}
    assert "COL_REMOVED" in kinds
    assert any(r.get("columns") == ["y"] for r in out)


def test_missing_output_flags():
    base = {"a.parquet": _b(100, ["x"])}
    out = find_regressions({}, base)
    assert len(out) == 1 and out[0]["kind"] == "MISSING"


def test_unreadable_output_flags_as_missing():
    base = {"a.parquet": _b(100, ["x"])}
    out = find_regressions({"a.parquet": {"error": "ComputeError"}}, base)
    assert out and out[0]["kind"] == "MISSING"


def test_new_output_without_baseline_is_ignored():
    """A brand-new gold table (not in the baseline yet) is not a regression."""
    base = {"a.parquet": _b(100, ["x"])}
    cur = {"a.parquet": _b(100, ["x"]), "brand_new.parquet": _b(5, ["q"])}
    assert find_regressions(cur, base) == []
