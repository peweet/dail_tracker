"""Unit tests for the gold content-quality guard (tools/check_gold_quality.py).

Pure-function tests — no committed data needed, so they run in the default CI lane. They
prove the guard MEASURES the right things and, crucially, that a regression actually FIRES
(a guard that never trips is decoration)."""

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parents[2]))
sys.path.insert(0, str(Path(__file__).parents[2] / "tools"))

from check_gold_quality import find_regressions, measure_table, summarise  # noqa: E402

# --------------------------------------------------------------------------- measure_table


def test_measure_counts_all_null_dup_encoding_sentinel():
    df = pl.DataFrame(
        {
            "name": ["A Ltd", "A Ltd", "Caf� Ltd", "B Ltd"],  # rows 0==1 duplicate; row 2 mojibake
            "dead": [None, None, None, None],  # all-null column
            "flag": ["Y", "Y", "N", "null"],  # rows 0==1 (for the dup); row 3 whole-value sentinel
        }
    )
    m = measure_table(df)
    assert m["rows"] == 4
    assert m["all_null_cols"] == ["dead"]
    assert m["dup_rows"] == 2  # both copies of the duplicate row are counted
    assert m["encoding"]["name"] == 1
    assert m["sentinels"]["flag"] == 1


def test_measure_clean_table_has_no_findings():
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    m = measure_table(df)
    assert m["all_null_cols"] == [] and m["dup_rows"] == 0 and m["encoding"] == {} and m["sentinels"] == {}


# --------------------------------------------------------------------------- find_regressions


def _base(**over):
    rec = {"rows": 100, "all_null_cols": [], "dup_rows": 0, "encoding": {}, "sentinels": {}}
    rec.update(over)
    return {"t": rec}


def test_no_regression_when_identical():
    assert find_regressions(_base(), _base()) == []


def test_column_newly_all_null_fires():
    regs = find_regressions(_base(all_null_cols=["x"]), _base())
    assert regs and regs[0]["kind"] == "COL_EMPTIED" and regs[0]["columns"] == ["x"]


def test_stable_all_null_column_does_not_fire():
    # a column that was already all-null in the baseline is NOT a new regression
    assert find_regressions(_base(all_null_cols=["x"]), _base(all_null_cols=["x"])) == []


def test_duplicate_growth_fires_above_threshold():
    regs = find_regressions(_base(dup_rows=200), _base(dup_rows=10))
    assert regs and regs[0]["kind"] == "DUP_INCREASE"


def test_small_duplicate_wobble_does_not_fire():
    # +3 rows is below the absolute floor — stable-but-duplicated tables must not nag
    assert find_regressions(_base(dup_rows=13), _base(dup_rows=10)) == []


def test_stable_high_duplicates_do_not_fire():
    # a table that legitimately carries many duplicates (e.g. 'Vacancy' board rows) is fine
    # as long as it does not GROW — the baseline absorbs the known level
    assert find_regressions(_base(dup_rows=900), _base(dup_rows=900)) == []


def test_new_encoding_artifact_fires():
    regs = find_regressions(_base(encoding={"name": 5}), _base())
    assert regs and regs[0]["kind"] == "ENCODING_INCREASE" and regs[0]["columns"]["name"]["now"] == 5


def test_sentinel_growth_fires_but_stable_does_not():
    assert find_regressions(_base(sentinels={"c": 10}), _base(sentinels={"c": 4}))  # grew → fires
    assert find_regressions(_base(sentinels={"c": 4}), _base(sentinels={"c": 4})) == []  # stable → silent


def test_missing_table_fires():
    regs = find_regressions({}, _base())
    assert regs and regs[0]["kind"] == "MISSING"


# --------------------------------------------------------------------------- summarise


def test_summarise_groups_findings():
    current = {
        "a.parquet": {"rows": 5, "all_null_cols": ["x"], "dup_rows": 0, "encoding": {}, "sentinels": {}},
        "b.parquet": {"rows": 5, "all_null_cols": [], "dup_rows": 3, "encoding": {"n": 1}, "sentinels": {}},
    }
    s = summarise(current)
    assert s["tables_with_all_null_cols"] == {"a.parquet": ["x"]}
    assert s["tables_with_dup_rows"]["b.parquet"]["dup_rows"] == 3
    assert s["tables_with_encoding_artifacts"] == {"b.parquet": {"n": 1}}
