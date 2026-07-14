"""Unit tests for tools/check_extraction_quality.py — the match-rate regression guard.

Lock the pure `find_regressions` verdict (mirrors test_output_regressions.py's style) plus
the ADAPTERS registry against the real coverage-JSON shapes it reads. This is the "did the
extracted FIELDS silently degrade" guard — check_output_regressions only catches row-count
collapse, this catches a same-row-count-but-garbage-match-rate regression.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.check_extraction_quality import ADAPTERS, BASELINE_PATH, META_DIR, emit_current, find_regressions


def test_no_regression_when_identical():
    base = {"x_coverage.json": {"m": 0.95}}
    assert find_regressions(base, base) == []


def test_ratio_improvement_is_not_a_regression():
    base = {"x_coverage.json": {"m": 0.80}}
    cur = {"x_coverage.json": {"m": 0.99}}
    assert find_regressions(cur, base) == []


def test_ratio_drop_beyond_tolerance_flags():
    base = {"x_coverage.json": {"m": 0.95}}
    cur = {"x_coverage.json": {"m": 0.50}}  # far more than 15% relative drop
    out = find_regressions(cur, base, tolerance=0.15)
    assert len(out) == 1
    assert out[0]["kind"] == "MATCH_RATE_DROP"
    assert out[0]["ratio"] == 0.50


def test_ratio_drop_within_tolerance_is_ok():
    base = {"x_coverage.json": {"m": 0.90}}
    cur = {"x_coverage.json": {"m": 0.80}}  # -11.1% relative, under 15% tolerance
    assert find_regressions(cur, base, tolerance=0.15) == []


def test_missing_coverage_file_flags():
    base = {"x_coverage.json": {"m": 0.90}}
    out = find_regressions({}, base)
    assert len(out) == 1 and out[0]["kind"] == "MISSING"


def test_errored_coverage_file_flags_as_missing():
    base = {"x_coverage.json": {"m": 0.90}}
    out = find_regressions({"x_coverage.json": {"error": "missing"}}, base)
    assert out and out[0]["kind"] == "MISSING"


def test_metric_disappearing_from_a_still_present_file_flags():
    base = {"x_coverage.json": {"m": 0.90, "n": 0.80}}
    cur = {"x_coverage.json": {"m": 0.90}}  # metric "n" vanished (schema drift)
    out = find_regressions(cur, base)
    assert len(out) == 1 and out[0]["kind"] == "METRIC_MISSING" and out[0]["metric"] == "n"


def test_new_metric_without_baseline_is_ignored():
    """A brand-new coverage file/metric (not in the baseline yet) is not a regression."""
    base = {"x_coverage.json": {"m": 0.90}}
    cur = {"x_coverage.json": {"m": 0.90}, "brand_new_coverage.json": {"q": 0.10}}
    assert find_regressions(cur, base) == []


# ─────────────────────────────────────────── adapters (real coverage-JSON shapes)


def test_judiciary_diary_link_adapter_reads_row_level_ratios():
    adapt = ADAPTERS["judiciary_diary_link_coverage.json"]
    coverage = {
        "row_level": {
            "cases": {"rows_matched": 7341, "rows_with_judge": 7492},
            "schedule": {"rows_matched": 1564, "rows_with_judge": 1637},
        }
    }
    out = adapt(coverage)
    assert out["cases_row_level"] == (7341, 7492)
    assert out["schedule_row_level"] == (1564, 1637)


def test_supplier_entity_xref_adapter_reads_cro_match():
    adapt = ADAPTERS["supplier_entity_xref_coverage.json"]
    coverage = {"supplier_entities": 10017, "with_cro": 6469}
    assert adapt(coverage)["cro_match"] == (6469, 10017)


def test_emit_current_marks_missing_file_as_error(tmp_path):
    # An empty meta dir: every ADAPTERS entry is absent -> each gets an "error" marker,
    # never a crash.
    out = emit_current(meta_dir=tmp_path)
    assert set(out) == set(ADAPTERS)
    assert all(v == {"error": "missing"} for v in out.values())


def test_emit_current_survives_a_schema_drift(tmp_path):
    import orjson

    bad = tmp_path / "judiciary_diary_link_coverage.json"
    bad.write_bytes(orjson.dumps({"unexpected": "shape"}))
    out = emit_current(meta_dir=tmp_path)
    assert "error" in out["judiciary_diary_link_coverage.json"]


# ─────────────────────────────────────────── integration (real pipeline output)


@pytest.mark.skipif(
    os.environ.get("DAIL_INTEGRATION_TESTS") != "1",
    reason="needs real pipeline output + a committed baseline (set DAIL_INTEGRATION_TESTS=1)",
)
def test_real_coverage_has_no_regression_vs_committed_baseline():
    import orjson

    if not BASELINE_PATH.exists():
        pytest.skip(f"no baseline at {BASELINE_PATH} yet — run --update-baseline once")
    baseline = orjson.loads(BASELINE_PATH.read_bytes())
    current = emit_current(meta_dir=META_DIR)
    regressions = find_regressions(current, baseline)
    assert regressions == [], f"extraction-quality regression(s): {regressions}"
