"""Unit tests for tools/freshness_report.py — the read-only staleness canary.

Pure unit tests (no marker) so they run in the default CI lane. They write tiny
freshness.json files to a tmp path and assert the exit code, since CI relies on
that exit code to decide whether to open a staleness issue.
"""

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools.freshness_report import _report  # noqa: E402


def _write(tmp_path: Path, generated_at, datasets=None) -> Path:
    payload = {
        "generated_at": generated_at,
        "as_of_utc_date": "2026-06-02",
        "datasets": datasets or {"votes": {"status": "ok", "latest_record_date": "2026-04-29"}},
    }
    p = tmp_path / "freshness.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def _iso_days_ago(days: int) -> str:
    d = datetime.now(UTC).replace(microsecond=0) - timedelta(days=days)
    return d.isoformat().replace("+00:00", "Z")


def test_fresh_data_passes(tmp_path):
    """generated_at today → within threshold → exit 0."""
    p = _write(tmp_path, _iso_days_ago(0))
    assert _report(14, p) == 0


def test_stale_data_fails(tmp_path):
    """generated_at well past the threshold → exit 1 (opens an issue in CI)."""
    p = _write(tmp_path, _iso_days_ago(30))
    assert _report(14, p) == 1


def test_threshold_boundary_is_inclusive(tmp_path):
    """Age exactly == threshold is still OK; only strictly older is stale."""
    p = _write(tmp_path, _iso_days_ago(14))
    assert _report(14, p) == 0
    assert _report(13, p) == 1


def test_missing_file_fails(tmp_path):
    """A missing freshness.json is treated as stale, not silently green."""
    assert _report(14, tmp_path / "does_not_exist.json") == 1


def test_malformed_json_fails(tmp_path):
    p = tmp_path / "freshness.json"
    p.write_text("{not valid json", encoding="utf-8")
    assert _report(14, p) == 1


@pytest.mark.parametrize("bad", [None, "", "not-a-date", "garbage"])
def test_missing_or_invalid_generated_at_fails(tmp_path, bad):
    p = _write(tmp_path, bad)
    assert _report(14, p) == 1


def test_non_ok_dataset_status_does_not_gate_exit(tmp_path):
    """Per-dataset 'unavailable' is reported but does NOT fail the run — a
    not-yet-built source must not trip the canary. Only the pipeline-ran age
    (generated_at) and per-dataset 'stale' gate the exit code."""
    datasets = {"lobbying": {"status": "unavailable", "latest_period_end_date": None}}
    p = _write(tmp_path, _iso_days_ago(1), datasets=datasets)
    assert _report(14, p) == 0


def test_stale_dataset_gates_even_when_pipeline_ran_recently(tmp_path):
    """The DAIL-160 case: pipeline ran today (generated_at fresh) but a source froze.
    A per-dataset status='stale' must fail the canary."""
    datasets = {
        "votes": {"status": "stale", "latest_record_date": "2026-04-29", "fetch_age_days": 34, "stale_after_days": 14},
        "questions": {"status": "ok", "latest_record_date": "2026-05-26"},
    }
    p = _write(tmp_path, _iso_days_ago(1), datasets=datasets)
    assert _report(14, p) == 1


def test_all_ok_recent_pipeline_passes(tmp_path):
    datasets = {"votes": {"status": "ok", "latest_record_date": "2026-06-05", "fetch_age_days": 1}}
    p = _write(tmp_path, _iso_days_ago(1), datasets=datasets)
    assert _report(14, p) == 0
