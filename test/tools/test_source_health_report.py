"""Unit tests for tools/source_health_report.py — the read-only source-health canary.

Pure unit tests (no marker) so they run in the default CI lane. They write tiny
source_health.json files to a tmp path and assert the exit code, since CI relies
on that exit code to decide whether to open a tracking issue.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.source_health_report import _report  # noqa: E402


def _write(tmp_path: Path, sources, summary=None) -> Path:
    payload = {
        "generated_at": "2026-06-07T16:09:03Z",
        "links_checked": False,
        "summary": summary or {},
        "sources": sources,
    }
    p = tmp_path / "source_health.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def _src(source_id, status, check_type="file_age", detail=""):
    return {"source_id": source_id, "status": status, "check_type": check_type, "detail": detail}


def test_all_ok_passes(tmp_path):
    p = _write(tmp_path, [_src("file_sources:cro_companies", "ok")])
    assert _report(p) == 0


def test_skipped_only_passes(tmp_path):
    """The common shape: offline run leaves online sources 'skipped'. Not a failure."""
    p = _write(tmp_path, [_src("local_authority_afs:carlow", "skipped", check_type="index_poll")])
    assert _report(p) == 0


def test_warning_does_not_gate(tmp_path):
    """A 'warning' (e.g. no threshold configured) is context only, not a failure."""
    p = _write(tmp_path, [_src("file_sources:x", "warning", detail="no stale threshold configured")])
    assert _report(p) == 0


def test_failed_source_gates(tmp_path):
    """A 'failed' file_age source (poller stopped / overdue drop) → exit 1."""
    p = _write(
        tmp_path,
        [
            _src("file_sources:cro_companies", "ok"),
            _src("file_sources:charities_register", "failed", detail="stale: 200d old > 180d threshold"),
        ],
    )
    assert _report(p) == 1


def test_missing_file_fails(tmp_path):
    """A missing source_health.json is treated as unhealthy, not silently green."""
    assert _report(tmp_path / "does_not_exist.json") == 1


def test_malformed_json_fails(tmp_path):
    p = tmp_path / "source_health.json"
    p.write_text("{not valid json", encoding="utf-8")
    assert _report(p) == 1


def test_empty_sources_passes(tmp_path):
    """No sources recorded is not, by itself, a failure (nothing failed)."""
    p = _write(tmp_path, [])
    assert _report(p) == 0
