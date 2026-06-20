"""Unit tests for the per-lane freshness ledger.

Covers tools/freshness_heartbeat.py (the writer) and tools/freshness_status.py
(the OK/LATE/MISSING/INFO evaluator). Pure stdlib, no marker, so they run in the
default CI lane. The evaluator takes ``now`` explicitly, so staleness is tested
deterministically without sleeping or mocking the clock.
"""

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import tools.freshness_heartbeat as hb  # noqa: E402
import tools.freshness_status as status  # noqa: E402


@pytest.fixture
def beats_dir(tmp_path, monkeypatch):
    """Point both tools at a throwaway heartbeats dir."""
    d = tmp_path / "heartbeats"
    monkeypatch.setattr(hb, "HEARTBEAT_DIR", d)
    monkeypatch.setattr(status, "HEARTBEAT_DIR", d)
    return d


def _iso_hours_ago(hours: float) -> str:
    t = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=hours)
    return t.isoformat().replace("+00:00", "Z")


def test_record_writes_one_file_per_lane(beats_dir):
    p = hb.record("live_tenders", runner="cloud", cadence_hours=24, note="42 notices")
    assert p == beats_dir / "live_tenders.json"
    import json

    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["lane"] == "live_tenders"
    assert data["runner"] == "cloud"
    assert data["cadence_hours"] == 24
    assert data["note"] == "42 notices"


def test_invalid_lane_slug_rejected(beats_dir):
    with pytest.raises(ValueError):
        hb.record("Bad Lane")  # space + capital → invalid filename slug


def test_fresh_beat_is_ok(beats_dir):
    hb.record("live_tenders", runner="cloud", cadence_hours=24, when=_iso_hours_ago(1))
    rollup = status._evaluate(datetime.now(UTC))
    row = next(r for r in rollup["lanes"] if r["lane"] == "live_tenders")
    assert row["status"] == "OK"
    assert "live_tenders" not in rollup["problem_lanes"]


def test_old_beat_past_grace_is_late(beats_dir):
    # cadence 24h * grace 2.0 = 48h tolerance; 60h old → LATE.
    hb.record("live_tenders", runner="cloud", cadence_hours=24, when=_iso_hours_ago(60))
    rollup = status._evaluate(datetime.now(UTC))
    row = next(r for r in rollup["lanes"] if r["lane"] == "live_tenders")
    assert row["status"] == "LATE"
    assert "live_tenders" in rollup["problem_lanes"]


def test_beat_within_grace_not_late(beats_dir):
    # 40h old < 48h tolerance → still OK (absorbs a late-but-not-missed run).
    hb.record("live_tenders", runner="cloud", cadence_hours=24, when=_iso_hours_ago(40))
    rollup = status._evaluate(datetime.now(UTC))
    row = next(r for r in rollup["lanes"] if r["lane"] == "live_tenders")
    assert row["status"] == "OK"


def test_registered_lane_with_no_beat_is_missing(beats_dir):
    rollup = status._evaluate(datetime.now(UTC))
    row = next(r for r in rollup["lanes"] if r["lane"] == "legal_diary_openview")
    assert row["status"] == "MISSING"
    assert "legal_diary_openview" in rollup["problem_lanes"]


def test_cadenceless_lane_is_info_never_a_problem(beats_dir):
    # `pipeline` has cadence None → INFO even with no beat, and never flagged.
    rollup = status._evaluate(datetime.now(UTC))
    row = next(r for r in rollup["lanes"] if r["lane"] == "pipeline")
    assert row["status"] == "INFO"
    assert "pipeline" not in rollup["problem_lanes"]


def test_unregistered_beat_is_surfaced(beats_dir):
    # A beat for a lane not in the registry still appears (so a typo'd lane is visible).
    hb.record("mystery_lane", runner="cloud", cadence_hours=24, when=_iso_hours_ago(1))
    rollup = status._evaluate(datetime.now(UTC))
    row = next(r for r in rollup["lanes"] if r["lane"] == "mystery_lane")
    assert row["registered"] is False
    assert row["status"] == "OK"


def test_strict_main_exit_code(beats_dir, capsys):
    # No beats at all → registered cadence lanes MISSING → strict exits 1.
    assert status.main(["--strict"]) == 1
    # One fresh beat for every registered cadence lane → strict exits 0.
    for lane, spec in status.LANES.items():
        if spec.get("cadence_hours"):
            hb.record(lane, runner=spec["runner"], cadence_hours=spec["cadence_hours"], when=_iso_hours_ago(1))
    assert status.main(["--strict"]) == 0
