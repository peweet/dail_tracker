"""Unit tests for tools/build_source_health.py.

Pure unit tests (no marker, default CI lane). No network: link checks are never
exercised here (that path needs requests/idna and a live host). The offline
manual-age logic is driven with real temp files + an injected ``now`` so the
staleness threshold and exit-code contract are deterministic.
"""

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools.build_source_health import (  # noqa: E402
    check_file_age,
    run,
)

NOW = datetime(2026, 6, 4, tzinfo=UTC)


def _manual_rec(pattern, threshold):
    return {
        "source_id": "manual:cro_companies", "group": "manual_sources",
        "check_type": "file_age", "input_pattern": pattern,
        "stale_after_days": threshold,
    }


def _touch(path: Path, days_ago: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    ts = (NOW - timedelta(days=days_ago)).timestamp()
    import os
    os.utime(path, (ts, ts))


def test_manual_fresh_is_ok(tmp_path):
    _touch(tmp_path / "data/bronze/cro/companies_2026.csv", days_ago=10)
    rec = _manual_rec("data/bronze/cro/companies_*.csv", threshold=45)
    h = check_file_age(rec, tmp_path, NOW)
    assert h["status"] == "ok"
    assert h["days_old"] == 10


def test_manual_stale_is_failed(tmp_path):
    _touch(tmp_path / "data/bronze/cro/companies_2025.csv", days_ago=60)
    rec = _manual_rec("data/bronze/cro/companies_*.csv", threshold=45)
    h = check_file_age(rec, tmp_path, NOW)
    assert h["status"] == "failed"
    assert "stale" in h["detail"]


def test_manual_missing_file_is_failed(tmp_path):
    rec = _manual_rec("data/bronze/cro/companies_*.csv", threshold=45)
    h = check_file_age(rec, tmp_path, NOW)
    assert h["status"] == "failed"
    assert h["latest_file"] is None


def test_manual_no_threshold_is_warning(tmp_path):
    _touch(tmp_path / "data/bronze/charities/public_register_2026.xlsx", days_ago=200)
    rec = _manual_rec("data/bronze/charities/public_register_*.xlsx", threshold=None)
    h = check_file_age(rec, tmp_path, NOW)
    assert h["status"] == "warning"
    assert h["days_old"] == 200  # age still reported for visibility


def test_manual_picks_newest_match(tmp_path):
    _touch(tmp_path / "data/bronze/cro/companies_old.csv", days_ago=90)
    _touch(tmp_path / "data/bronze/cro/companies_new.csv", days_ago=5)
    rec = _manual_rec("data/bronze/cro/companies_*.csv", threshold=45)
    h = check_file_age(rec, tmp_path, NOW)
    assert h["status"] == "ok"
    assert h["latest_file"].endswith("companies_new.csv")


def test_online_sources_skipped_when_links_disabled(tmp_path):
    records = [
        {"source_id": "oireachtas_pdfs:payments", "group": "oireachtas_pdfs",
         "check_type": "index_poll", "pollable": True, "listing_url": "https://o.ie"},
        {"source_id": "afs_amalgamated:2023", "group": "afs_amalgamated",
         "check_type": "fixed_file", "pollable": True,
         "direct_files": ["https://gov.ie/a.pdf"]},
    ]
    payload = run(records=records, check_links=False, root=tmp_path, now=NOW)
    assert payload["links_checked"] is False
    assert all(h["status"] == "skipped" for h in payload["sources"])
    assert payload["summary"]["sources_skipped"] == 2
    assert payload["summary"]["sources_failed"] == 0


def test_run_summary_counts_and_stale(tmp_path):
    _touch(tmp_path / "data/bronze/cro/companies_2025.csv", days_ago=60)
    records = [
        _manual_rec("data/bronze/cro/companies_*.csv", threshold=45),  # stale -> failed
        {"source_id": "oireachtas_pdfs:x", "group": "oireachtas_pdfs",
         "check_type": "index_poll", "pollable": True, "listing_url": "https://o.ie"},
    ]
    payload = run(records=records, check_links=False, root=tmp_path, now=NOW)
    s = payload["summary"]
    assert s["sources_failed"] == 1
    assert s["stale_sources"] == 1
    assert s["sources_skipped"] == 1
    assert s["sources_checked"] == 2
