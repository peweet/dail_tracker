"""Unit tests for the pure staleness helpers in tools/check_freshness.py.

These lock the rule that distinguishes "fresh", "stale", and "can't say": a
threshold-less or age-less dataset is never marked stale (it stays informational
or 'unavailable'), and only a strictly-exceeded threshold flips to stale. The
fetch-age path is what makes the canary recess-immune (a Dáil recess stops new
records, not our fetching).
"""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.check_freshness import _is_stale, _mtime_age


def test_is_stale_true_when_age_exceeds_threshold():
    assert _is_stale(34, 14) is True


def test_is_stale_false_at_or_below_threshold():
    assert _is_stale(14, 14) is False
    assert _is_stale(2, 14) is False


def test_is_stale_false_without_threshold():
    """A threshold-less dataset (e.g. quarterly lobbying) is never stale."""
    assert _is_stale(999, None) is False


def test_is_stale_false_without_age():
    """No age (missing file) is 'unavailable', handled elsewhere — not stale."""
    assert _is_stale(None, 14) is False


def test_mtime_age_missing_file_is_none():
    assert _mtime_age(Path("nope/missing.json"), date(2026, 6, 6)) == (None, None)


def test_mtime_age_reports_recent_file(tmp_path):
    p = tmp_path / "f.json"
    p.write_text("{}", encoding="utf-8")
    iso, age = _mtime_age(p, datetime.now(UTC).date())
    assert age == 0
    assert iso and iso.endswith("Z")


def test_mtime_age_counts_days_back(tmp_path):
    p = tmp_path / "f.json"
    p.write_text("{}", encoding="utf-8")
    import os

    past = (datetime.now(UTC) - timedelta(days=20)).timestamp()
    os.utime(p, (past, past))
    _, age = _mtime_age(p, datetime.now(UTC).date())
    assert age in (19, 20)  # tolerate same-day rounding
