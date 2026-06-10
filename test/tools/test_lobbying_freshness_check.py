"""Unit tests for tools/lobbying_freshness_check.py.

Pure unit tests (no marker, default CI lane). The network fetch and parquet read
are isolated in fetch_recent_periods/held_period_start; these tests drive the pure
period-parsing and verdict logic, plus the real upstream Period label format.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.lobbying_freshness_check import (  # noqa: E402
    latest_period_start,
    parse_period_start,
    verdict,
)


@pytest.mark.parametrize(
    "label,expected",
    [
        ("1 May, 2026 to 31 Aug, 2026", date(2026, 5, 1)),
        ("1 Jan, 2026 to 30 Apr, 2026", date(2026, 1, 1)),
        ("1 Sep, 2025 to 31 Dec, 2025", date(2025, 9, 1)),
        ("15 May, 2026 to 31 Aug, 2026", date(2026, 5, 15)),  # 2-digit day
    ],
)
def test_parse_period_start_real_formats(label, expected):
    assert parse_period_start(label) == expected


@pytest.mark.parametrize("bad", ["", "garbage", "Q1 2026", "2026-05-01 to 2026-08-31"])
def test_parse_period_start_unparseable_is_none(bad):
    assert parse_period_start(bad) is None


def test_latest_period_start_picks_max():
    labels = [
        "1 Jan, 2026 to 30 Apr, 2026",
        "1 May, 2026 to 31 Aug, 2026",
        "1 Sep, 2025 to 31 Dec, 2025",
    ]
    assert latest_period_start(labels) == date(2026, 5, 1)


def test_latest_period_start_all_unparseable_is_none():
    assert latest_period_start(["", "garbage"]) is None


def test_verdict_current_when_equal():
    """The live state today: upstream == held == 1 May 2026 -> current (exit 0)."""
    code, msg = verdict(date(2026, 5, 1), date(2026, 5, 1), slack_days=14)
    assert code == 0
    assert "OK" in msg


def test_verdict_stale_when_new_period_upstream():
    """A whole new period (next start Sep 1) appears upstream -> stale (exit 1)."""
    code, msg = verdict(date(2026, 9, 1), date(2026, 5, 1), slack_days=14)
    assert code == 1
    assert "STALE" in msg


def test_verdict_slack_absorbs_small_lead():
    """Upstream a few days ahead within slack is not flagged."""
    code, _ = verdict(date(2026, 5, 10), date(2026, 5, 1), slack_days=14)
    assert code == 0


def test_verdict_slack_boundary_is_inclusive():
    """gap == slack is OK; strictly greater is stale."""
    assert verdict(date(2026, 5, 15), date(2026, 5, 1), slack_days=14)[0] == 0
    assert verdict(date(2026, 5, 16), date(2026, 5, 1), slack_days=14)[0] == 1


def test_verdict_could_not_check_on_missing_held():
    code, _ = verdict(date(2026, 5, 1), None, slack_days=14)
    assert code == 2


def test_verdict_could_not_check_on_missing_upstream():
    code, _ = verdict(None, date(2026, 5, 1), slack_days=14)
    assert code == 2
