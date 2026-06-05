"""Unit tests for dail_tracker_core.attendance.split_attendance_hall.

These lock the fairness-critical "ministers excluded from the lowest list" rule
that used to live untested inside utility/pages_code/attendance.py::_render_good_bad,
and prove byte-for-byte parity with the original inline expression.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dail_tracker_core.attendance import is_minister_mask, split_attendance_hall

_COLS = ["member_name", "party_name", "constituency", "attended_count", "is_minister", "rank_high", "rank_low"]


def _row(name, attended, is_min, rank_high, rank_low, party="PartyX", const="ConstY"):
    return {
        "member_name": name,
        "party_name": party,
        "constituency": const,
        "attended_count": attended,
        "is_minister": is_min,
        "rank_high": rank_high,
        "rank_low": rank_low,
    }


@pytest.fixture
def ranking_df() -> pd.DataFrame:
    # rank_high: 1 = highest attendance; rank_low: 1 = lowest attendance.
    return pd.DataFrame(
        [
            _row("Alice", 95, False, 1, 6),   # top attender
            _row("Bob", 90, True, 2, 5),      # a MINISTER with high attendance
            _row("Carol", 50, False, 3, 4),
            _row("Dana", 20, True, 4, 2),     # a MINISTER with low attendance (must NOT show in lowest)
            _row("Eve", 10, False, 5, 1),     # genuine lowest attender
        ],
        columns=_COLS,
    )


def _legacy_split(ranking_df: pd.DataFrame, hall_size: int):
    """The exact expression that lived in attendance.py before extraction."""
    top = (
        ranking_df.sort_values(["rank_high", "attended_count"], ascending=[True, False])
        .head(hall_size)
        .reset_index(drop=True)
    )
    non_ministers = ranking_df[ranking_df["is_minister"].astype(str).str.lower() != "true"]
    bottom = (
        non_ministers.sort_values(["rank_low", "attended_count"], ascending=[True, True])
        .head(hall_size)
        .reset_index(drop=True)
    )
    return top, bottom


def test_parity_with_legacy_inline_expression(ranking_df):
    """The new function reproduces the old page code exactly."""
    legacy_top, legacy_bottom = _legacy_split(ranking_df, 15)
    hall = split_attendance_hall(ranking_df, hall_size=15)
    pd.testing.assert_frame_equal(hall.highest, legacy_top)
    pd.testing.assert_frame_equal(hall.lowest, legacy_bottom)


def test_highest_includes_ministers(ranking_df):
    hall = split_attendance_hall(ranking_df, hall_size=15)
    assert "Bob" in set(hall.highest["member_name"])  # minister, high attendance — kept


def test_lowest_excludes_ministers(ranking_df):
    hall = split_attendance_hall(ranking_df, hall_size=15)
    names = set(hall.lowest["member_name"])
    assert "Dana" not in names  # minister with low attendance — must be excluded
    assert "Eve" in names       # genuine lowest attender — present


def test_lowest_is_ordered_by_rank_low(ranking_df):
    hall = split_attendance_hall(ranking_df, hall_size=15)
    assert list(hall.lowest["member_name"]) == ["Eve", "Carol", "Alice"]


def test_highest_is_ordered_by_rank_high(ranking_df):
    hall = split_attendance_hall(ranking_df, hall_size=15)
    assert list(hall.highest["member_name"]) == ["Alice", "Bob", "Carol", "Dana", "Eve"]


def test_hall_size_caps_both_slices(ranking_df):
    hall = split_attendance_hall(ranking_df, hall_size=2)
    assert len(hall.highest) == 2
    assert len(hall.lowest) == 2
    assert list(hall.highest["member_name"]) == ["Alice", "Bob"]
    assert list(hall.lowest["member_name"]) == ["Eve", "Carol"]


@pytest.mark.parametrize("value", [True, "True", "TRUE", "true"])
def test_string_and_bool_true_are_ministers(value):
    s = pd.Series([value])
    assert bool(is_minister_mask(s).iloc[0]) is True


@pytest.mark.parametrize("value", [False, "False", None, float("nan"), "", "no"])
def test_falsey_and_null_are_not_ministers(value):
    s = pd.Series([value])
    assert bool(is_minister_mask(s).iloc[0]) is False


def test_numeric_is_minister_is_not_excluded():
    """Documents the sharp edge: integer 1 is NOT treated as a minister.

    If the upstream view ever switches is_minister to 0/1, this test fails loudly
    rather than letting ministers silently reappear in the lowest list.
    """
    s = pd.Series([1])
    assert bool(is_minister_mask(s).iloc[0]) is False


def test_empty_input_yields_empty_slices():
    empty = pd.DataFrame(columns=_COLS)
    hall = split_attendance_hall(empty, hall_size=15)
    assert hall.highest.empty
    assert hall.lowest.empty
