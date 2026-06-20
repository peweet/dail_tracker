"""Unit tests for the attendance metric derivations in dail_tracker_core.attendance.

These lock the *meaning* of the two distinct denominators that have repeatedly
been conflated on the attendance page:

  1. Plenary SITTING days the chamber sat that year  → the rate denominator.
  2. The statutory 120-day TAA minimum                → the compliance threshold.

The recurring bug ("page shows 82 sitting days but a member has 94 recorded",
"a member's combined total compared against a sitting-only denominator") comes
from mixing these. Every function below is pure and total, so a regression here
fails loudly and instantly instead of surfacing as a wrong number in the UI.
"""

from __future__ import annotations

import math

import pytest

from dail_tracker_core.attendance import (
    TAA_ATTENDANCE_BASIS_DAYS,
    TAA_FULL_ATTENDANCE_MINIMUM_DAYS,
    attendance_year_metrics,
    days_below_minimum,
    meets_taa_minimum,
    plenary_attendance_rate,
    statutory_attendance_minimum,
)

# ── Statutory constant sanity ────────────────────────────────────────────────


def test_statutory_constants_are_the_documented_figures():
    # If anyone "tidies" these to round numbers the citation no longer holds.
    assert TAA_FULL_ATTENDANCE_MINIMUM_DAYS == 120
    assert TAA_ATTENDANCE_BASIS_DAYS == 150


@pytest.mark.parametrize("year", [None, 2020, 2023, 2024, 2025, 2026, 2099])
def test_statutory_minimum_is_120_for_every_known_year(year):
    assert statutory_attendance_minimum(year) == 120


def test_statutory_minimum_accepts_year_like_strings_via_int():
    # The page sometimes carries year as a numpy int / str; int() coercion holds.
    assert statutory_attendance_minimum(int("2025")) == 120


# ── meets_taa_minimum ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("total", "expected"),
    [
        (120, True),  # exactly on the threshold counts as met
        (121, True),
        (200, True),
        (119, False),  # one day short — a 1% TAA deduction in reality
        (0, False),
        (None, False),  # missing data is never "met"
    ],
)
def test_meets_minimum_boundary(total, expected):
    assert meets_taa_minimum(total) is expected


def test_meets_minimum_handles_nan_without_raising():
    assert meets_taa_minimum(float("nan")) is False


def test_meets_minimum_uses_combined_total_not_sitting_only():
    # 77 plenary alone fails, but 77 + 69 other = 146 meets the minimum. This is
    # the whole point: the 120 mark is measured against the COMBINED total.
    assert meets_taa_minimum(77) is False
    assert meets_taa_minimum(77 + 69) is True


# ── days_below_minimum ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("total", "expected"),
    [
        (120, 0),
        (130, 0),
        (119, 1),
        (100, 20),
        (0, 120),
        (None, 120),  # no record → full shortfall, not a crash
    ],
)
def test_days_below_minimum(total, expected):
    assert days_below_minimum(total) == expected


def test_days_below_minimum_never_negative():
    assert days_below_minimum(500) == 0


# ── plenary_attendance_rate ──────────────────────────────────────────────────


def test_rate_is_sitting_over_chamber_sitting_days():
    assert plenary_attendance_rate(47, 94) == pytest.approx(0.5)


def test_rate_full_attendance_is_one():
    assert plenary_attendance_rate(94, 94) == pytest.approx(1.0)


@pytest.mark.parametrize("denom", [None, 0, -3, float("nan")])
def test_rate_is_none_when_denominator_unusable(denom):
    assert plenary_attendance_rate(50, denom) is None


@pytest.mark.parametrize("sitting", [None, float("nan")])
def test_rate_is_none_when_sitting_missing(sitting):
    assert plenary_attendance_rate(sitting, 94) is None


def test_rate_exceeds_one_only_when_denominator_is_wrong():
    """The signature of the historic bug: a sitting-only denominator SMALLER than
    the member's own recorded sitting days pushes the rate above 100%.

    With the correct data-derived denominator (94 for 2025) the rate is sane; the
    old stale config denominator (82) would have produced >100% for a high
    attender. This test documents that the function faithfully reports the
    impossible ratio — the data-consistency tests are what guarantee the page
    never *feeds* it a too-small denominator.
    """
    assert plenary_attendance_rate(90, 94) < 1.0  # correct denominator → sane
    assert plenary_attendance_rate(90, 82) > 1.0  # stale denominator → impossible


# ── attendance_year_metrics (the bundle the UI renders) ──────────────────────


def test_year_metrics_combines_and_separates_day_types():
    m = attendance_year_metrics(year=2025, sitting_days=77, other_days=43, chamber_sitting_days=94)
    assert m.sitting_days == 77
    assert m.other_days == 43
    assert m.total_days == 120  # sitting + other
    assert m.chamber_sitting_days == 94
    assert m.plenary_rate == pytest.approx(77 / 94)
    assert m.statutory_minimum == 120
    assert m.meets_minimum is True  # 120 >= 120
    assert m.days_below_minimum == 0


def test_year_metrics_below_minimum():
    m = attendance_year_metrics(year=2025, sitting_days=40, other_days=30, chamber_sitting_days=94)
    assert m.total_days == 70
    assert m.meets_minimum is False
    assert m.days_below_minimum == 50


def test_year_metrics_coerces_none_and_nan_day_counts_to_zero():
    m = attendance_year_metrics(year=2024, sitting_days=None, other_days=float("nan"), chamber_sitting_days=83)
    assert m.sitting_days == 0
    assert m.other_days == 0
    assert m.total_days == 0
    assert m.plenary_rate == pytest.approx(0.0)
    assert m.meets_minimum is False


def test_year_metrics_missing_denominator_yields_none_rate_not_crash():
    m = attendance_year_metrics(year=2025, sitting_days=50, other_days=10, chamber_sitting_days=None)
    assert m.chamber_sitting_days is None
    assert m.plenary_rate is None
    # Compliance is independent of the (missing) plenary denominator.
    assert m.total_days == 60
    assert m.meets_minimum is False


def test_year_metrics_rate_never_nan():
    m = attendance_year_metrics(year=2025, sitting_days=0, other_days=0, chamber_sitting_days=0)
    # denom 0 → None, not NaN (NaN would render as an ugly bar/percentage).
    assert m.plenary_rate is None
    assert not (m.plenary_rate is not None and math.isnan(m.plenary_rate))
