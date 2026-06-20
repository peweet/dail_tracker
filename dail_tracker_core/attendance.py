"""Pure attendance-ranking derivation — Streamlit-free.

Extracted from ``utility/pages_code/attendance.py::_render_good_bad`` so the
"ministers are excluded from the lowest-attendance list" rule is a single,
unit-tested function instead of a one-liner buried in a Streamlit render path.

Why the minister rule is a *contract*, not presentation: the source Travel &
Accommodation Allowance (TAA) PDFs do not record ministerial attendance, so a
minister's low ``attended_count`` is a data artefact, not a fact. Showing a
minister in a "lowest recorded attendance" ranking would be misleading and
unfair. ANY interface (Streamlit today, FastAPI/React later) must apply the same
exclusion — which is exactly why it belongs below the UI, here.

Input is the DataFrame returned by ``attendance_data.fetch_year_ranking`` (one
row per member for a single (year, house), with columns ``member_name,
party_name, constituency, attended_count, is_minister, rank_high, rank_low``).
Output is the two presentation slices. No streamlit/duckdb import; no date or IO.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pandas as pd

_MINISTER_TRUE = "true"


# ── Statutory TAA attendance thresholds ───────────────────────────────────────
# The source PDFs are the Travel & Accommodation Allowance (TAA) verification
# records. A TD (NOT the Taoiseach or Ministers — they are excluded, which is
# why is_minister rows are dropped from the lowest list and from the source
# entirely) must personally attend Leinster House on at least 120 days in a year
# to retain the FULL TAA; the allowance is calculated on a 150-day basis, with a
# 1% deduction for each day attended below 120. BOTH the "sitting days" and the
# "other days" columns are days physically present at Leinster House, so both
# count toward the 120-day total — which is exactly why so many members cluster
# at total_days == 120 (they attend to the threshold).
#
# Source: Houses of the Oireachtas — Salaries & Allowances / Travel and
# Accommodation Allowance (oireachtas.ie/en/members/salaries-and-allowances/).
# Corroborated by the RTÉ Investigations Unit attendance analysis (2019).
#
# These are NOT the same as the number of plenary SITTING DAYS the Dáil sat in a
# year (config.SITTING_DAYS_BY_YEAR / the data-derived chamber count) — that is a
# separate denominator used for the plenary-attendance *rate*. Conflating the two
# (showing a sitting+other total against a sitting-only denominator) is the bug
# this module's metrics exist to prevent.
TAA_FULL_ATTENDANCE_MINIMUM_DAYS = 120
TAA_ATTENDANCE_BASIS_DAYS = 150

# Per-year overrides for the statutory minimum, should it ever change. Empty
# today (120 has applied across every year in the dataset); structured so a
# future change is a one-line edit rather than a refactor. The user flagged that
# the mark "varies over time" — this is where a changed year goes.
_STATUTORY_MINIMUM_OVERRIDES: dict[int, int] = {}


def statutory_attendance_minimum(year: int | None = None) -> int:
    """Minimum days a TD must attend Leinster House to keep the FULL TAA.

    120 for every year in the dataset (see module-level citation). ``year`` is
    accepted so a future per-year change is local to ``_STATUTORY_MINIMUM_OVERRIDES``.
    """
    if year is None:
        return TAA_FULL_ATTENDANCE_MINIMUM_DAYS
    return _STATUTORY_MINIMUM_OVERRIDES.get(int(year), TAA_FULL_ATTENDANCE_MINIMUM_DAYS)


def meets_taa_minimum(total_days: float | int | None, year: int | None = None) -> bool:
    """True iff total recorded attendance (sitting + other) meets the statutory
    minimum. ``None``/``NaN`` total_days reads as 0 (not met), never raises."""
    if total_days is None or (isinstance(total_days, float) and pd.isna(total_days)):
        return False
    return int(total_days) >= statutory_attendance_minimum(year)


def days_below_minimum(total_days: float | int | None, year: int | None = None) -> int:
    """Days short of the statutory minimum (0 if met). ``None``/``NaN`` -> full
    shortfall. Each day here is a 1% TAA deduction under the regulations."""
    minimum = statutory_attendance_minimum(year)
    if total_days is None or (isinstance(total_days, float) and pd.isna(total_days)):
        return minimum
    return max(0, minimum - int(total_days))


def plenary_attendance_rate(
    sitting_days: float | int | None,
    chamber_sitting_days: float | int | None,
) -> float | None:
    """Fraction of the year's plenary sitting days the member was recorded present.

    Numerator and denominator are BOTH plenary-only (chamber sitting days), so the
    result is in [0, 1] even though a member's headline ``total_days`` includes
    committee/other days. Returns ``None`` when the denominator is missing or
    non-positive (the page renders an em-dash rather than a divide-by-zero).

    Critically, the denominator must be the data-derived count of distinct sitting
    dates (or an official figure that is >= it). A denominator SMALLER than the
    member's own sitting_days (the historic "82 scheduled days vs 94 recorded"
    bug) would push the rate above 100% — guarded by the data-consistency tests.
    """
    if sitting_days is None or chamber_sitting_days is None:
        return None
    if isinstance(sitting_days, float) and pd.isna(sitting_days):
        return None
    if isinstance(chamber_sitting_days, float) and pd.isna(chamber_sitting_days):
        return None
    denom = int(chamber_sitting_days)
    if denom <= 0:
        return None
    return int(sitting_days) / denom


@dataclass(frozen=True)
class AttendanceYearMetrics:
    """All derived attendance figures for one (member, year), ready to render.

    Bundles the two distinct day-types kept separate (``sitting_days`` plenary vs
    ``other_days`` committee/other) plus the two distinct denominators they map to
    (the plenary ``chamber_sitting_days`` for the rate, and the statutory
    ``statutory_minimum`` for TAA compliance). Pure data — no Streamlit, no IO.
    """

    year: int
    sitting_days: int
    other_days: int
    total_days: int
    chamber_sitting_days: int | None
    plenary_rate: float | None
    statutory_minimum: int
    meets_minimum: bool
    days_below_minimum: int


def attendance_year_metrics(
    *,
    year: int,
    sitting_days: float | int | None,
    other_days: float | int | None,
    chamber_sitting_days: float | int | None = None,
) -> AttendanceYearMetrics:
    """Derive the full per-(member, year) attendance metric set.

    ``total_days = sitting_days + other_days`` (the figure the 120-day statutory
    minimum applies to). The plenary rate uses ``sitting_days`` against
    ``chamber_sitting_days`` only. None/NaN day counts coerce to 0.
    """
    s = 0 if sitting_days is None or (isinstance(sitting_days, float) and pd.isna(sitting_days)) else int(sitting_days)
    o = 0 if other_days is None or (isinstance(other_days, float) and pd.isna(other_days)) else int(other_days)
    total = s + o
    denom = (
        None
        if chamber_sitting_days is None or (isinstance(chamber_sitting_days, float) and pd.isna(chamber_sitting_days))
        else int(chamber_sitting_days)
    )
    return AttendanceYearMetrics(
        year=int(year),
        sitting_days=s,
        other_days=o,
        total_days=total,
        chamber_sitting_days=denom,
        plenary_rate=plenary_attendance_rate(s, denom),
        statutory_minimum=statutory_attendance_minimum(year),
        meets_minimum=meets_taa_minimum(total, year),
        days_below_minimum=days_below_minimum(total, year),
    )


def is_minister_mask(col: pd.Series) -> pd.Series:
    """Boolean mask: ``True`` where the row is a cabinet / junior minister.

    Mirrors the historical page rule *exactly*: a row counts as a minister iff
    ``str(is_minister).lower() == "true"``. So a real boolean ``True`` and the
    strings ``"True"/"TRUE"/"true"`` are ministers; ``NULL``/``NaN`` and any
    other value (including the integers ``0``/``1``) are treated as NON-ministers
    and stay eligible for the lowest list.

    That last point is a deliberately preserved sharp edge — if the upstream
    view ever emits ``is_minister`` as 0/1 instead of bool/text, ministers would
    silently reappear in the shame list. Pinned by
    ``test_numeric_is_minister_is_not_excluded`` so the regression is loud.
    """
    return col.astype(str).str.lower() == _MINISTER_TRUE


@dataclass(frozen=True)
class AttendanceHall:
    """The two ranked slices the page renders side by side.

    ``highest`` keeps ministers (their high attendance is genuine); ``lowest``
    excludes them (see module docstring).
    """

    highest: pd.DataFrame
    lowest: pd.DataFrame


def split_attendance_hall(ranking_df: pd.DataFrame, *, hall_size: int) -> AttendanceHall:
    """Split one year's ranking into the top-N highest and bottom-N lowest.

    - ``highest``: ordered by ``rank_high`` asc then ``attended_count`` desc,
      capped at ``hall_size``. Ministers included.
    - ``lowest``: ministers removed, then ordered by ``rank_low`` asc then
      ``attended_count`` asc, capped at ``hall_size``.

    Pure and total: an empty (but correctly-typed) input yields two empty frames.
    Index is reset on both slices to match the original render contract.
    """
    highest = (
        ranking_df.sort_values(["rank_high", "attended_count"], ascending=[True, False])
        .head(hall_size)
        .reset_index(drop=True)
    )
    # casts are type-only (pandas stubs widen __getitem__ to Series|DataFrame);
    # they document the real runtime types and keep basedpyright clean. Behaviour
    # is unchanged — locked by the parity tests in test_core_attendance_hall.py.
    minister_col = cast("pd.Series", ranking_df["is_minister"])
    non_ministers = cast("pd.DataFrame", ranking_df[~is_minister_mask(minister_col)])
    lowest = (
        non_ministers.sort_values(["rank_low", "attended_count"], ascending=[True, True])
        .head(hall_size)
        .reset_index(drop=True)
    )
    return AttendanceHall(highest=highest, lowest=lowest)
