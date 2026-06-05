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
