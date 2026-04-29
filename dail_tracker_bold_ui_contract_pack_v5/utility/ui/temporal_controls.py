from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence

import streamlit as st


@dataclass(frozen=True)
class DateRange:
    start: date | None
    end: date | None


def date_range_filter(
    label: str,
    min_date: date | None,
    max_date: date | None,
    *,
    default_start: date | None = None,
    default_end: date | None = None,
    help_text: str | None = None,
) -> DateRange:
    value = st.date_input(
        label,
        value=(default_start or min_date, default_end or max_date),
        min_value=min_date,
        max_value=max_date,
        help=help_text,
    )
    if isinstance(value, tuple) and len(value) == 2:
        return DateRange(start=value[0], end=value[1])
    return DateRange(start=None, end=None)


def year_selector(
    label: str,
    years: Sequence[int],
    *,
    default: int | None = None,
    allow_multiple: bool = False,
):
    years = sorted({int(y) for y in years}, reverse=True)
    if not years:
        st.warning("No years available.")
        return [] if allow_multiple else None

    default_year = default if default in years else years[0]

    if allow_multiple:
        if len(years) <= 8:
            return st.multiselect(label, years, default=[default_year])
        start, end = st.select_slider(label, options=sorted(years), value=(min(years), max(years)))
        return [y for y in sorted(years) if start <= y <= end]

    if len(years) <= 6:
        return st.radio(label, years, index=years.index(default_year), horizontal=True)

    return st.selectbox(label, years, index=years.index(default_year))
