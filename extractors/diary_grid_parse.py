"""Geometry-aware parser for SCANNED weekly-calendar diary pages (Outlook week view).

The DPER / Taoiseach / older-DCCS diaries are image scans of an Outlook WEEK grid, not
the daily lists the born-digital state machine reads — so they need column geometry, not
line order. Layout (landscape A4, from the OCR cell coords):
  * a week header  "4 - 8 January 2021"  -> the Monday date + month/year
  * a row of weekday headers MONDAY..FRIDAY -> the day COLUMNS (x-bands)
  * a left time gutter (hour numbers) -> approximate time-of-day from a cell's y
  * event text sits in (day-column x, time y) cells; one engagement = a vertical cluster

This maps each event cell to its day (by x-column) and a coarse hour (by y vs the gutter),
groups vertically-adjacent cells into one engagement, and emits the same
(entry_date, time_slot, subject) rows as the born-digital parser so OCR'd scans flow
through classify -> match -> overlap -> promote unchanged.

Works on CACHED OCR cells ([[project_sipo_ocr]] lesson: cache cells so parser fixes need
no re-OCR). Input = list of pages, each a list of {t,x0,y0,x1,y1}.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

_WEEKDAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
_MONTHS = {
    m: i + 1
    for i, m in enumerate(
        ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
    )
}
_MONTHS |= {m[:3]: n for m, n in list(_MONTHS.items())}

# Week header — two real layouts, both yielding (day1, month1, year):
#   single-month "4 - 8 January 2021"   → day-dash-day month year
#   cross-month  "28 June - 2 July 2021" → day month - day month year (week spans a month boundary)
_WEEK_HDR = re.compile(r"(\d{1,2})\s*[-–]\s*\d{1,2}\s+([A-Za-z]{3,9})\s+(\d{4})")
_WEEK_HDR_X = re.compile(r"(\d{1,2})\s+([A-Za-z]{3,9})\s*[-–]\s*\d{1,2}\s+[A-Za-z]{3,9}\s+(\d{4})")
_EVENT_GAP = 70  # px: cells within this vertical gap in a column = one engagement block
_VENUE_RE = re.compile(r"^(online|microsoft teams.*|zoom.*|https?://.*|teams meeting|phone|video call)$", re.IGNORECASE)
# repeated page/column chrome from the Outlook export (footer/header watermark) — drop, not an engagement.
# Includes the all-caps "DFIN Diary" department-code header (case-sensitive code so it can't eat a
# real mixed-case "Press Diary" subject).
_CHROME_RE = re.compile(
    r"^(minister of state.*|search minister.*|.*\(ctrl\s*\+\s*e\).*|.*- calendar|(?-i:[A-Z]{2,6}) diary)$",
    re.IGNORECASE,
)


def _norm(t: str) -> str:
    return re.sub(r"[^a-z]", "", t.lower())


def _week_start(cells: list[dict], year_hint: int | None) -> date | None:
    for c in sorted(cells, key=lambda c: c["y0"]):  # header is near the top
        m = _WEEK_HDR.search(c["t"]) or _WEEK_HDR_X.search(c["t"])
        if m:
            mon = _MONTHS.get(m.group(2).lower()[:3])
            if mon:
                try:
                    return date(int(m.group(3)), mon, int(m.group(1)))
                except ValueError:
                    return None
    return None


def _columns(cells: list[dict]) -> list[tuple[float, int]]:
    """Day columns as (x_center, column_index) from the weekday-header row, left to right."""
    hdr = [c for c in cells if _norm(c["t"]) in _WEEKDAYS]
    if not hdr:
        return []
    hdr.sort(key=lambda c: c["x0"])
    return [((c["x0"] + c["x1"]) / 2, i) for i, c in enumerate(hdr)]


def _gutter_x(cells: list[dict], cols: list[tuple[float, int]]) -> float:
    """Right edge of the time gutter = just left of the first day column."""
    first_col_x = min(x for x, _ in cols)
    return first_col_x - (cols[1][0] - cols[0][0]) / 2 if len(cols) > 1 else first_col_x - 100


def _hour_markers(cells: list[dict], gutter_x: float) -> list[tuple[int, int]]:
    """(y, hour) from integer cells in the left gutter — OCR splits '10'->'1','0' so this is
    best-effort; used only to stamp a coarse time_slot."""
    out = []
    for c in cells:
        if c["x1"] <= gutter_x and re.fullmatch(r"\d{1,2}", c["t"]):
            h = int(c["t"])
            if 6 <= h <= 21:
                out.append(((c["y0"] + c["y1"]) // 2, h))
    return sorted(out)


def _coarse_time(y: int, markers: list[tuple[int, int]]) -> str:
    if not markers:
        return ""
    best = min(markers, key=lambda m: abs(m[0] - y))
    return f"{best[1]:02d}:00"


def _assign_col(x_center: float, cols: list[tuple[float, int]]) -> int:
    return min(cols, key=lambda c: abs(c[0] - x_center))[1]


def parse_grid_page(cells: list[dict], year_hint: int | None) -> list[dict]:
    start = _week_start(cells, year_hint)
    cols = _columns(cells)
    if start is None or not cols:
        return []  # not a recognisable week grid (caller can fall back to linear)
    gutter_x = _gutter_x(cells, cols)
    markers = _hour_markers(cells, gutter_x)
    header_y = max(c["y1"] for c in cells if _norm(c["t"]) in _WEEKDAYS)

    # event cells = below the weekday header, right of the gutter, not pure noise
    body = [
        c
        for c in cells
        if c["y0"] > header_y
        and (c["x0"] + c["x1"]) / 2 > gutter_x
        and not re.fullmatch(r"\d{1,2}", c["t"])
        and "calendar (ctrl" not in c["t"].lower()
    ]
    # group per column by vertical adjacency
    by_col: dict[int, list[dict]] = {}
    for c in body:
        by_col.setdefault(_assign_col((c["x0"] + c["x1"]) / 2, cols), []).append(c)

    entries: list[dict] = []

    def flush(block: list[dict], day: date) -> None:
        if not block:
            return
        parts = [b["t"] for b in block if not _VENUE_RE.match(b["t"].strip()) and not _CHROME_RE.match(b["t"].strip())]
        subject = re.sub(r"\s+", " ", " ".join(parts)).strip(" -·,")
        if len(subject) >= 3:  # drop single-char / empty OCR noise
            entries.append({"entry_date": day, "time_slot": _coarse_time(block[0]["y0"], markers), "subject": subject})

    for col_idx, ccells in by_col.items():
        day = start + timedelta(days=col_idx)
        ccells.sort(key=lambda c: (c["y0"], c["x0"]))
        block: list[dict] = []
        last_top = None
        for c in ccells:
            if last_top is not None and c["y0"] - last_top > _EVENT_GAP:  # top-to-top gap splits events
                flush(block, day)
                block = []
            block.append(c)
            last_top = c["y0"]
        flush(block, day)
    return entries


def parse_grid(pages: list[list[dict]], year_hint: int | None) -> list[dict]:
    out: list[dict] = []
    for cells in pages:
        out.extend(parse_grid_page(cells, year_hint))
    return out
