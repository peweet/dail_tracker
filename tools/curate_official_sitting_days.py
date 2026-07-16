"""Curate the OFFICIAL number of chamber sitting days per year, from the source.

Why this exists: the per-member attendance "rate" needs a denominator — how many
days the chamber actually sat that year. That figure has been a recurring source
of bugs (a hand-typed ``config.SITTING_DAYS_BY_YEAR`` drifted to ``2023: 100``
when the real figure is 97; an extractor bug truncated member sitting days; the
"82 vs 94" mismatch). Each time, the fix was undone by the next change because
there was no committed, source-derived ground truth to test against.

This tool produces that ground truth. For each chamber it reads the Travel &
Accommodation Allowance "Verification of Attendance" PDFs (the same bronze source
the attendance pipeline parses) and records, per calendar year, the number of
distinct SITTING dates the chamber sat — the union of every member's left-column
(sitting) dates. Because the Dáil/Seanad cannot sit with nobody present, that
union is exactly the chamber's sitting-day count, and it equals the PDFs' own
printed "Total number of sitting days in the period" on single-term full-year
reports (cross-checked here and logged).

Output: ``data/_meta/official_sitting_days.csv`` (committed). The guard test
``test/pipeline/test_attendance_official_sitting_days.py`` asserts the LIVE
data-derived view still equals this committed reference for completed years, and
that no member's recorded sitting days exceed it — so any future regression that
drops or inflates sitting dates fails CI instead of silently shipping.

Run:  python -m tools.curate_official_sitting_days
"""

from __future__ import annotations

import contextlib
import datetime as dt
import logging
from collections import defaultdict
from pathlib import Path

import fitz
import polars as pl

import attendance.attendance as att
from config import ATTENDANCE_PDF_DIR, ATTENDANCE_PDF_DIR_SEANAD
from services.logging_setup import setup_standalone_logging

_log = logging.getLogger("curate_official_sitting_days")

_OUT = Path("data/_meta/official_sitting_days.csv")

# A year is "complete" only once a PDF covers attendance through that December —
# otherwise the count is a partial running total of an in-progress year.
_PRINTED_TOTAL_MARK = "Total number of sitting days in the period"


def _distinct_sitting_dates_by_year(pdf_dir: Path) -> dict[int, set[dt.datetime]]:
    """{year: set of distinct chamber sitting dates} from the union of all members."""
    by_year: dict[int, set[dt.datetime]] = defaultdict(set)
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        if att._ATTENDANCE_PDF_MARKER not in pdf_path.name.lower():
            continue
        for _ident, _fn, _ln, _text, kind, iso in att._extract_pdf_member_dates(fitz.open(str(pdf_path))):
            if kind == "sitting":
                by_year[int(iso.year)].add(iso)
    return by_year


def _printed_period_total_cross_check(pdf_dir: Path) -> dict[str, int]:
    """{period_range: max printed 'Total number of sitting days in the period'}.

    The printed total is per member's membership window; its MAX over a full-year
    single-term PDF is the chamber figure. Used only to corroborate the union
    count in the log — not written to the CSV.
    """
    out: dict[str, int] = {}
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        if att._ATTENDANCE_PDF_MARKER not in pdf_path.name.lower():
            continue
        vals: list[int] = []
        doc = fitz.open(str(pdf_path))
        for page in doc:
            lines = [ln.strip() for ln in page.get_text("text").split("\n")]
            for i, ln in enumerate(lines):
                if ln == _PRINTED_TOTAL_MARK and i + 1 < len(lines):
                    with contextlib.suppress(ValueError):
                        vals.append(int(lines[i + 1]))
        if vals:
            out[pdf_path.name] = max(vals)
    return out


def _year_complete(pdf_dir: Path, year: int) -> bool:
    """True iff some verification PDF covers attendance through December of ``year``."""
    for pdf_path in pdf_dir.glob("*.pdf"):
        name = pdf_path.name.lower()
        if att._ATTENDANCE_PDF_MARKER not in name:
            continue
        # filename range ends e.g. "...to-31-december-2023_en.pdf" / "...to-30-december-2025..."
        if f"december-{year}" in name:
            return True
    return False


def curate() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for house, pdf_dir in (("Dáil", ATTENDANCE_PDF_DIR), ("Seanad", ATTENDANCE_PDF_DIR_SEANAD)):
        by_year = _distinct_sitting_dates_by_year(pdf_dir)
        cross = _printed_period_total_cross_check(pdf_dir)
        _log.info("%s printed-total cross-check (per PDF max): %s", house, cross)
        for year in sorted(by_year):
            complete = _year_complete(pdf_dir, year)
            rows.append(
                {
                    "house": house,
                    "year": year,
                    "official_sitting_days": len(by_year[year]),
                    "complete_year": complete,
                    "first_sitting_date": min(by_year[year]).date().isoformat(),
                    "last_sitting_date": max(by_year[year]).date().isoformat(),
                    "source": "TAA Verification of Attendance PDFs (union of distinct sitting dates)",
                }
            )
            _log.info(
                "%s %d: %d sitting days (%s)",
                house,
                year,
                len(by_year[year]),
                "complete" if complete else "PARTIAL/in-progress",
            )
    df = pl.DataFrame(rows).sort(["house", "year"])
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    # complete_year is written as True/False (pandas casing) to keep the committed
    # reference byte-stable across the pandas->polars engine swap.
    df.with_columns(
        pl.when(pl.col("complete_year")).then(pl.lit("True")).otherwise(pl.lit("False")).alias("complete_year")
    ).write_csv(_OUT)
    _log.info("Wrote %s (%d rows)", _OUT, len(df))
    return df


if __name__ == "__main__":
    setup_standalone_logging("curate_official_sitting_days")
    with pl.Config(tbl_rows=-1, tbl_cols=-1):
        print(curate())
