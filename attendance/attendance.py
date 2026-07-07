"""
attendance.py — extract TD attendance from Oireachtas PDFs.

Reads bronze PDFs in ATTENDANCE_PDF_DIR, writes:
  - silver/aggregated_td_tables.csv         (raw concatenated tables)
  - silver/td_attendance_fact_table.csv     (per-row with year + counts)
  - silver/parquet/td_attendance_fact_table.parquet

Behaviour:
  - Skipped cleanly (exit 0) when ATTENDANCE_PDF_DIR is empty.
  - If aggregated_td_tables.csv already exists, PDF processing is skipped
    and only the fact table is rebuilt (matches pre-refactor behaviour).
"""

from __future__ import annotations

import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd
from numpy import nan

from config import ATTENDANCE_PDF_DIR, SILVER_DIR
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

DATE_RANGE = re.compile(r"(\d{1,2}-[a-zA-Z]+-\d{4})-to-(\d{1,2}-[a-zA-Z]+-\d{4})")

# ── PDF geometry ──────────────────────────────────────────────────────────────
# The TAA "Verification of Attendance" PDFs lay each member out as two side-by-side
# date columns: "Sitting days" on the left (x0 ≈ 78) and "Other days" on the right
# (x0 ≈ 313). A member's longer (sitting) column overflows onto a *continuation*
# page that carries NO header row — which is exactly why the previous
# find_tables()/to_pandas() approach silently dropped those rows: to_pandas() named
# the headerless columns after their first date value, so _build_silver_csv could
# never map them to ``sitting_days_attendance`` and dropna() discarded them. The
# net effect was sitting days truncated to a single page (~72/yr) for every
# high-attendance member, while the shorter (single-page) "other" column was
# untouched. Validated against the PDFs' own published per-member "Sub-total:"
# figures: x-coordinate assignment reproduces them exactly across both chambers
# and every published period (2023–2026); the old approach got 108/124 wrong for
# 2023 alone. See test/pipeline/test_attendance_extraction.py.
_COLUMN_X_SPLIT = 200.0
_DATE_CELL = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_ALPHA = re.compile(r"[A-Za-zÁÉÍÓÚáéíóú]")
_PERIOD_HEADER = re.compile(r"^(Deputy|Senator),")
_SUBTOTAL = re.compile(r"^Sub-total:\s*(\d+)")
_TOTALS_MARK = "Totals"
# Only the attendance verification PDFs carry the two-column layout; the same
# bronze dir also holds the monthly PSA-payment PDFs, which must be skipped.
_ATTENDANCE_PDF_MARKER = "verification-of-attendance"


def _parse_member_name(name_line: str) -> tuple[str, str, str]:
    """(identifier, first_name, last_name) from a 'Lastname Firstname' header line.

    Mirrors the historical split exactly so the downstream join key is unchanged:
    identifier is the raw header with spaces→underscores; the name is split on the
    FIRST space only (``maxsplit=1``), taking the remainder as first name. The
    normalise step downstream sorts characters, so the first/last allocation does
    not affect matching — but it is preserved to keep the silver schema stable.
    """
    names = name_line.split(maxsplit=1)
    first_name = names[-1] if names else ""
    last_name = " ".join(names[:-1])
    identifier = name_line.replace(" ", "_")
    return identifier, first_name, last_name


def _extract_pdf_member_dates(
    doc: fitz.Document,
) -> list[tuple[str, str, str, str, str, pd.Timestamp]]:
    """One PDF → list of (identifier, first_name, last_name, date_text, kind, iso).

    ``kind`` is ``"sitting"`` (left column, x0 < split) or ``"other"`` (right
    column). Each date is attributed to the member whose period header most
    recently appeared — a member's date list spills onto headerless continuation
    pages, and ``current`` carries across them. Dates sharing a text line with any
    alphabetic token are skipped (the "Date Range …" / "Deputy, … Limit:" lines).
    No de-duplication here; callers dedup as needed.
    """
    out: list[tuple[str, str, str, str, str, pd.Timestamp]] = []
    current: tuple[str, str, str] | None = None
    for page in doc:
        lines = page.get_text("text").split("\n")
        for i, line in enumerate(lines):
            if _PERIOD_HEADER.match(line.strip()) and i >= 1:
                current = _parse_member_name(lines[i - 1].strip())
        if current is None:
            continue
        identifier, first_name, last_name = current
        words = page.get_text("words")  # (x0, y0, x1, y1, text, block, line, word)
        lines_with_alpha = {round(w[1]) for w in words if _ALPHA.search(w[4])}
        for x0, y0, _x1, _y1, text, *_rest in words:
            if not _DATE_CELL.match(text) or round(y0) in lines_with_alpha:
                continue
            iso = pd.to_datetime(text, format="%d/%m/%Y", errors="coerce")
            if pd.isna(iso):
                continue
            kind = "sitting" if x0 < _COLUMN_X_SPLIT else "other"
            out.append((identifier, first_name, last_name, text, kind, iso))
    return out


def _extract_silver_dataframe(pdf_dir: Path) -> tuple[pd.DataFrame, str]:
    """Build the tidy silver attendance frame directly from PDF word geometry.

    One row per (member, distinct date), tagging each date as a sitting day (left
    column) or other day (right column) from its x-coordinate. Dates that share a
    text line with any alphabetic token are skipped — that excises the "Date Range
    01/01/2023 to 31/12/2023" and "Deputy, 33rd Dáil, … Limit: 120" header lines,
    which also contain dates. Cumulative PDFs restate prior days, so rows are
    de-duplicated on (identifier, iso_date, kind) across the whole corpus.

    Returns the silver DataFrame (legacy column schema) and the last date_range
    string parsed from a filename (kept only for the existing log line).
    """
    seen: set[tuple[str, pd.Timestamp, str]] = set()
    records: list[dict[str, object]] = []
    date_range = ""

    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        if _ATTENDANCE_PDF_MARKER not in pdf_path.name.lower():
            continue
        match = re.search(DATE_RANGE, pdf_path.stem.lower())
        date_range = f"{match.group(1)}_to_{match.group(2)}" if match else "unknown"
        for identifier, first_name, last_name, text, kind, iso in _extract_pdf_member_dates(fitz.open(str(pdf_path))):
            key = (identifier, iso, kind)
            if key in seen:  # cumulative PDFs restate prior days — keep one per (member, date, kind)
                continue
            seen.add(key)
            is_sitting = kind == "sitting"
            records.append(
                {
                    "identifier": identifier,
                    "first_name": first_name,
                    "last_name": last_name,
                    "sitting_days_attendance": text if is_sitting else nan,
                    "other_days_attendance": nan if is_sitting else text,
                    "year": int(iso.year),
                    "iso_sitting_days_attendance": iso if is_sitting else pd.NaT,
                    "iso_other_days_attendance": pd.NaT if is_sitting else iso,
                }
            )

    columns = [
        "identifier",
        "first_name",
        "last_name",
        "sitting_days_attendance",
        "other_days_attendance",
        "year",
        "iso_sitting_days_attendance",
        "iso_other_days_attendance",
    ]
    df = pd.DataFrame.from_records(records, columns=columns)
    df["iso_sitting_days_attendance"] = pd.to_datetime(df["iso_sitting_days_attendance"], errors="coerce")
    df["iso_other_days_attendance"] = pd.to_datetime(df["iso_other_days_attendance"], errors="coerce")
    df = df.sort_values(["identifier", "year", "iso_sitting_days_attendance", "iso_other_days_attendance"])
    print(f"Silver attendance: {len(df):,} (member, date) rows from {df['identifier'].nunique():,} members")
    return df, date_range


def _published_totals_for_doc(doc: fitz.Document) -> dict[str, tuple[int, int]]:
    """One PDF → {identifier: (published_sitting, published_other)} from Sub-totals.

    A member entry can hold several office-holder sub-periods, each ending in a
    "Totals" block preceded by two Sub-total lines (sitting, then other). Sum the
    sub-periods, de-duplicating an identical sub-period pair that immediately
    repeats — the PDF restates a period's Sub-total row when its date list and its
    "Totals" block straddle a page break.

    IMPORTANT: this is PER PDF. Each cumulative PDF restates the running total for
    the period it covers, so these figures must NEVER be summed across PDFs — only
    compared to the dates extracted from the SAME PDF.
    """
    totals: dict[str, list[tuple[int, int]]] = defaultdict(list)
    current: str | None = None
    pending: list[int] = []
    for page in doc:
        lines = [ln.strip() for ln in page.get_text("text").split("\n")]
        for i, line in enumerate(lines):
            if _PERIOD_HEADER.match(line) and i >= 1:
                current = lines[i - 1].replace(" ", "_")
            m = _SUBTOTAL.match(line)
            if m:
                pending.append(int(m.group(1)))
            elif line == _TOTALS_MARK and current is not None and len(pending) >= 2:
                pair = (pending[-2], pending[-1])
                if not totals[current] or totals[current][-1] != pair:
                    totals[current].append(pair)
                pending = []
    return {ident: (sum(s for s, _ in pairs), sum(o for _, o in pairs)) for ident, pairs in totals.items()}


def _reconcile_against_published(pdf_dir: Path) -> int:
    """Guard: per PDF, extracted distinct-date counts must equal published Sub-totals.

    For each verification PDF, compares the distinct (sitting, other) dates this
    extractor reads for each member against that PDF's own published ``Sub-total:``
    figures. This is the regression tripwire for the continuation-page truncation
    bug — it cannot pass while sitting days are being dropped on overflow pages.
    Reconciliation is strictly per-PDF: cumulative PDFs restate running totals, so
    cross-PDF summing would be meaningless. Returns the total mismatch count.
    """
    pdfs = 0
    total_mismatches = 0
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        if _ATTENDANCE_PDF_MARKER not in pdf_path.name.lower():
            continue
        doc = fitz.open(str(pdf_path))
        published = _published_totals_for_doc(doc)
        per_member: dict[str, dict[str, set[pd.Timestamp]]] = defaultdict(lambda: {"sitting": set(), "other": set()})
        for identifier, _fn, _ln, _text, kind, iso in _extract_pdf_member_dates(doc):
            per_member[identifier][kind].add(iso)
        for ident, (pub_s, pub_o) in published.items():
            got = per_member.get(ident, {"sitting": set(), "other": set()})
            if len(got["sitting"]) != pub_s or len(got["other"]) != pub_o:
                total_mismatches += 1
                logger.warning(
                    "attendance reconcile MISMATCH %s in %s: published=(%d,%d) extracted=(%d,%d)",
                    ident,
                    pdf_path.name,
                    pub_s,
                    pub_o,
                    len(got["sitting"]),
                    len(got["other"]),
                )
        pdfs += 1
    print(
        f"Attendance reconciliation: {pdfs} PDFs checked against published "
        f"Sub-totals, {total_mismatches} member-mismatch(es)."
    )
    return total_mismatches


def _build_silver_csv(df: pd.DataFrame, csv_path: Path) -> None:
    """Write the already-tidy silver attendance frame to CSV."""
    df.to_csv(csv_path, index=False)


def _build_fact_table(silver_csv: Path, fact_csv: Path, fact_parquet: Path, house: str | None = None) -> None:
    """Re-read silver CSV, compute counts, write fact CSV + parquet.

    Re-read (rather than passing the in-memory df) is intentional — keeps
    dtype coercions to_csv → read_csv applies, so the fact table is
    deterministic across silver-write paths.

    `house`, when given, is tagged onto every row (Senator chain passes "Seanad").
    """
    df = pd.read_csv(silver_csv)

    df["sitting_flag"] = df["iso_sitting_days_attendance"].notna().astype(int)
    df["other_flag"] = df["iso_other_days_attendance"].notna().astype(int)

    # Defensive dedup — silver write already deduped, this is an idempotency
    # check. With clean silver, this should drop 0.
    _before_dedup = len(df)
    df = df.drop_duplicates(subset=["identifier", "iso_sitting_days_attendance", "iso_other_days_attendance"])
    print(f"Attendance dedup: dropped {_before_dedup - len(df):,} duplicate rows (kept {len(df):,})")

    # Counts are of UNIQUE dates per (TD, year). Silver is now one tidy row per
    # (member, date, kind), so nunique == row count per column here — but nunique
    # is kept as the contract: it is the published meaning (distinct sitting /
    # other days) and stays correct if an upstream change ever reintroduces
    # repeated rows.
    df["sitting_days_count"] = df.groupby(["identifier", "year"])["iso_sitting_days_attendance"].transform("nunique")
    df["other_days_count"] = df.groupby(["identifier", "year"])["iso_other_days_attendance"].transform("nunique")

    df["sitting_total_days"] = df["sitting_days_count"] + df["other_days_count"]

    drop_cols = [
        c for c in ["sitting_flag", "other_flag", "sitting_days_attendance", "other_days_attendance"] if c in df.columns
    ]
    if drop_cols:
        df = df.drop(drop_cols, axis=1)
    if house is not None:
        df["house"] = house
    df.to_csv(fact_csv, index=False)

    save_parquet(df, fact_parquet)


def main(
    pdf_dir: Path = ATTENDANCE_PDF_DIR,
    silver_csv: Path | None = None,
    fact_csv: Path | None = None,
    fact_parquet: Path | None = None,
    house: str | None = None,
) -> int:
    """Build silver attendance CSV + fact table from bronze PDFs.

    Defaults reproduce the original Dáil behaviour exactly. The Senator chain
    passes pdf_dir=ATTENDANCE_PDF_DIR_SEANAD + Senator output paths + house=
    "Seanad" to reuse the whole parser unchanged (name detection + date-table
    counting are already chamber-agnostic and term-agnostic).

    Exit codes:
        0 — ok, or skipped cleanly (no PDFs / no tables extracted)
    """
    silver_csv = silver_csv or SILVER_DIR / "aggregated_td_tables.csv"
    fact_csv = fact_csv or SILVER_DIR / "td_attendance_fact_table.csv"
    fact_parquet = fact_parquet or SILVER_DIR / "parquet" / "td_attendance_fact_table.parquet"

    date_range = ""

    if not silver_csv.is_file():
        print("Aggregated attendance tables CSV not found. Starting PDF processing to create it...")
        pdfs_exist = any(pdf_dir.glob("*.pdf"))
        if not pdfs_exist:
            logger.warning(
                "No attendance PDFs in %s and no existing silver CSV — skipping attendance build.",
                pdf_dir,
            )
            print(f"No attendance PDFs in {pdf_dir} — skipping.")
            return 0

        silver_df, date_range = _extract_silver_dataframe(pdf_dir)
        if silver_df.empty:
            logger.warning("Attendance PDFs present but no dates extracted — skipping silver write.")
            print("No attendance dates extracted from PDFs — skipping.")
            return 0

        _reconcile_against_published(pdf_dir)
        _build_silver_csv(silver_df, silver_csv)
    else:
        print(f"Aggregated attendance tables CSV already exists at {silver_csv}. Skipping PDF processing.")

    _build_fact_table(silver_csv, fact_csv, fact_parquet, house=house)

    print(f"date range extracted from title: {date_range}")
    print("Attendance CSV created successfully.")
    return 0


if __name__ == "__main__":
    rc = main()
    if rc == 0:
        print("TD attendance CSV created successfully and saved to td_attendance_fact_table.csv.")
    sys.exit(rc)
