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
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd
from numpy import nan

from config import ATTENDANCE_PDF_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

IRISH_NAME_REGEX = re.compile(r"^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$")
EXCLUDE_CASES = re.compile(r"^(Member|Sitting|Totals|Total)")
DATE_RANGE = re.compile(r"(\d{1,2}-[a-zA-Z]+-\d{4})-to-(\d{1,2}-[a-zA-Z]+-\d{4})")


def _extract_pdf_tables(pdf_dir: Path) -> tuple[list[pd.DataFrame], str]:
    """Extract tables from every PDF in pdf_dir. No cwd mutation."""
    dataframes: list[pd.DataFrame] = []
    date_range = ""
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        pdf = pdf_path.name
        print(pdf_path.stem.title())
        match = re.search(DATE_RANGE, pdf_path.stem.lower())
        date_range = f"{match.group(1)}_to_{match.group(2)}" if match else "unknown"
        print(f"Processing {pdf}...")
        doc = fitz.open(str(pdf_path))
        print(f"Number of pages in {pdf}: {doc.page_count}")
        first_name = ""
        last_name = ""
        identifier = ""
        for page in doc:
            print(f"Processing page {page.number} of {pdf}...")
            text = page.get_text("text")
            lines = text.split("\n")
            for line in lines:
                print(f"Processing line: {line}")
                if IRISH_NAME_REGEX.search(line) and not EXCLUDE_CASES.search(line):
                    names = line.split(maxsplit=1)
                    first_name = names[-1]
                    last_name = " ".join(names[:-1])
                    identifier = line.replace(" ", "_")
            tabs = page.find_tables()
            if len(tabs.tables) == 0:
                continue
            for tab in tabs.tables:
                df = tab.to_pandas()
                df.insert(0, "identifier", identifier)
                df.insert(1, "first_name", first_name)
                df.insert(2, "last_name", last_name)
                dataframes.append(df)
    return dataframes, date_range


def _build_silver_csv(dataframes: list[pd.DataFrame], csv_path: Path) -> None:
    """Concat → clean → dedup → write silver CSV."""
    df = pd.concat(dataframes).drop(["Col1", "Col2", "Col3", "Col4", "Col5"], axis=1, errors="ignore")
    df = df.iloc[:, :5].fillna(nan)
    df = df.replace("", nan).rename(
        columns={
            "Sitting days attendance recorded on system": "sitting_days_attendance",
            "Other days attendance recorded on system *": "other_days_attendance",
            "Sitting days attendance": "sitting_days_attendance",
            "Other days attendance": "other_days_attendance",
        }
    )
    drop_cols = [c for c in ["sitting_days_attendance", "other_days_attendance"] if c in df.columns]
    if drop_cols:
        df = df.dropna(subset=drop_cols, how="all")
    year_from_sitting = (
        df["sitting_days_attendance"].str.split("/", n=3).str[-1] if "sitting_days_attendance" in df.columns else None
    )
    year_from_other = (
        df["other_days_attendance"].str.split("/", n=3).str[-1] if "other_days_attendance" in df.columns else None
    )
    df["year"] = year_from_sitting.fillna(year_from_other).fillna("Missing")
    df["iso_sitting_days_attendance"] = pd.to_datetime(
        df["sitting_days_attendance"], format="%d/%m/%Y", errors="coerce"
    )
    df["iso_other_days_attendance"] = pd.to_datetime(df["other_days_attendance"], format="%d/%m/%Y", errors="coerce")
    # Dedup before writing silver — cumulative attendance PDFs all restate
    # prior days, so the raw concat above contains 3-9x duplication. Drop
    # on (TD, sitting_date, other_date) so identical date entries collapse
    # whichever column carries the date.
    _before = len(df)
    df = df.drop_duplicates(subset=["identifier", "iso_sitting_days_attendance", "iso_other_days_attendance"])
    print(f"Silver dedup: dropped {_before - len(df):,} duplicate rows before silver CSV write (kept {len(df):,})")
    df.to_csv(csv_path, index=False)


def _build_fact_table(silver_csv: Path, fact_csv: Path, fact_parquet: Path) -> None:
    """Re-read silver CSV, compute counts, write fact CSV + parquet.

    Re-read (rather than passing the in-memory df) is intentional — keeps
    dtype coercions to_csv → read_csv applies, so the fact table is
    deterministic across silver-write paths.
    """
    df = pd.read_csv(silver_csv)

    df["sitting_flag"] = df["iso_sitting_days_attendance"].notna().astype(int)
    df["other_flag"] = df["iso_other_days_attendance"].notna().astype(int)

    # Defensive dedup — silver write already deduped, this is an idempotency
    # check. With clean silver, this should drop 0.
    _before_dedup = len(df)
    df = df.drop_duplicates(subset=["identifier", "iso_sitting_days_attendance", "iso_other_days_attendance"])
    print(f"Attendance dedup: dropped {_before_dedup - len(df):,} duplicate rows (kept {len(df):,})")

    # Counts must be of UNIQUE dates per (TD, year), not of rows. PDF rows
    # pair two independent date columns (sitting + other) by row index, so
    # the same sitting date can appear in N rows paired with N different
    # other dates — counting flags would inflate. nunique gives the count
    # of distinct sitting / other days, which is the published meaning.
    df["sitting_days_count"] = df.groupby(["identifier", "year"])["iso_sitting_days_attendance"].transform("nunique")
    df["other_days_count"] = df.groupby(["identifier", "year"])["iso_other_days_attendance"].transform("nunique")

    df["sitting_total_days"] = df["sitting_days_count"] + df["other_days_count"]

    drop_cols = [
        c for c in ["sitting_flag", "other_flag", "sitting_days_attendance", "other_days_attendance"] if c in df.columns
    ]
    if drop_cols:
        df = df.drop(drop_cols, axis=1)
    df.to_csv(fact_csv, index=False)

    df.to_parquet(
        fact_parquet,
        index=False,
        compression="zstd",
        compression_level=3,
    )


def main() -> int:
    """Build silver attendance CSV + fact table from bronze PDFs.

    Exit codes:
        0 — ok, or skipped cleanly (no PDFs / no tables extracted)
    """
    silver_csv = SILVER_DIR / "aggregated_td_tables.csv"
    fact_csv = SILVER_DIR / "td_attendance_fact_table.csv"
    fact_parquet = SILVER_DIR / "parquet" / "td_attendance_fact_table.parquet"

    date_range = ""

    if not silver_csv.is_file():
        print("Aggregated payment tables CSV not found. Starting PDF processing to create it...")
        pdfs_exist = any(ATTENDANCE_PDF_DIR.glob("*.pdf"))
        if not pdfs_exist:
            logger.warning(
                "No attendance PDFs in %s and no existing silver CSV — skipping attendance build.",
                ATTENDANCE_PDF_DIR,
            )
            print(f"No attendance PDFs in {ATTENDANCE_PDF_DIR} — skipping.")
            return 0

        dataframes, date_range = _extract_pdf_tables(ATTENDANCE_PDF_DIR)
        if not dataframes:
            logger.warning("Attendance PDFs present but no tables extracted — skipping silver write.")
            print("No tables extracted from attendance PDFs — skipping.")
            return 0

        _build_silver_csv(dataframes, silver_csv)
    else:
        print(f"Aggregated payment tables CSV already exists at {silver_csv}. Skipping PDF processing.")

    _build_fact_table(silver_csv, fact_csv, fact_parquet)

    print(f"date range extracted from title: {date_range}")
    print("TD attendance CSV created successfully.")
    return 0


if __name__ == "__main__":
    rc = main()
    if rc == 0:
        print("TD attendance CSV created successfully and saved to td_attendance_fact_table.csv.")
    sys.exit(rc)
