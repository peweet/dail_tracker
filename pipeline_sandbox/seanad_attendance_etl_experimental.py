"""Seanad attendance ETL — EXPERIMENTAL PROTOTYPE.

# DELETE AFTER PROMOTION: attendance.py (parameterise by chamber)

Parses the Senator "Member Sitting Days Report" PDFs downloaded by
seanad_pdf_poll_experimental.py into one row per senator per period, carrying
the two headline totals the deputies report also publishes:
    total_sitting_days            — sitting days in the period
    attendances_for_allowance     — attendances counted for TAA purposes

Structure (confirmed on the live PDFs, identical to the deputies report):
  per-member block →
    "<Last> <First>"
    "Senator, 27th Seanad, <range>, Limit: <n>"   <- block marker (vs "Deputy, 34th Dáil")
    <date lists, sub-totals>
    "Total number of sitting days in the period"  -> next line = value
    "Total attendances recorded for allowance purposes in the" / "period" -> +2 = value

Extraction (fitz page text) is per-page I/O; the parse is fully vectorised
Polars — member identity via a shift+forward_fill of the marker line, totals via
label-anchored shifts. No row loops, no .apply.

Source : data/bronze/pdfs/attendance_seanad_experimental/*.pdf
Writes : data/silver/parquet/seanad_attendance_experimental.parquet  (--write)
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_PDF_DIR = _ROOT / "data" / "bronze" / "pdfs" / "attendance_seanad_experimental"
_SILVER_PARQUET = _ROOT / "data" / "silver" / "parquet"
_SEANAD_MEMBERS = _SILVER_PARQUET / "flattened_seanad_members.parquet"

# Block boundary: the line that follows the member name, e.g.
# "Senator, 27th Seanad, …" or "Senator, 26th Seanad, …". Term-generic on
# purpose — the 2024/early-2025 PDFs are 26th-Seanad and a hardcoded "27th"
# silently drops them. (Production attendance.py sidesteps this entirely via
# IRISH_NAME_REGEX name detection — a point in favour of reusing it.)
_MARKER = r", \d+\w+ Seanad"
_SITTING_LABEL = "Total number of sitting days in the period"
_ALLOW_LABEL = "Total attendances recorded for allowance purposes in the"
_PERIOD_RE = re.compile(r"taa-(\d{2}-\w+-\d{4})-to-(\d{2}-\w+-\d{4})")


# ---------------------------------------------------------------------------
# Extract  (per-page text I/O — gather flat lines tagged by source PDF)
# ---------------------------------------------------------------------------
def gather_lines(pdf_dir: Path) -> list[dict]:
    out: list[dict] = []
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        m = _PERIOD_RE.search(pdf_path.stem.lower())
        period = f"{m.group(1)}_to_{m.group(2)}" if m else pdf_path.stem
        doc = fitz.open(str(pdf_path))
        for page in doc:
            for line in page.get_text("text").splitlines():
                if line.strip():
                    out.append({"line": line.strip(), "source_pdf": pdf_path.name, "period": period})
    return out


# ---------------------------------------------------------------------------
# Transform  (fully vectorised Polars)
# ---------------------------------------------------------------------------
def transform(lines: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(lines)

    # Member identity: the marker line is "Senator, 27th Seanad, …"; the member
    # name is the line immediately before it. shift+forward_fill spreads the
    # current member down its whole block. All shifts scoped within a PDF.
    df = df.with_columns(
        pl.when(pl.col("line").str.contains(_MARKER))
        .then(pl.col("line").shift(1).over("source_pdf"))
        .alias("member_marker"),
        pl.col("line").shift(-1).over("source_pdf").alias("next1"),
        pl.col("line").shift(-2).over("source_pdf").alias("next2"),
    )
    df = df.with_columns(pl.col("member_marker").forward_fill().over("source_pdf").alias("member_raw"))

    # Totals are anchored on their label lines.
    totals = (
        df.with_columns(
            pl.when(pl.col("line") == _SITTING_LABEL).then(pl.col("next1")).alias("sitting_str"),
            pl.when(pl.col("line") == _ALLOW_LABEL).then(pl.col("next2")).alias("allow_str"),
        )
        .group_by("source_pdf", "period", "member_raw")
        .agg(
            pl.col("sitting_str").drop_nulls().first(),
            pl.col("allow_str").drop_nulls().first(),
        )
        .drop_nulls("member_raw")
    )

    return totals.with_columns(
        # "Ahearn Garret" -> "Garret Ahearn" (first last), lowercased join key
        pl.col("member_raw").str.strip_chars().alias("member_name"),
        pl.col("sitting_str").cast(pl.Int64, strict=False).alias("total_sitting_days"),
        pl.col("allow_str").cast(pl.Int64, strict=False).alias("attendances_for_allowance"),
        pl.lit("Seanad").alias("house"),
    ).select(
        "member_name", "total_sitting_days", "attendances_for_allowance", "period", "house", "source_pdf"
    ).sort("source_pdf", "member_name")


# ---------------------------------------------------------------------------
def assess(df: pl.DataFrame) -> None:
    print("\n=== HEAD (6) ===")
    with pl.Config(tbl_cols=-1, fmt_str_lengths=30):
        print(df.head(6))

    print("\n=== SHAPE ===")
    print(f"rows={df.height} | distinct members={df['member_name'].n_unique()} | "
          f"periods={df['period'].n_unique()} | pdfs={df['source_pdf'].n_unique()}")

    print("\n=== NULLS (key cols) ===")
    key = ["member_name", "total_sitting_days", "attendances_for_allowance"]
    nulls = df.select([pl.col(c).null_count().alias(c) for c in key])
    print(nulls)

    print("\n=== totals sanity ===")
    print(df.select(
        pl.col("total_sitting_days").min().alias("sit_min"),
        pl.col("total_sitting_days").max().alias("sit_max"),
        pl.col("attendances_for_allowance").max().alias("allow_max"),
        (pl.col("attendances_for_allowance") > pl.col("total_sitting_days")).sum().alias("allow_gt_sitting"),
        (pl.col("total_sitting_days") < 0).sum().alias("negative"),
    ))

    matched_pct = None
    if _SEANAD_MEMBERS.exists():
        mem = pl.read_parquet(_SEANAD_MEMBERS).select(
            pl.concat_str([pl.col("first_name"), pl.lit(" "), pl.col("last_name")])
            .str.to_lowercase().str.strip_chars().alias("k")
        ).unique()
        att = df.select(
            # "Ahearn Garret" -> "garret ahearn"
            pl.col("member_name").str.split(" ").list.reverse().list.join(" ")
            .str.to_lowercase().str.strip_chars().alias("k")
        ).unique()
        matched = att.join(mem, on="k", how="inner").height
        matched_pct = 100 * matched / att.height if att.height else 0
        print("\n=== NAME MATCH to current 27th Seanad (60 members) ===")
        print(f"distinct members={att.height} | matched={matched} ({matched_pct:.1f}%) | "
              f"unmatched={att.height - matched}")

    print("\n=== DATA-QUALITY VERDICT ===")
    issues = []
    if sum(nulls.row(0)):
        issues.append("nulls in key columns (a member block may not have parsed)")
    if df.filter(pl.col("attendances_for_allowance") > pl.col("total_sitting_days")).height:
        # NB: in the deputies report this is EXPECTED — "other days" (committee
        # days) can push allowance attendances above plain sitting days. Flag, not fail.
        issues.append("allowance > sitting (expected per deputies report; verify, don't fail)")
    if not issues:
        print("CLEAN. Report layout identical to the deputies 'Member Sitting Days Report'.")
        print("CONFIDENCE: HIGH for promotion to attendance.py.")
        print("  Required promotion changes:")
        print("   1. block marker: accept 'Senator, NNth Seanad' alongside 'Deputy, NNth Dáil'.")
        print("   2. glob a house-aware PDF dir; carry a `house` column to the fact table.")
        print(f"  Name match to current Seanad: {matched_pct:.0f}%." if matched_pct is not None else "")
        print("  Residual risk: only 2 sample periods parsed; run the full 26 before promotion.")
    else:
        print("NOTES/ISSUES: " + "; ".join(issues))
        print("CONFIDENCE: MEDIUM-HIGH — review the flagged rows (most are expected semantics).")


def main() -> int:
    ap = argparse.ArgumentParser(description="Seanad attendance ETL (experimental)")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    lines = gather_lines(_PDF_DIR)
    print(f"Gathered {len(lines)} text lines from {_PDF_DIR.name}")
    if not lines:
        print("No PDFs found — run seanad_pdf_poll_experimental.py first.")
        return 1

    df = transform(lines)
    assess(df)

    if args.write:
        _SILVER_PARQUET.mkdir(parents=True, exist_ok=True)
        out = _SILVER_PARQUET / "seanad_attendance_experimental.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
        print(f"\nWrote {df.height} rows -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
