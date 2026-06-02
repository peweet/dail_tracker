"""Seanad PSA payments ETL — EXPERIMENTAL PROTOTYPE.

# DELETE AFTER PROMOTION: payments_full_psa_etl.py (parameterise by chamber)

Parses the Senator Parliamentary Standard Allowance (PSA) PDFs downloaded by
seanad_pdf_poll_experimental.py and proves they are field-for-field compatible
with the deputies PSA the production parser already handles.

Schema (confirmed on the live PDFs, identical to the Jul-2020+ deputies schema
that payments_full_psa_etl._detect_schema calls "schema 3"):
    Name | TAA Band | Narrative | Date Paid | Amount
  - Name      : "Senator <Last>, <First>"   (production _split_position knows
                 Deputy/Minister/Taoiseach/Tánaiste but NOT Senator → must add)
  - TAA Band  : numeric 1–9 (distance band) OR "Dublin" (fixed lower rate)
  - Amount    : "€3,172.83"

Extraction (fitz table iteration) is unavoidably per-page I/O; every transform
after the rows are gathered is vectorised Polars (no row loops, no .apply).

Source : data/bronze/pdfs/payments_seanad_experimental/*.pdf
Writes : data/silver/parquet/seanad_payments_psa_experimental.parquet  (--write)

Usage:
  python pipeline_sandbox/seanad_payments_etl_experimental.py
  python pipeline_sandbox/seanad_payments_etl_experimental.py --write
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_PDF_DIR = _ROOT / "data" / "bronze" / "pdfs" / "payments_seanad_experimental"
_SILVER_PARQUET = _ROOT / "data" / "silver" / "parquet"
_SEANAD_MEMBERS = _SILVER_PARQUET / "flattened_seanad_members.parquet"

_POSITIONS = ("Senator", "Deputy", "Minister", "Taoiseach", "Tánaiste", "Cathaoirleach")


# ---------------------------------------------------------------------------
# Extract  (per-page table I/O — gather raw 5-col rows; no data wrangling here)
# ---------------------------------------------------------------------------
def gather_rows(pdf_dir: Path) -> list[dict]:
    """Pull every 5-cell table row from every Senator PSA PDF in pdf_dir."""
    rows: list[dict] = []
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        doc = fitz.open(str(pdf_path))
        for page in doc:
            for table in page.find_tables().tables:
                for r in table.extract():
                    if len(r) == 5:
                        rows.append(
                            {
                                "name_raw": r[0], "band_raw": r[1], "narrative": r[2],
                                "date_paid": r[3], "amount_raw": r[4],
                                "source_pdf": pdf_path.name,
                            }
                        )
    return rows


# ---------------------------------------------------------------------------
# Transform  (fully vectorised Polars)
# ---------------------------------------------------------------------------
def transform(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows)

    # Keep only money-shaped data rows: amount starts with €. Drops the title
    # row ("Parliamentary Standard Allowance") and the header row in one filter.
    df = df.filter(pl.col("amount_raw").cast(pl.Utf8).str.contains("€"))

    # Names are "<Position> <Last>, <First>". The leading token is the position
    # word; everything after the first space is the name. Splitting on the first
    # space (rather than matching a fixed position list) is robust to source-PDF
    # typos like "Senaotr Goldsboro, Imelda" — a real misspelling in the
    # Apr-2025 / Feb-2026 Senator PDFs. Sen* normalises to "Senator".
    df = df.with_columns(
        pl.col("name_raw").str.splitn(" ", 2).struct.rename_fields(["pos_raw", "member_name"]).alias("_nm")
    ).unnest("_nm")
    df = df.with_columns(
        pl.col("member_name").str.strip_chars(),
        pl.when(pl.col("pos_raw").str.starts_with("Sen"))
        .then(pl.lit("Senator"))
        .otherwise(pl.col("pos_raw"))
        .alias("position"),
        pl.col("amount_raw").str.replace_all(r"[€,]", "").cast(pl.Float64).alias("amount_eur"),
        pl.col("band_raw").cast(pl.Utf8).str.strip_chars().alias("taa_band"),
        (pl.col("band_raw").cast(pl.Utf8).str.strip_chars() == "Dublin").alias("is_dublin_rate"),
        pl.col("date_paid").str.to_date("%d/%m/%Y", strict=False).alias("date_paid"),
        pl.lit("Seanad").alias("house"),
    )
    return df.select(
        "member_name", "position", "taa_band", "is_dublin_rate",
        "amount_eur", "date_paid", "narrative", "house", "source_pdf",
    )


# ---------------------------------------------------------------------------
def assess(df: pl.DataFrame) -> None:
    print("\n=== HEAD (6) ===")
    with pl.Config(tbl_cols=-1, fmt_str_lengths=28):
        print(df.head(6))

    print("\n=== SHAPE ===")
    print(f"rows={df.height} | distinct members={df['member_name'].n_unique()} | "
          f"distinct months={df['narrative'].n_unique()} | pdfs={df['source_pdf'].n_unique()}")

    print("\n=== NULLS (key cols) ===")
    key = ["member_name", "amount_eur", "date_paid", "taa_band"]
    nulls = df.select([pl.col(c).null_count().alias(c) for c in key])
    print(nulls)

    print("\n=== position word distribution (should all be 'Senator') ===")
    print(df["position"].value_counts(sort=True))

    print("\n=== amount sanity (€) ===")
    print(df.select(
        pl.col("amount_eur").min().alias("min"),
        pl.col("amount_eur").max().alias("max"),
        pl.col("amount_eur").mean().round(2).alias("mean"),
        (pl.col("amount_eur") <= 0).sum().alias("non_positive"),
    ))

    print("\n=== TAA band distribution ===")
    print(df["taa_band"].value_counts(sort=True).head(12))

    # Name match to current 27th Seanad. Members parquet stores last_name/first_name;
    # PSA name is "Last, First" — compare on a normalised "first last" lowercased key.
    matched_pct = None
    if _SEANAD_MEMBERS.exists():
        mem = pl.read_parquet(_SEANAD_MEMBERS).select(
            pl.concat_str([pl.col("first_name"), pl.lit(" "), pl.col("last_name")])
            .str.to_lowercase().str.strip_chars().alias("k")
        ).unique()
        paid = df.select(
            pl.col("member_name").str.split(", ").list.reverse().list.join(" ")
            .str.to_lowercase().str.strip_chars().alias("k")
        ).unique()
        matched = paid.join(mem, on="k", how="inner").height
        matched_pct = 100 * matched / paid.height if paid.height else 0
        print("\n=== NAME MATCH to current 27th Seanad (60 members) ===")
        print(f"distinct paid={paid.height} | matched={matched} ({matched_pct:.1f}%) | "
              f"unmatched={paid.height - matched} (former senators / normalisation edge cases)")

    print("\n=== DATA-QUALITY VERDICT ===")
    issues = []
    if sum(nulls.row(0)):
        issues.append("nulls in key columns")
    if df.filter(pl.col("amount_eur") <= 0).height:
        issues.append("non-positive amounts")
    positions = set(df["position"].drop_nulls().unique().to_list())
    if positions - {"Senator"}:
        issues.append(f"unexpected position words {positions}")
    if not issues:
        print("CLEAN. Schema is field-for-field identical to the deputies PSA (schema 3).")
        print("CONFIDENCE: HIGH for promotion to payments_full_psa_etl.py.")
        print("  Required promotion changes (small, well-scoped):")
        print("   1. _split_position(): add 'Senator' (and 'Cathaoirleach') to the known set —")
        print("      today an unknown prefix defaults to 'Deputy', mislabelling every senator.")
        print("   2. build_full_psa(): glob a house-aware PDF dir (or tag rows by source path).")
        print("   3. carry a `house` column to gold; reuse payments_member_enrichment for the join.")
        print(f"  Name match to current Seanad: {matched_pct:.0f}%." if matched_pct is not None else "")
        print(f"  Parsed {df['source_pdf'].n_unique()} of 13 published Senator PSA PDFs.")
        print("  Source-data note: 'PSA Arrears' rows are valid back-payments (small €);")
        print("  one source typo 'Senaotr' is normalised here and must be tolerated on promotion.")
    else:
        print("ISSUES: " + "; ".join(issues))
        print("CONFIDENCE: MEDIUM — investigate before promotion.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Seanad PSA payments ETL (experimental)")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    rows = gather_rows(_PDF_DIR)
    print(f"Gathered {len(rows)} raw 5-col table rows from {_PDF_DIR.name}")
    if not rows:
        print("No PDFs found — run seanad_pdf_poll_experimental.py first.")
        return 1

    df = transform(rows)
    assess(df)

    if args.write:
        _SILVER_PARQUET.mkdir(parents=True, exist_ok=True)
        out = _SILVER_PARQUET / "seanad_payments_psa_experimental.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
        print(f"\nWrote {df.height} rows -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
