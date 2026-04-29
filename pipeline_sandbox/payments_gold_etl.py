"""
payments_gold_etl.py — ISOLATED SANDBOX SCRIPT

Transforms silver payments CSV → clean gold Parquet fact table.

DO NOT import or call this from any existing pipeline file.
DO NOT modify any existing pipeline files.
Test this independently, validate output, then integrate once confident.

When integrated:
- Output replaces the current complex string manipulation in payments_base.sql
- SQL views should be simplified to: SELECT * FROM read_parquet('data/gold/parquet/payments_fact.parquet')
  with aggregation/ranking only — no CASE, no REGEXP_REPLACE, no SPLIT_PART

TODO_PIPELINE_VIEW_REQUIRED: once validated and integrated, update payments_base.sql to a
simple read_parquet and remove all transformation logic from SQL.

Reads:   data/silver/aggregated_payment_tables.csv
Writes:  data/gold/parquet/payments_fact.parquet
         data/gold/parquet/payments_quarantined.parquet  ← rejected rows for inspection
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

# ── Paths ──────────────────────────────────────────────────────────────────────

_ROOT      = Path(__file__).resolve().parents[1]
_SILVER    = _ROOT / "data" / "silver" / "aggregated_payment_tables.csv"
_OUT_FACT  = _ROOT / "data" / "gold" / "parquet" / "payments_fact.parquet"
_OUT_QUAR  = _ROOT / "data" / "gold" / "parquet" / "payments_quarantined.parquet"

# ── TAA band label lookup ───────────────────────────────────────────────────────
# Official Oireachtas PSA band labels.
# Source: oireachtas.ie/en/members/salaries-and-allowances/parliamentary-standard-allowances/
# Bands 9-12 are not in the published 1-8 table but appear in the source data with valid
# payment amounts — retained as-is pending pipeline clarification.
# TODO_PIPELINE_VIEW_REQUIRED: clarify whether bands 9-12 represent an extended or
# superseded band range. Do not infer or guess — flag only.

_TAA_LABELS: dict[str, str] = {
    "Dublin": "Dublin / under 25 km",
    "1":      "Band 1 — 25–60 km",
    "2":      "Band 2 — 60–80 km",
    "3":      "Band 3 — 80–100 km",
    "4":      "Band 4 — 100–130 km",
    "5":      "Band 5 — 130–160 km",
    "6":      "Band 6 — 160–190 km",
    "7":      "Band 7 — 190–210 km",
    "8":      "Band 8 — over 210 km",
    # Extended numeric bands — present in data, meaning unclear, retained without label
    "9":      "Band 9 (unmapped)",
    "10":     "Band 10 (unmapped)",
    "11":     "Band 11 (unmapped)",
    "12":     "Band 12 (unmapped)",
}


def _is_clean_band(band: str) -> bool:
    """
    A band is clean if it is 'Dublin' or a pure integer string (any number).
    Everything else — 'Vouched', 'MIN', 'NoTAA', combined codes like '2/MIN',
    garbled values like 'Kenny', encoding artifacts — is quarantined.
    """
    if band == "Dublin":
        return True
    try:
        int(band)
        return True
    except (ValueError, TypeError):
        return False


def run() -> None:
    print(f"Reading: {_SILVER}")
    raw = pl.read_csv(
        _SILVER,
        schema_overrides={
            "TAA_Band":  pl.Utf8,
            "Amount":    pl.Utf8,
            "Date_Paid": pl.Utf8,
        },
        infer_schema_length=0,  # all columns as string — we parse explicitly
        truncate_ragged_lines=True,
        ignore_errors=True,
    )
    print(f"Raw rows: {len(raw)}")

    # ── Normalise TAA_Band: strip whitespace, cast NaN strings to null ─────────
    raw = raw.with_columns(
        pl.col("TAA_Band")
          .str.strip_chars()
          .replace("nan", None)
          .alias("TAA_Band")
    )

    # ── Split into clean and quarantined ───────────────────────────────────────
    # Clean: TAA_Band is 'Dublin' or a pure integer string
    clean_mask = raw["TAA_Band"].map_elements(
        lambda b: _is_clean_band(str(b)) if b is not None else False,
        return_dtype=pl.Boolean,
    )
    clean      = raw.filter(clean_mask)
    quarantined = raw.filter(~clean_mask)

    print(f"Clean rows:      {len(clean)}")
    print(f"Quarantined rows:{len(quarantined)}")
    print(f"Quarantined TAA_Band values: {sorted(quarantined['TAA_Band'].drop_nulls().unique().to_list())}")

    # ── Name normalisation: 'Last, First' → 'First Last' ─────────────────────
    # Only split if a comma is present; otherwise leave as-is.
    clean = clean.with_columns(
        pl.col("Full_Name")
          .str.strip_chars()
          .alias("Full_Name")
    ).with_columns(
        pl.when(pl.col("Full_Name").str.contains(","))
          .then(
              pl.col("Full_Name").str.split(",").list.get(1).str.strip_chars()
              + pl.lit(" ")
              + pl.col("Full_Name").str.split(",").list.get(0).str.strip_chars()
          )
          .otherwise(pl.col("Full_Name"))
          .alias("member_name")
    )

    # ── Amount: strip all non-numeric chars except decimal point ──────────────
    # Handles any euro sign encoding variant.
    clean = clean.with_columns(
        pl.col("Amount")
          .str.replace_all(r"[^0-9.]", "")
          .cast(pl.Float64, strict=False)
          .alias("amount_num")
    )

    # ── Date: parse to date, derive payment_year ───────────────────────────────
    # The source has mixed formats: ISO (2022-01-28) and non-ISO (26/06/2020, 5/28/2020).
    # Polars strptime with strict=False handles ISO; non-ISO rows become null.
    # TODO_PIPELINE_VIEW_REQUIRED: fix non-ISO date rows in silver CSV upstream.
    clean = clean.with_columns(
        pl.col("Date_Paid")
          .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
          .alias("date_paid")
    ).with_columns(
        pl.col("date_paid").dt.year().alias("payment_year")
    )

    # ── TAA band label ────────────────────────────────────────────────────────
    clean = clean.with_columns(
        pl.col("TAA_Band")
          .replace(_TAA_LABELS)
          .alias("taa_band_label")
    )

    # ── Position: normalise null ───────────────────────────────────────────────
    clean = clean.with_columns(
        pl.col("Position").fill_null("Deputy").str.strip_chars().alias("position")
    )

    # ── Final gate: drop rows where date or amount is null ─────────────────────
    before_gate = len(clean)
    clean = clean.filter(
        pl.col("date_paid").is_not_null()
        & pl.col("amount_num").is_not_null()
        & (pl.col("amount_num") > 0)
        & pl.col("member_name").is_not_null()
    )
    print(f"Dropped by date/amount gate: {before_gate - len(clean)}")
    print(f"Final clean rows: {len(clean)}")

    # ── Select and order output columns ───────────────────────────────────────
    fact = clean.select([
        "member_name",
        "position",
        pl.col("TAA_Band").alias("taa_band_raw"),
        "taa_band_label",
        "date_paid",
        pl.col("Narrative").str.strip_chars().alias("narrative"),
        "amount_num",
        "payment_year",
    ])

    # ── Write outputs ─────────────────────────────────────────────────────────
    _OUT_FACT.parent.mkdir(parents=True, exist_ok=True)

    fact.write_parquet(_OUT_FACT)
    print(f"Written: {_OUT_FACT}")

    quarantined.write_parquet(_OUT_QUAR)
    print(f"Written (quarantined): {_OUT_QUAR}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=== Fact table summary ===")
    print(f"  Distinct members : {fact['member_name'].n_unique()}")
    print(f"  Years covered    : {sorted(fact['payment_year'].drop_nulls().unique().to_list())}")
    print(f"  TAA bands in data: {sorted(fact['taa_band_raw'].unique().to_list())}")
    print()
    print("=== Top 10 members by all-time total ===")
    top = (
        fact
        .group_by("member_name")
        .agg(pl.col("amount_num").sum().alias("total"))
        .sort("total", descending=True)
        .head(10)
    )
    print(top)


if __name__ == "__main__":
    run()
