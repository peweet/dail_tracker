"""Option (b) exploration: can we NAME the C&AG's anonymised suppliers A-G?

FINDING (sandbox, exploratory — NOT a decode of A-G):
- The C&AG's Figure 10.4 suppliers were paid from **Vote 40 (Children, Equality,
  Disability, Integration & Youth) 2024**. That department is NOT among the 86
  publishers in procurement_payments_fact — so the 2024 payments the C&AG
  analysed are NOT in our data. A-G cannot be decoded for 2024. Full stop.
- BUT IPAS transferred to the **Department of Justice** on 1 May 2025, and Justice
  IS a publisher we hold. From 2025 the named accommodation providers become
  visible. This builds the *named* provider list for 2025-2026 as an honest
  CORROBORATION layer alongside the (anonymised) C&AG 2024 chart — explicitly a
  different Vote and year, NOT an identity match to A-G.

Also captures accommodation providers under Dublin City Council (homeless/emergency
accommodation — a DIFFERENT spend stream) and HSE, for context, tagged by stream.

SANDBOX ONLY. value_safe_to_sum carried from source; po_committed and
payment_actual kept separate (never mixed). No promotion.
"""
from __future__ import annotations

import re
import polars as pl

from _common import SILVER, now_iso

GOLD = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet/procurement_payments_fact.parquet"

# accommodation / hospitality signal in the supplier name
ACCOM_RX = (r"hotel|hospitality|lodge|guest|hostel|holiday|inn\b|resort|manor|"
            r"mosney|bridgestock|fazyard|tifco|cape wrath|guestford|trailhead|"
            r"accommodat|equestrian|strand|castle|court\b|centre\b|smorgs|coolebridge|"
            r"drumhouse|ballyroe|sheebeen|bessborough|didean")

# light entity-resolution: collapse obvious variants to a display root
def canon(name: str) -> str:
    n = name.upper()
    n = re.sub(r"\bTA\b.*|\bT A\b.*|\bTRADING AS\b.*", "", n)  # drop 'trading as' tail
    n = re.sub(r"\b(LTD|LIMITED|ULC|DAC|CLG|CO|NOS?\s*\d+.*|UNLIMITED|GROUP|"
               r"MANAGEMENT|SERVICES|CARE|HOLDINGS|ROI)\b", "", n)
    n = re.sub(r"[^A-Z0-9 ]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def main() -> None:
    lf = pl.scan_parquet(GOLD)
    accom = (lf.filter(pl.col("supplier_normalised").str.to_lowercase().str.contains(ACCOM_RX))
               .filter(pl.col("year") >= 2016)
               .group_by("supplier_normalised", "publisher_name", "publisher_id",
                         "year", "amount_semantics")
               .agg(pl.col("amount_eur").sum().alias("amount_eur"),
                    pl.len().alias("rows"),
                    pl.col("value_safe_to_sum").max().alias("value_safe_to_sum"))
               .collect())

    accom = accom.with_columns([
        pl.col("supplier_normalised").map_elements(canon, return_dtype=pl.Utf8).alias("provider_root"),
        pl.when(pl.col("publisher_id") == "dept_justice").then(pl.lit("justice_ipas_2025plus"))
          .when(pl.col("publisher_id") == "ie_la_dublin_city").then(pl.lit("dcc_homeless_emergency"))
          .when(pl.col("publisher_id") == "ie_hse").then(pl.lit("hse_accommodation"))
          .otherwise(pl.lit("other_stream")).alias("spend_stream"),
        pl.lit("RoAPS 2024 Ch.10 suppliers A-G are Vote 40 2024 (NOT in our data). "
               "These are NAMED providers from other Votes/years — corroboration, "
               "NOT an A-G decode.").alias("caveat"),
        pl.lit(now_iso()).alias("derived_at"),
        pl.lit("payments_fact_keyword_scan").alias("extraction_method"),
        pl.lit("public_bodies").alias("privacy_tier"),
    ])

    out = SILVER / "cag_ipas_provider_candidates.parquet"
    accom.write_parquet(out, compression="zstd", statistics=True)
    print(f"wrote {out} - {accom.height} rows")

    # headline: Justice (IPAS) named providers, 2025-2026, by provider_root
    j = (accom.filter((pl.col("spend_stream") == "justice_ipas_2025plus") &
                      (pl.col("year") >= 2025))
              .group_by("provider_root")
              .agg(pl.col("amount_eur").sum().alias("total"),
                   pl.col("supplier_normalised").unique().alias("name_variants"),
                   pl.col("year").min().alias("y0"), pl.col("year").max().alias("y1"))
              .sort("total", descending=True))
    print("\nNAMED IPAS accommodation providers under Dept of Justice, 2025-2026 "
          "(po_committed + payment_actual combined — indicative only):")
    with pl.Config(tbl_rows=35, fmt_str_lengths=60, tbl_width_chars=150):
        print(j.with_columns((pl.col("total") / 1e6).round(2).alias("total_m")).drop("total"))

    # CSV exports for eyeballing
    exp = SILVER / "_eyeball"
    exp.mkdir(exist_ok=True)
    j.with_columns((pl.col("total") / 1e6).round(3).alias("total_m")).drop("total") \
     .with_columns(pl.col("name_variants").list.join(" | ")) \
     .write_csv(exp / "ipas_provider_candidates_justice_2025_26.csv")
    for f in ("cag_ipas_chapter_figures", "cag_ipas_chart_recovery"):
        pl.read_parquet(SILVER / f"{f}.parquet").write_csv(exp / f"{f}.csv")
    print(f"\nCSV exports -> {exp}")


if __name__ == "__main__":
    main()
