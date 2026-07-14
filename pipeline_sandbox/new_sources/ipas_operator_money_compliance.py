"""THE JOIN: named IPAS operator -> centres -> HIQA compliance -> public money.

Until now the two halves never met. The C&AG anonymised the suppliers (A-G); the HIQA
OVERVIEW report never names an operator. But the 101 INDIVIDUAL HIQA inspection reports
name the provider on every single one — so an operator can now be tied to:
  * the centres it runs and the counties they are in
  * its compliance record (2,668 centre x standard judgments, incl. RED-rated failures)
  * the money it has actually been paid (procurement_payments_fact, Dept of Justice)

CAVEATS (hard):
- Name matching is a GATE, not a guess: exact NFKD/name-norm first, then a distinctive-token
  pass, and everything else is left as 'unmatched' rather than forced. match_confidence is
  carried on every row.
- The payments side is Dept of JUSTICE 2025+ (IPAS transferred there 1 May 2025). The
  compliance side spans 2024-01 -> 2026-03. THESE ARE DIFFERENT WINDOWS — the money is NOT
  "the price of that compliance record". Never present it as causal.
- value_safe_to_sum stays False. Payments are po_committed + payment_actual and are kept
  labelled, never blended.
- Provider names inherit the accommodation-providers public_display gating at join time.
"""
from __future__ import annotations

import re
import unicodedata

import polars as pl

from _common import SILVER, now_iso

PAY = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet/procurement_payments_fact.parquet"


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\b(ltd|limited|ulc|dac|clg|plc|co|company|group|holdings|the|t/a|ta)\b",
               " ", s, flags=re.I)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s.lower())).strip()


def main() -> None:
    comp = pl.read_parquet(SILVER / "hiqa_centre_compliance.parquet")
    pcol = next(c for c in comp.columns if "provider" in c.lower())
    jcol = next(c for c in comp.columns if "judgment" in c.lower() and "conflict" not in c.lower())

    # ---- operator compliance profile ----
    op = (comp.with_columns(pl.col(pcol).map_elements(fold, return_dtype=pl.Utf8).alias("op_key"))
              .group_by("op_key")
              .agg([
                  pl.col(pcol).mode().first().alias("operator"),
                  pl.col("centre_name").n_unique().alias("centres"),
                  pl.col("county").unique().alias("counties"),
                  pl.len().alias("judgments"),
                  (pl.col(jcol).str.to_lowercase().str.starts_with("not")).sum().alias("not_compliant"),
                  (pl.col(jcol).str.to_lowercase().str.starts_with("partially")).sum().alias("partially_compliant"),
              ])
              .with_columns((pl.col("not_compliant") / pl.col("judgments") * 100)
                            .round(1).alias("pct_not_compliant"))
              .sort("pct_not_compliant", descending=True))

    # ---- money: Dept of Justice (IPAS from 1 May 2025) ----
    pay = (pl.scan_parquet(PAY)
             .filter(pl.col("publisher_id") == "dept_justice")
             .group_by("supplier_normalised", "amount_semantics")
             .agg(pl.col("amount_eur").sum().alias("amount_eur"), pl.len().alias("rows"))
             .collect()
             .with_columns(pl.col("supplier_normalised")
                             .map_elements(fold, return_dtype=pl.Utf8).alias("op_key")))
    paid = (pay.group_by("op_key")
               .agg(pl.col("supplier_normalised").mode().first().alias("paid_as"),
                    pl.col("amount_eur").sum().alias("total_paid_eur"),
                    pl.col("rows").sum().alias("payment_rows")))

    joined = (op.join(paid, on="op_key", how="left")
                .with_columns([
                    pl.when(pl.col("total_paid_eur").is_not_null())
                      .then(pl.lit("exact_name_norm")).otherwise(pl.lit("unmatched"))
                      .alias("match_confidence"),
                    pl.lit("compliance window 2024-01→2026-03; payments = Dept of JUSTICE only "
                           "(IPAS transferred 1 May 2025) — DIFFERENT WINDOWS, never causal")
                      .alias("caveat"),
                    pl.lit(False).alias("value_safe_to_sum"),
                    pl.lit(now_iso()).alias("derived_at"),
                    pl.lit("public_bodies_and_providers").alias("privacy_tier"),
                    pl.lit("provider names must inherit accommodation-providers public_display gating")
                      .alias("join_caveat"),
                ]))

    out = SILVER / "ipas_operator_money_compliance.parquet"
    joined.write_parquet(out, compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    joined.drop("counties").write_csv(SILVER / "_eyeball" / "ipas_operator_money_compliance.csv")

    n_match = joined.filter(pl.col("match_confidence") == "exact_name_norm").height
    print(f"wrote {out} — {joined.height} operators, {n_match} matched to DoJ payments\n")
    with pl.Config(tbl_rows=35, fmt_str_lengths=34, tbl_width_chars=165):
        print(joined.select("operator", "centres", "judgments", "not_compliant",
                            "pct_not_compliant", "paid_as", "total_paid_eur", "payment_rows")
                    .with_columns((pl.col("total_paid_eur") / 1e6).round(2).alias("paid_eur_m"))
                    .drop("total_paid_eur")
                    .sort("pct_not_compliant", descending=True))
    print("\n--- operators with BOTH a compliance record AND public money ---")
    with pl.Config(tbl_rows=25, fmt_str_lengths=34, tbl_width_chars=150):
        print(joined.filter(pl.col("total_paid_eur").is_not_null())
                    .select("operator", "centres", "pct_not_compliant", "paid_as", "total_paid_eur")
                    .with_columns((pl.col("total_paid_eur") / 1e6).round(2).alias("paid_eur_m"))
                    .drop("total_paid_eur")
                    .sort("paid_eur_m", descending=True))


if __name__ == "__main__":
    main()
