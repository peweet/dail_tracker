"""Entity resolution for IPAS accommodation providers — the D1 prerequisite.

Before any operator can be NAMED on a page, its identity must be certain. Three
independent name spaces have to reconcile:
  1. HIQA inspection reports  -> the operator running each centre
  2. dceidy_ipas_legacy_spend -> the payee (Vote 40 / Dept of Children, 2023-24)
  3. procurement_payments_fact -> the payee (Dept of Justice, 2025+)

Uses the HOUSE normaliser (`shared.name_norm.name_norm_str`) — NOT a local fold — so
these keys join the CRO register and every other entity surface in the project.

DISCIPLINE:
- `match_confidence` on every row. Only `exact` is publishable (D1 option b).
- Known FRAGMENTS are merged only where the source itself shows one entity under two
  payee strings (e.g. IGO EMERGENCY MANAGEMENT SERVICES x2). Each merge is declared
  explicitly below, never inferred by fuzzy distance.
- The Ukraine/IP `stream` filter is MANDATORY on the DCEDIY parquet. Without it Cape
  Wrath reads as a EUR 46m IP provider when its IP spend is EUR 10.9m.
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.name_norm import name_norm_str  # noqa: E402

from _common import SILVER, now_iso  # noqa: E402

GOLD = Path("c:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet")
DCEDIY = GOLD / "dceidy_ipas_legacy_spend.parquet"
PAY = GOLD / "procurement_payments_fact.parquet"

# ⚠️ HOUSE-NORMALISER DEFECT FOUND (project-wide, not just IPAS):
# `shared/name_norm.py::LEGAL_SUFFIX_PATTERN` strips LIMITED/LTD/DAC/PLC/CLG/UC/
# "UNLIMITED COMPANY" — but NOT **ULC**. So "Townbe ULC" -> "TOWNBE ULC" while
# "Townbe Ltd" -> "TOWNBE": the same company yields two different join keys.
# ULC (unlimited company) is a common Irish form, so EVERY ULC silently fails to
# join CRO/payments/compliance across the whole project. Patched LOCALLY here rather
# than editing the shared module mid-flight (its expr/str twins are pinned equal by
# test/shared/test_name_norm.py — changing it needs its own change + rebaseline).
_ULC_RE = __import__("re").compile(r"\bULC\b")

# Declared fragment merges: ONE entity appearing under several payee strings in the
# SOURCE. Each is a human-verifiable statement about the data, never a fuzzy guess.
# (Two apparent pairs were REJECTED as unevidenced: DOUBLE PROPERTY GROUP vs DOUBLE
#  PROPERTY SERVICES, and ARAMARK vs ARAMARK IRELAND — plausible, but nothing in the
#  sources proves they are the same legal entity. They stay separate.)
FRAGMENT_MERGES = {
    "IGO EMERGENCY MANAGEMENT SERVICES IGO CAFE": "IGO EMERGENCY MANAGEMENT SERVICES",
    "ON SITE FACILITIES MANAGEMENT": "ONSITE FACILITIES MANAGEMENT",
    "ONSITE FACILITIES": "ONSITE FACILITIES MANAGEMENT",
    "COZIQ ENTREPRISES": "COZIQ ENTERPRISES",                            # source typo
    "DIDEAN DACHAS EIREANN TEORANTA": "DIDEAN DOCHAS EIREANN TEORANTA",  # source typo
    "MOSNEY HOLIDAYS": "MOSNEY",
    "BRIDGESTOCK CARE": "BRIDGESTOCK",
}
# Where a source typo won the mode(), force the correct spelling for display.
DISPLAY_FIX = {
    "DIDEAN DOCHAS EIREANN TEORANTA": "Dídean Dóchas Éireann Teoranta",
}


def key(s: object) -> str:
    k = _ULC_RE.sub("", name_norm_str(s)).strip()
    k = " ".join(k.split())
    return FRAGMENT_MERGES.get(k, k)


def main() -> None:
    # ---- 1. HIQA: who OPERATES each centre ----
    comp = pl.read_parquet(SILVER / "hiqa_centre_compliance.parquet")
    pcol = next(c for c in comp.columns if c == "provider_name")
    jcol = "judgment"
    hiqa = (comp.with_columns(pl.col(pcol).map_elements(key, return_dtype=pl.Utf8).alias("entity_key"))
                .group_by("entity_key")
                .agg([
                    pl.col(pcol).mode().first().alias("operator_name"),
                    pl.col("centre_name").n_unique().alias("centres"),
                    pl.col("county").unique().alias("counties"),
                    pl.len().alias("judgments"),
                    (pl.col(jcol).str.to_lowercase().str.starts_with("not")).sum().alias("not_compliant"),
                ])
                .with_columns((pl.col("not_compliant") / pl.col("judgments") * 100)
                              .round(1).alias("pct_not_compliant")))

    # ---- 2. DCEDIY Vote 40, 2023-24, INTERNATIONAL PROTECTION ONLY ----
    dce = (pl.read_parquet(DCEDIY)
             .filter(pl.col("stream") == "International Protection")   # MANDATORY
             .with_columns(pl.col("provider").map_elements(key, return_dtype=pl.Utf8).alias("entity_key"))
             .group_by("entity_key", "year")
             .agg(pl.col("amount_eur").sum().alias("eur"),
                  pl.col("provider").mode().first().alias("paid_as_dcediy")))
    dce_tot = (dce.group_by("entity_key")
                  .agg(pl.col("eur").sum().alias("dcediy_ip_eur"),
                       pl.col("paid_as_dcediy").mode().first()))

    # ---- 3. Dept of Justice payments (IPAS from 1 May 2025) ----
    doj = (pl.scan_parquet(PAY)
             .filter(pl.col("publisher_id") == "dept_justice")
             .select("supplier_normalised", "amount_eur")
             .collect()
             .with_columns(pl.col("supplier_normalised")
                             .map_elements(key, return_dtype=pl.Utf8).alias("entity_key"))
             .group_by("entity_key")
             .agg(pl.col("amount_eur").sum().alias("doj_eur"),
                  pl.col("supplier_normalised").mode().first().alias("paid_as_doj")))

    res = (hiqa.join(dce_tot, on="entity_key", how="full", coalesce=True)
               .join(doj, on="entity_key", how="full", coalesce=True))

    res = res.with_columns([
        pl.col("operator_name").is_not_null().alias("has_compliance"),
        (pl.col("dcediy_ip_eur").is_not_null() | pl.col("doj_eur").is_not_null()).alias("has_money"),
        pl.coalesce("operator_name", "paid_as_dcediy", "paid_as_doj").alias("display_name"),
    ]).with_columns(
        pl.col("entity_key").replace(DISPLAY_FIX).alias("_fixed"),
    ).with_columns(
        pl.when(pl.col("_fixed") != pl.col("entity_key"))
          .then(pl.col("_fixed")).otherwise(pl.col("display_name")).alias("display_name"),
    ).drop("_fixed").with_columns(
        pl.when(pl.col("has_compliance") & pl.col("has_money"))
          .then(pl.lit("exact"))            # same house name_norm key on both sides
          .when(pl.col("has_compliance"))
          .then(pl.lit("compliance_only"))
          .otherwise(pl.lit("payment_only")).alias("match_confidence")
    ).with_columns([
        pl.lit(now_iso()).alias("derived_at"),
        pl.lit("shared.name_norm.name_norm_str + declared fragment merges").alias("extraction_method"),
        pl.lit(False).alias("value_safe_to_sum"),
        pl.lit("DCEDIY filtered to stream='International Protection' (Ukraine EXCLUDED — "
               "unfiltered, Cape Wrath reads EUR 46m vs its true EUR 10.9m IP spend). "
               "Compliance window 2024-01..2026-03; money DCEDIY 2023-24 + DoJ 2025+. "
               "DIFFERENT WINDOWS — co-occurrence only, NEVER causal.").alias("caveat"),
        pl.lit("provider names inherit accommodation-providers public_display gating")
          .alias("join_caveat"),
    ])

    out = SILVER / "ipas_entity_resolution.parquet"
    res.write_parquet(out, compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    res.drop("counties").write_csv(SILVER / "_eyeball" / "ipas_entity_resolution.csv")

    print(f"wrote {out} — {res.height} entities")
    print(res.group_by("match_confidence").len().sort("len", descending=True))
    pub = res.filter(pl.col("match_confidence") == "exact")
    print(f"\n>>> PUBLISHABLE (exact — compliance AND money on the same house key): {pub.height}")
    with pl.Config(tbl_rows=30, fmt_str_lengths=34, tbl_width_chars=150):
        print(pub.select("display_name", "centres", "judgments", "pct_not_compliant",
                         "dcediy_ip_eur", "doj_eur")
                 .with_columns([(pl.col("dcediy_ip_eur") / 1e6).round(2).alias("dcediy_ip_m"),
                                (pl.col("doj_eur") / 1e6).round(2).alias("doj_m")])
                 .drop("dcediy_ip_eur", "doj_eur")
                 .sort("pct_not_compliant", descending=True))
    print("\n--- compliance record but NO money matched (investigate, do NOT imply unpaid) ---")
    with pl.Config(tbl_rows=15, fmt_str_lengths=40):
        print(res.filter(pl.col("match_confidence") == "compliance_only")
                 .select("display_name", "centres", "pct_not_compliant")
                 .sort("centres", descending=True))


if __name__ == "__main__":
    main()
