"""PROTOTYPE / EXPLORATION ONLY — do not wire into the pipeline or a page.

Question: does cross-referencing corporate insolvency (Iris notices) against state
procurement (CRO-matched suppliers) surface a real, on-mission accountability signal —
"state contractors that fell into distress" — distinct from the commodity insolvency
feeds (Stubbs Gazette / Vision-Net)?

Join spine (all already in gold):
    procurement_awards            (supplier_norm, value_eur, value_safe_to_sum, buyer, date)
      -> procurement_supplier_cro_match   (supplier_norm -> company_num)   [exact_unique only]
      -> cro_xref_corporate_notices       (company_num -> insolvency notice)

Honesty rails:
  * MATCH: only match_method == "exact_unique" (confidence 0.9). Ambiguous/none dropped.
  * DISTRESS: keep only genuinely-insolvent subtypes. Exclude members_voluntary_liquidation
    (solvent wind-up) and the "*_unspecified" buckets (the ETL couldn't tell) — those would
    mislead. They're reported separately, never as "failures".
  * VALUE: sum only value_safe_to_sum rows (awards are AWARD CEILINGS, not money paid, and
    frameworks/call-offs would double-count). Award value != taxpayer loss.
  * Name-match false positives remain possible even at 0.9 — this is a probe, not gold.

Run:  python pipeline_sandbox/probe_state_contractor_insolvency.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import GOLD_PARQUET_DIR  # noqa: E402

# Genuinely-insolvent subtypes (involuntary / creditor-driven / court / rescue).
DISTRESS = {
    "creditors_voluntary_liquidation",
    "receivership",
    "court_winding_up",
    "examinership",
    "scarp_process_adviser",
}
# Solvent or undetermined — reported separately, NEVER counted as a failure.
NON_DISTRESS = {"members_voluntary_liquidation", "voluntary_liquidation_unspecified", "liquidation_unspecified"}


def _load(name: str) -> pl.DataFrame:
    return pl.read_parquet(GOLD_PARQUET_DIR / f"{name}.parquet")


def main() -> None:
    awards = _load("procurement_awards")
    match = _load("procurement_supplier_cro_match")
    xref = _load("cro_xref_corporate_notices")

    # 1) reliable supplier -> CRO number
    match = match.filter((pl.col("match_method") == "exact_unique") & pl.col("company_num").is_not_null())

    # 2) per-supplier procurement exposure (safe-to-sum only)
    award_date = pl.col("Notice Published Date/Contract Created Date").str.to_date(strict=False)
    awards = awards.with_columns(_award_date=award_date)
    agg = awards.group_by("supplier_norm").agg(
        n_awards=pl.len(),
        safe_value_eur=pl.col("value_eur").filter(pl.col("value_safe_to_sum")).sum(),
        n_buyers=pl.col("Contracting Authority").n_unique(),
        first_award=pl.col("_award_date").min(),
        last_award=pl.col("_award_date").max(),
    )

    # 3) insolvency notices keyed by CRO number (keep earliest event per company)
    insol = xref.filter(pl.col("notice_category").is_in(["corporate_insolvency", "corporate_rescue"])).with_columns(
        _idate=pl.col("issue_date").str.to_date(strict=False),
        is_distress=pl.col("notice_subtype").is_in(list(DISTRESS)),
    )
    earliest = (
        insol.sort("_idate")
        .group_by("company_num")
        .agg(
            entity_name=pl.col("entity_name").first(),
            first_insolvency_date=pl.col("_idate").min(),
            subtypes=pl.col("notice_subtype").unique().str.join(", "),
            any_distress=pl.col("is_distress").any(),
            n_notices=pl.len(),
        )
    )

    # 4) join: state contractor -> CRO -> insolvency
    j = agg.join(match.select(["supplier_norm", "company_num"]), on="supplier_norm", how="inner").join(
        earliest, on="company_num", how="inner"
    )

    distressed = j.filter(pl.col("any_distress"))
    benign = j.filter(~pl.col("any_distress"))

    print("=" * 78)
    print("STATE CONTRACTORS APPEARING IN INSOLVENCY NOTICES (exact_unique CRO match)")
    print("=" * 78)
    print(f"  total matched companies            : {j.height}")
    print(f"  DISTRESSED (insolvent subtypes)    : {distressed.height}")
    print(f"  solvent/undetermined (excluded)    : {benign.height}  <- NOT failures; MVL etc.")
    safe_total = distressed.select(pl.col("safe_value_eur").sum()).item() or 0
    print(f"  safe-to-sum award value (distressed): EUR {safe_total:,.0f}")
    print("    (award ceilings, not money paid; not taxpayer loss)")

    # "won then failed": last award strictly before the insolvency date
    wtf = distressed.filter(
        pl.col("last_award").is_not_null()
        & pl.col("first_insolvency_date").is_not_null()
        & (pl.col("last_award") < pl.col("first_insolvency_date"))
    )
    print(f"\n  won a state contract BEFORE the insolvency event: {wtf.height} companies")

    print("\n  TOP 15 distressed state contractors by safe-to-sum award value:")
    top = distressed.sort("safe_value_eur", descending=True).head(15)
    for r in top.select(
        ["entity_name", "safe_value_eur", "n_awards", "n_buyers", "last_award", "first_insolvency_date", "subtypes"]
    ).iter_rows():
        name = (r[0] or "?")[:34]
        val = f"EUR {r[1]:,.0f}" if r[1] else "EUR 0*"
        gap = ""
        if r[4] and r[5]:
            gap = f" | award->insol {(r[5] - r[4]).days}d"
        print(f"    {name:35s} {val:>16s} | {r[2]:>3}aw {r[3]:>2}buyers | insol {r[5]} | {r[6]}{gap}")

    print("\n  (*EUR 0 = all that supplier's awards were framework/call-off/shared = not safe to sum)")
    print("\n  NOTE: this is a probe. Name matches (even exact) can be wrong; award value is a")
    print("  ceiling not spend; insolvency after contract end is normal. Verify before publishing.")


if __name__ == "__main__":
    main()
