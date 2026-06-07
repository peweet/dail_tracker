"""Consolidate the per-publisher payment-grain facts into one gold fact.

The semistate/public-body lane produced several sandbox facts, all sharing an IDENTICAL
28-column schema and NO publisher overlap:
  public_payments_fact (28 publishers) + hse_tusla + nta + nphdb + seai  →  one gold fact.

This is the Stage-D consolidation (see doc/PROCUREMENT_MASTER.md §6). It does four things, all
mechanical — no re-parsing of source documents:
  1. concat the conformed facts (asserting schema identity first);
  2. add ``vat_status`` so totals are never silently summed across different VAT bases
     (HSE/Tusla publish VAT-inclusive; the rest are not confirmed → ``unknown``);
  3. map the legacy ``amount_semantics`` enum onto the canonical 2-axis taxonomy
     (``value_kind`` + ``realisation_tier``) the rest of procurement uses;
  4. attach the CRO company match (same matcher as eTenders/TED — join the already-normalised
     supplier name to data/silver/cro/companies.parquet).

PRIVACY (owner decision 2026-06-06, see PROCUREMENT_MASTER.md §6): suppliers are NAMED,
including sole traders / individuals, because the source documents are official published
PO/payments-over-€20k lists (Circular 07/2012 / FOI) — re-surfacing name+amount+description is
not a new disclosure. We carry the original ``supplier_class`` / ``privacy_status`` columns for
transparency but DO NOT suppress rows. The facts hold no address/PII beyond what is published.

VALUE IS NOT INTERCHANGEABLE: ``po_committed`` (ordered) and ``payment_actual`` (paid) are
different lifecycle tiers — never summed together, and only ``value_safe_to_sum`` rows sum even
within a tier. The views and page enforce one tier per section.

Writes data/gold/parquet/procurement_payments_fact.parquet (+ a coverage JSON). Gold parquets
are gitignore-negated already (!data/gold/parquet/*.parquet), so the output is tracked.
"""

from __future__ import annotations

import contextlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SANDBOX = ROOT / "data/sandbox/parquet"
CRO = ROOT / "data/silver/cro/companies.parquet"
OUT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/procurement_payments_fact_coverage.json"

# The per-publisher facts to fold in. All share the 28-column schema; none overlap.
SOURCE_FACTS = [
    "public_payments_fact.parquet",
    "hse_tusla_payments_fact.parquet",
    "nta_payments_fact.parquet",
    "nphdb_payments_fact.parquet",
    "seai_payments_fact.parquet",
]

# Publishers known to publish VAT-INCLUSIVE figures (mixing bases would corrupt any
# cross-publisher total). Everything else is left 'unknown' rather than assumed exclusive —
# honest, and the "never sum across differing vat_status" rule then holds by default.
VAT_INCLUSIVE_PUBLISHERS = {
    "Health Service Executive",
    "Tusla – Child and Family Agency",
}

# amount_semantics (legacy single enum) → canonical 2-axis taxonomy.
SEMANTICS_TO_KIND = {
    "payment_actual": ("payment_actual", "SPENT"),
    "po_committed": ("po_committed", "COMMITTED"),
}


def _load_facts() -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    base_cols: set[str] | None = None
    for fname in SOURCE_FACTS:
        path = SANDBOX / fname
        if not path.exists():
            print(f"  WARN missing fact, skipped: {fname}")
            continue
        df = pl.read_parquet(path)
        if base_cols is None:
            base_cols = set(df.columns)
        elif set(df.columns) != base_cols:
            raise SystemExit(
                f"schema drift in {fname}: +{set(df.columns) - base_cols} -{base_cols - set(df.columns)}"
            )
        frames.append(df)
        print(f"  + {fname:38} {df.height:>7,} rows")
    if not frames:
        raise SystemExit("no payment facts found under data/sandbox/parquet/")
    return pl.concat(frames, how="vertical")


def _conform(df: pl.DataFrame) -> pl.DataFrame:
    # value_kind + realisation_tier from amount_semantics (canonical 2-axis taxonomy)
    kind = pl.col("amount_semantics").replace_strict(
        {k: v[0] for k, v in SEMANTICS_TO_KIND.items()}, default="unknown"
    )
    tier = pl.col("amount_semantics").replace_strict(
        {k: v[1] for k, v in SEMANTICS_TO_KIND.items()}, default="UNKNOWN"
    )
    vat = (
        pl.when(pl.col("publisher_name").is_in(list(VAT_INCLUSIVE_PUBLISHERS)))
        .then(pl.lit("incl_vat"))
        .otherwise(pl.lit("unknown"))
    )
    return df.with_columns(
        kind.alias("value_kind"),
        tier.alias("realisation_tier"),
        vat.alias("vat_status"),
    )


def _attach_cro(df: pl.DataFrame) -> pl.DataFrame:
    if not CRO.exists():
        print("  WARN CRO register absent — payments will carry no company match")
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("cro_company_num"),
            pl.lit(None, dtype=pl.Utf8).alias("cro_company_status"),
        )
    cro = (
        pl.read_parquet(CRO)
        .select(["name_norm", "company_num", "company_status"])
        .filter(pl.col("name_norm").str.len_chars() >= 4)
        .unique(subset=["name_norm"])
    )
    # Only company-class suppliers are matched (individuals are not CRO-registered).
    return (
        df.join(cro, left_on="supplier_normalised", right_on="name_norm", how="left")
        .with_columns(
            pl.when(pl.col("supplier_class") == "company")
            .then(pl.col("company_num"))
            .otherwise(None)
            .alias("cro_company_num"),
            pl.when(pl.col("supplier_class") == "company")
            .then(pl.col("company_status"))
            .otherwise(None)
            .alias("cro_company_status"),
        )
        .drop(["company_num", "company_status"])
    )


def main() -> None:
    print("Consolidating payment-grain facts → gold:")
    df = _load_facts()
    df = _conform(df)
    df = _attach_cro(df)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)
    print(f"\nwrote {df.height:,} rows / {df['publisher_name'].n_unique()} publishers -> {OUT}")

    safe = df.filter(pl.col("value_safe_to_sum"))
    by_tier = (
        safe.group_by("realisation_tier")
        .agg(pl.col("amount_eur").sum().alias("safe_eur"), pl.len().alias("rows"))
        .sort("safe_eur", descending=True)
    )
    cov = {
        "generated_utc": datetime.now(UTC).isoformat(),
        "layer": "gold",
        "source": "extractors/procurement_payments_consolidate.py",
        "n_rows": df.height,
        "n_publishers": int(df["publisher_name"].n_unique()),
        "n_suppliers": int(df["supplier_normalised"].n_unique()),
        "cro_matched_pct": round(100.0 * df["cro_company_num"].is_not_null().sum() / df.height, 1),
        "safe_eur_by_tier": {r["realisation_tier"]: round(r["safe_eur"], 2) for r in by_tier.to_dicts()},
        "vat_status_counts": dict(df["vat_status"].value_counts().iter_rows()),
        "privacy_note": "Suppliers named per published source PO/payments-over-€20k lists "
        "(Circular 07/2012 / FOI); no address/PII beyond the published figure.",
        "value_note": "po_committed (ordered) and payment_actual (paid) are different lifecycle "
        "tiers — never summed together; only value_safe_to_sum rows sum, and never across vat_status.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage -> {OUT_COV}")
    print("\nsafe €/tier:", cov["safe_eur_by_tier"])


if __name__ == "__main__":
    main()
