"""Consolidate the per-publisher payment-grain facts into one gold fact.

The semistate/public-body lane produced several silver facts (lifted out of data/sandbox/
2026-06-12), all sharing an IDENTICAL 28-column schema and NO publisher overlap:
  public_payments_fact (28 publishers) + hse_tusla + nta + nphdb + seai  →  one gold fact.

The 31 local authorities' Purchase-Orders/Payments-over-€20k fact (silver
la_payments_fact, extractors/procurement_la_payments_extract.py) is ALSO folded in here, via a
dedicated conformer (it was already built on the canonical value_kind + realisation_tier
taxonomy). Councils enter the gold fact as publisher_type='local_authority' and surface
through v_procurement_payments automatically.

⚠️ TRIPLE-COUNT TRAP (do not let a consumer sum the whole fact blindly): TII "Road Grant"
rows in public_payments_fact are central→council TRANSFERS (supplier is a council,
supplier_class='public_body'); the LA fact then records that same money flowing council→
contractor. Summing both double-counts it. The guard is at the consuming view/page: exclude
supplier_class='public_body' from spend totals (those rows are transfers/councils-as-payee,
not procurement). The LA contractors are companies, so this fold does not worsen the trap — it
just makes both legs visible in one fact.

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
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/parquet"
CRO = ROOT / "data/silver/cro/companies.parquet"
OUT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/procurement_payments_fact_coverage.json"

# The 31 local authorities' PO/Payments-over-€20k fact (silver, canonical taxonomy already) —
# folded in via _load_la_fact() rather than SOURCE_FACTS (different layer + native value_kind).
LA_FACT = SILVER / "la_payments_fact.parquet"

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
        path = SILVER / fname
        if not path.exists():
            print(f"  WARN missing fact, skipped: {fname}")
            continue
        df = pl.read_parquet(path)
        if base_cols is None:
            base_cols = set(df.columns)
        elif set(df.columns) != base_cols:
            raise SystemExit(f"schema drift in {fname}: +{set(df.columns) - base_cols} -{base_cols - set(df.columns)}")
        frames.append(df)
        print(f"  + {fname:38} {df.height:>7,} rows")
    if not frames:
        raise SystemExit("no payment facts found under data/silver/parquet/")
    return pl.concat(frames, how="vertical")


def _load_la_fact(base: pl.DataFrame) -> pl.DataFrame | None:
    """Conform the local-authority fact to the base 28-col schema so it can
    concat. The LA fact was built on the canonical taxonomy (value_kind ∈ {po_committed,
    payment_actual}); that vocabulary is identical to the base's legacy ``amount_semantics``, so
    the map is lossless and _conform then re-derives value_kind/realisation_tier consistently.

    Privacy: the LA fact carries its own quarantine vocab (public/quarantined); we remap it to
    the consolidated fact's (ok/review_personal_data) and keep the columns as transparency
    metadata. No rows are suppressed — the gold view names suppliers either way (the established
    owner decision), and the LA source is the council's own published over-€20k list.
    """
    if not LA_FACT.exists():
        print("  WARN local-authority fact absent — councils not folded in")
        return None
    la = pl.read_parquet(LA_FACT)
    n, n_la = la.height, la["publisher_name"].n_unique()
    la = la.with_columns(
        pl.col("value_kind").alias("amount_semantics"),  # same vocab; _conform re-derives the 2 axes
        pl.lit("extracted").alias("extraction_status"),
        pl.lit("high").alias("extraction_confidence"),  # reconcile-gated parse, no OCR
        pl.lit(False).alias("caveat_text_detected"),  # LA caveat is config-level (source_caveat), not doc-detected
        pl.col("privacy_status")
        .replace({"quarantined": "review_personal_data", "public": "ok"})
        .alias("privacy_status"),
    )
    missing = set(base.columns) - set(la.columns)
    if missing:
        raise SystemExit(f"LA fact cannot conform — missing base columns: {sorted(missing)}")
    la = la.select(base.columns).cast(dict(base.schema))
    print(f"  + la_payments_fact.parquet (silver)    {n:>7,} rows  [{n_la} local authorities]")
    # Listing-rot guard: a council whose site newly blocks the harvester (bot-wall, moved
    # listing) must not vanish from gold — its published over-€20k disclosures are immutable
    # history (Waterford/Wicklow grew JS challenges 2026-06; Wicklow has no bronze cache to
    # replay). Carry such a council's rows forward from the existing gold fact, loudly.
    if OUT.exists():
        gold = pl.read_parquet(OUT)
        gone = set(gold.filter(pl.col("publisher_type") == "local_authority")["publisher_id"].unique()) - set(
            la["publisher_id"].unique()
        )
        if gone:
            carried = gold.filter(pl.col("publisher_id").is_in(sorted(gone))).select(base.columns).cast(dict(base.schema))
            print(f"  ! listing-rot carry-forward: {sorted(gone)} absent from silver — kept {carried.height:,} gold rows")
            la = pl.concat([la, carried], how="vertical")
    return la


def _conform(df: pl.DataFrame) -> pl.DataFrame:
    # value_kind + realisation_tier from amount_semantics (canonical 2-axis taxonomy)
    kind = pl.col("amount_semantics").replace_strict({k: v[0] for k, v in SEMANTICS_TO_KIND.items()}, default="unknown")
    tier = pl.col("amount_semantics").replace_strict({k: v[1] for k, v in SEMANTICS_TO_KIND.items()}, default="UNKNOWN")
    vat = (
        pl.when(pl.col("publisher_name").is_in(list(VAT_INCLUSIVE_PUBLISHERS)))
        .then(pl.lit("incl_vat"))
        .otherwise(pl.lit("unknown"))
    )
    # Triple-count firewall (operationalised): a public_body RECIPIENT is an intergovernmental
    # transfer / council-as-payee (TII Road Grants central→council; LA payments to Irish Water,
    # ETBs, other councils), NOT summable procurement spend — summing it would double-count the
    # same money against the council→contractor leg. Force value_safe_to_sum=False for them so the
    # single value_safe_to_sum filter (used by every view/page) enforces the guard. Rows stay in
    # the fact, visible; they are merely excluded from spend totals.
    safe = (
        pl.when(pl.col("supplier_class") == "public_body")
        .then(pl.lit(False))
        .otherwise(pl.col("value_safe_to_sum"))
    )
    # Privacy-flag invariant (re-derived, never trusted): public_display must be False for
    # any likely natural person. The base extractor enforces this at write time, but bespoke
    # sandbox parsers have drifted (nta/nphdb/seai reading_order parsers set
    # privacy_status='review_personal_data' yet left public_display=True — 830 such rows
    # reached gold before 2026-06-11, and the base view's public_display gate made them
    # visible). The fold is the last common chokepoint, so the rule lives here too.
    display = (
        pl.col("public_display")
        & (pl.col("supplier_class") != "sole_trader_or_individual")
        & (pl.col("privacy_status") != "review_personal_data")
    )
    return df.with_columns(
        kind.alias("value_kind"),
        tier.alias("realisation_tier"),
        vat.alias("vat_status"),
        safe.alias("value_safe_to_sum"),
        display.alias("public_display"),
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
    base = _load_facts()
    la = _load_la_fact(base)
    df = pl.concat([base, la], how="vertical") if la is not None else base
    df = _conform(df)
    df = _attach_cro(df)

    # PRIVACY INVARIANT (runtime, -O-proof): mirrors procurement_public_body_extract.py —
    # refuse to write gold if any likely-person row is left displayable.
    leaked = df.filter(
        pl.col("public_display")
        & ((pl.col("supplier_class") == "sole_trader_or_individual") | (pl.col("privacy_status") == "review_personal_data"))
    )
    if leaked.height:
        raise SystemExit(
            f"privacy quarantine breached: {leaked.height} likely-person rows left "
            "public_display=True; refusing to write procurement_payments_fact"
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT)
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
        "rows_by_publisher_type": dict(df["publisher_type"].value_counts(sort=True).iter_rows()),
        "n_local_authorities": int(
            df.filter(pl.col("publisher_type") == "local_authority")["publisher_name"].n_unique()
        ),
        "safe_eur_by_tier": {r["realisation_tier"]: round(r["safe_eur"], 2) for r in by_tier.to_dicts()},
        "vat_status_counts": dict(df["vat_status"].value_counts().iter_rows()),
        "privacy_note": "Suppliers named per published source PO/payments-over-€20k lists "
        "(Circular 07/2012 / FOI); no address/PII beyond the published figure.",
        "value_note": "po_committed (ordered) and payment_actual (paid) are different lifecycle "
        "tiers — never summed together; only value_safe_to_sum rows sum, and never across vat_status.",
        "triple_count_note": "Includes both central→council transfers (TII Road Grants, "
        "supplier_class='public_body') and council→contractor LA payments. To avoid double-"
        "counting the same money across those two legs, exclude supplier_class='public_body' "
        "from spend totals at the consuming view/page.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage -> {OUT_COV}")
    print("\nsafe €/tier:", cov["safe_eur_by_tier"])


if __name__ == "__main__":
    main()
