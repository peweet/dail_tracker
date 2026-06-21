"""EXPERIMENTAL sandbox prototype — council PO spend → AFS service-division bridge (v2).

Question: of the purchase-order / payment money a council publishes (named suppliers),
how much can be tied to one of the 8 audited Local-Authority service divisions, and how
does that named-supplier-visible spend compare against the *audited* cost of that service
(revenue + capital) from the council's Annual Financial Statement?

NOTHING here touches gold or the pipeline. Reads three existing facts, attributes each PO
row to a division in two passes, and writes coverage + a candidate bridge to this sandbox
dir only.

Attribution (two passes, confidence-tagged so the UI can show / hide the weaker pass):
  1. keyword           — conservative keyword match on spend_category (fallback description).
                         A road contract and a housing contract both say "Contract Payment",
                         so only distinctive vocab maps; the rest stays blank on pass 1.
  2. supplier_dominant — a supplier whose pass-1 *attributable* spend is >=80% one division
                         (and >= EUR 50k total) is assumed that division on its otherwise
                         generic ("Contract Payments") rows too. Supplier specialisation,
                         not a guess from thin air; flagged distinctly from keyword.
  (anything left)      — (unattributable): never guessed.

Honesty rails (same as the live page):
  * PO/payment cash commitments and AFS audited accruals are DIFFERENT grains — shown side
    by side, never summed into one number. AFS here = revenue + capital so the comparison
    is not biased low against capital-heavy PO spend (roads/housing).
  * sum-safe rows only, supplier_class='public_body' excluded (intergovernmental transfers,
    see consolidate triple_count_note).
"""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
PAY = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
AFS_REV = ROOT / "data/silver/parquet/la_afs_divisions.parquet"
AFS_CAP = ROOT / "data/silver/parquet/la_afs_capital_divisions.parquet"
OUT = Path(__file__).resolve().parent

# Canonical 8 LA service divisions (must match la_afs_*divisions.division verbatim).
DIV_HOUSING = "Housing and Building"
DIV_ROADS = "Roads, Transportation and Safety"
DIV_WATER = "Water Services"
DIV_DEV = "Development Management"
DIV_ENV = "Environmental Services"
DIV_REC = "Recreation and Amenity"
DIV_AGR = "Agriculture, Education, Health & Welfare"
DIV_MISC = "Miscellaneous Services"
UNATTRIB = "(unattributable)"

# Supplier-dominant backfill thresholds.
DOM_SHARE = 0.80
DOM_MIN_EUR = 50_000

# Conservative, order-sensitive keyword rules. First match wins. Generic words
# ("contract", "construction", "minor contract", "professional fees") are intentionally
# NOT mapped.
RULES: list[tuple[str, str]] = [
    (r"\broad|\bsurfac|\bfootpath|\bpavement|\bbridge|\btraffic|\bcarriagew|\bn\d{1,2}\b|\bbitumen|\bgrit", DIV_ROADS),
    (r"\bwater|\bsewer|\bdrainage|\bwastewater|\bwwtp|\beffluent|\bwell\b", DIV_WATER),
    (r"\bhous|\bhouse\b|\bdwelling|\btenant|\bestate\b|\bras\b|\bhap\b|\bvoid\b|\bregenerat", DIV_HOUSING),
    (r"\bwaste|\blitter|\brecycl|\blandfill|\bburial|\bcemeter|\bgraveyard|\bfire\b|\bcivil defence", DIV_ENV),
    (r"\blibrar|\bpark\b|\bplayground|\bsport|\bleisure|\barts?\b|\bheritage|\bmuseum|\bswimming", DIV_REC),
    (r"\bplanning|\benterprise|\beconomic dev|\btourism|\bderelict|\bvacant site", DIV_DEV),
    (r"\bveterinar|\bagricultur|\beducation\b|\bschool\b|\bscholarship|\bhigher educat", DIV_AGR),
]


def keyword_div(col: pl.Expr) -> pl.Expr:
    expr = pl.lit(UNATTRIB)
    text = col.fill_null("").str.to_lowercase()
    for pat, div in reversed(RULES):  # reversed so RULES[0] wins
        expr = pl.when(text.str.contains(pat)).then(pl.lit(div)).otherwise(expr)
    return expr


def load_la() -> pl.DataFrame:
    f = pl.read_parquet(PAY)
    return f.filter(
        (pl.col("publisher_type") == "local_authority")
        & pl.col("value_safe_to_sum")
        & (pl.col("supplier_class") != "public_body")
    )


def attribute(la: pl.DataFrame) -> pl.DataFrame:
    sig = pl.coalesce([pl.col("spend_category"), pl.col("description")])
    la = la.with_columns(keyword_div(sig).alias("kw_div"))

    # Pass 2: supplier dominant division from pass-1 attributable rows.
    att = la.filter(pl.col("kw_div") != UNATTRIB)
    sup = (
        att.group_by(["supplier_normalised", "kw_div"])
        .agg(pl.col("amount_eur").sum().alias("e"))
        .sort("e", descending=True)
    )
    dom = (
        sup.group_by("supplier_normalised")
        .agg(
            pl.col("kw_div").first().alias("dom_div"),
            pl.col("e").first().alias("dom_e"),
            pl.col("e").sum().alias("all_e"),
        )
        .with_columns((pl.col("dom_e") / pl.col("all_e")).alias("dom_share"))
        .filter((pl.col("dom_share") >= DOM_SHARE) & (pl.col("all_e") >= DOM_MIN_EUR))
        .select("supplier_normalised", "dom_div")
    )
    la = la.join(dom, on="supplier_normalised", how="left")
    la = la.with_columns(
        pl.when(pl.col("kw_div") != UNATTRIB)
        .then(pl.col("kw_div"))
        .when(pl.col("dom_div").is_not_null())
        .then(pl.col("dom_div"))
        .otherwise(pl.lit(UNATTRIB))
        .alias("service_division"),
        pl.when(pl.col("kw_div") != UNATTRIB)
        .then(pl.lit("keyword"))
        .when(pl.col("dom_div").is_not_null())
        .then(pl.lit("supplier_dominant"))
        .otherwise(pl.lit("unattributable"))
        .alias("attribution_method"),
    )
    return la


def afs_combined() -> pl.DataFrame:
    rev = pl.read_parquet(AFS_REV).select(
        "council", "year", "division",
        pl.col("gross_expenditure").alias("afs_rev_gross_eur"),
    )
    cap = pl.read_parquet(AFS_CAP).select(
        "council", "year", "division",
        pl.col("capital_expenditure").alias("afs_cap_eur"),
    )
    return rev.join(cap, on=["council", "year", "division"], how="full", coalesce=True).with_columns(
        (pl.col("afs_rev_gross_eur").fill_null(0) + pl.col("afs_cap_eur").fill_null(0)).alias("afs_total_eur")
    )


def main() -> None:
    la = attribute(load_la())
    total = la["amount_eur"].sum()

    by = (
        la.group_by("service_division")
        .agg(pl.len().alias("n_rows"), pl.col("amount_eur").sum().alias("eur"))
        .with_columns((pl.col("eur") / total * 100).round(1).alias("pct_eur"))
        .sort("eur", descending=True)
    )
    by_method = (
        la.group_by("attribution_method")
        .agg(pl.col("amount_eur").sum().alias("eur"))
        .with_columns((pl.col("eur") / total * 100).round(1).alias("pct_eur"))
        .sort("eur", descending=True)
    )
    attributed_pct = round(
        la.filter(pl.col("service_division") != UNATTRIB)["amount_eur"].sum() / total * 100, 1
    )

    print("=== PO spend by inferred service division (LA, sum-safe, excl public_body) ===")
    print(f"total in scope: EUR {total/1e9:.2f}bn across {la.height:,} rows")
    print(f"ATTRIBUTED to a service: {attributed_pct}% of EUR")
    print(by)
    print("\n=== by attribution method ===")
    print(by_method)

    # Bridge: per council x year x division — named-supplier PO vs AFS revenue+capital.
    afs = afs_combined()
    po = (
        la.filter(pl.col("service_division") != UNATTRIB)
        .group_by(["publisher_name", "year", "service_division"])
        .agg(
            pl.col("amount_eur").sum().alias("po_named_supplier_eur"),
            (pl.col("amount_eur").filter(pl.col("attribution_method") == "keyword").sum()).alias("po_keyword_eur"),
        )
        .rename({"publisher_name": "council", "service_division": "division"})
    )
    bridge = afs.join(po, on=["council", "year", "division"], how="inner").with_columns(
        (pl.col("po_named_supplier_eur") / pl.col("afs_total_eur") * 100).round(1).alias("po_vs_afs_total_pct")
    )
    print("\n=== Bridge sample (Roads, vs AFS revenue+capital) ===")
    print(
        bridge.filter(pl.col("division") == DIV_ROADS)
        .sort("year", descending=True)
        .select("council", "year", "afs_total_eur", "po_named_supplier_eur", "po_vs_afs_total_pct")
        .head(12)
    )

    cov = {
        "note": "EXPERIMENTAL sandbox prototype v2, not gold. Grains never summed.",
        "scope": "LA sum-safe rows, supplier_class != public_body",
        "n_rows_in_scope": la.height,
        "total_eur_in_scope": float(total),
        "attributed_pct_eur": attributed_pct,
        "by_attribution_method": by_method.to_dicts(),
        "by_division": by.to_dicts(),
        "supplier_dominant_thresholds": {"min_share": DOM_SHARE, "min_eur": DOM_MIN_EUR},
        "bridge_rows": bridge.height,
    }
    (OUT / "bridge_coverage.json").write_text(json.dumps(cov, indent=2), encoding="utf-8")
    bridge.write_parquet(OUT / "spend_service_bridge.parquet")
    print(f"\nwrote {OUT/'bridge_coverage.json'} and spend_service_bridge.parquet")


if __name__ == "__main__":
    main()
