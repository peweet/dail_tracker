"""PRE-ETL (sandbox): award <-> realised-spend linkage view.

Joins the three procurement datasets at the SUPPLIER-ENTITY level to answer
"this firm won €X in tenders — how much was it actually paid?". They are DIFFERENT
lifecycle stages (award ceiling vs realised spend) so this is a JOIN, never a sum
(see [[project_procurement_ted_overlap]] — never add the value columns together).

Sides:
  - realised SPEND  : data/sandbox/parquet/public_payments_fact.parquet (+ bespoke nphdb/seai/nta)
                      value_safe_to_sum rows only; EXCLUDES supplier_class==public_body (councils —
                      the €2.44bn TII "Road Grant" inter-governmental transfers) and the
                      extraction_confidence=='low' blank-supplier rows.
  - eTenders AWARDS : data/gold/parquet/procurement_awards.parquet (value_safe_to_sum)
  - TED AWARDS      : data/silver/parquet/ted_ie_awards.parquet (value_safe_to_sum)

Entity key is HYBRID: CRO company_num when available (most reliable, catches name variants
like BAM Building / BAM Contractors), else the normalised name. CRO numbers come from
procurement_supplier_cro_match.parquet (spend + eTenders side) and ted's own cro_company_num.

Output: data/sandbox/parquet/procurement_award_spend_link.parquet (one row per supplier entity)
        + data/_meta/procurement_award_spend_link_summary.json

Run: ./.venv/Scripts/python.exe extractors/procurement_award_spend_link.py
"""

from __future__ import annotations

import contextlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SPEND_PARQUETS = ["public_payments_fact", "nphdb_payments_fact", "seai_payments_fact", "nta_payments_fact"]
CRO_MATCH = ROOT / "data/gold/parquet/procurement_supplier_cro_match.parquet"
ETENDERS = ROOT / "data/gold/parquet/procurement_awards.parquet"
TED = ROOT / "data/silver/parquet/ted_ie_awards.parquet"
OUT = ROOT / "data/sandbox/parquet/procurement_award_spend_link.parquet"
OUT_SUMMARY = ROOT / "data/_meta/procurement_award_spend_link_summary.json"


def _entity(cro_col: str, norm_col: str) -> pl.Expr:
    """Hybrid entity key: CRO number when present, else 'N:'+normalised name."""
    return (
        pl.when(pl.col(cro_col).is_not_null() & (pl.col(cro_col).cast(pl.Utf8).str.strip_chars() != ""))
        .then(pl.lit("CRO:") + pl.col(cro_col).cast(pl.Utf8))
        .otherwise(pl.lit("N:") + pl.col(norm_col))
        .alias("entity")
    )


def load_spend() -> pl.DataFrame:
    parts = []
    for p in SPEND_PARQUETS:
        fp = ROOT / f"data/sandbox/parquet/{p}.parquet"
        if fp.exists():
            d = pl.read_parquet(fp)
            keep = ["supplier_normalised", "supplier_raw", "supplier_class", "amount_eur",
                    "value_safe_to_sum", "extraction_confidence", "publisher_id"]
            parts.append(d.select([c for c in keep if c in d.columns]))
    pay = pl.concat(parts, how="vertical_relaxed")
    clean = pay.filter(
        pl.col("value_safe_to_sum")
        & (pl.col("supplier_class") != "public_body")          # councils / inter-gov transfers
        & (pl.col("extraction_confidence") != "low")           # blank-supplier flagged rows
        & pl.col("supplier_normalised").is_not_null()
        & (pl.col("supplier_normalised").str.strip_chars() != "")
    )
    cro = (pl.read_parquet(CRO_MATCH)
           .filter(pl.col("company_num").is_not_null())
           .select(["supplier_norm", "company_num", "company_status"]).unique("supplier_norm"))
    clean = clean.join(cro, left_on="supplier_normalised", right_on="supplier_norm", how="left")
    clean = clean.with_columns(_entity("company_num", "supplier_normalised"))
    return clean.group_by("entity").agg(
        pl.col("supplier_raw").drop_nulls().first().alias("spend_name"),
        pl.col("supplier_normalised").first().alias("spend_norm"),
        pl.col("company_num").first().alias("company_num"),
        pl.col("company_status").first().alias("company_status"),
        pl.col("amount_eur").sum().alias("realised_spend_eur"),
        pl.len().alias("spend_rows"),
        pl.col("publisher_id").n_unique().alias("n_spend_publishers"),
    )


def load_etenders() -> pl.DataFrame:
    aw = pl.read_parquet(ETENDERS).filter(pl.col("value_safe_to_sum"))
    cro = (pl.read_parquet(CRO_MATCH)
           .filter(pl.col("company_num").is_not_null())
           .select(["supplier_norm", "company_num"]).unique("supplier_norm"))
    aw = aw.join(cro, on="supplier_norm", how="left").with_columns(_entity("company_num", "supplier_norm"))
    return aw.group_by("entity").agg(
        pl.col("supplier").drop_nulls().first().alias("etenders_name"),
        pl.col("value_eur").sum().alias("etenders_award_eur"),
        pl.len().alias("etenders_awards"),
    )


def load_ted() -> pl.DataFrame:
    ted = pl.read_parquet(TED).filter(pl.col("value_safe_to_sum") & pl.col("winner_name_norm").is_not_null())
    ted = ted.with_columns(_entity("cro_company_num", "winner_name_norm"))
    return ted.group_by("entity").agg(
        pl.col("winner_name").drop_nulls().first().alias("ted_name"),
        pl.col("award_value_eur").sum().alias("ted_award_eur"),
        pl.len().alias("ted_awards"),
    )


def main() -> None:
    spend = load_spend()
    et = load_etenders()
    ted = load_ted()
    print(f"{'=' * 78}\nAWARD <-> SPEND LINKAGE\n{'=' * 78}")
    print(f"entities — spend {spend.height:,} | eTenders {et.height:,} | TED {ted.height:,}")

    link = (spend.join(et, on="entity", how="full", coalesce=True)
            .join(ted, on="entity", how="full", coalesce=True))
    link = link.with_columns([
        pl.col("realised_spend_eur").fill_null(0.0),
        pl.col("etenders_award_eur").fill_null(0.0),
        pl.col("ted_award_eur").fill_null(0.0),
        (pl.col("spend_rows").fill_null(0) > 0).alias("in_spend"),
        (pl.col("etenders_awards").fill_null(0) > 0).alias("in_etenders"),
        (pl.col("ted_awards").fill_null(0) > 0).alias("in_ted"),
        pl.coalesce(["spend_name", "etenders_name", "ted_name"]).alias("supplier_name"),
        pl.col("entity").str.starts_with("CRO:").alias("keyed_by_cro"),
    ]).with_columns(
        (pl.col("etenders_award_eur") + pl.col("ted_award_eur")).alias("total_award_eur"),
    ).with_columns(
        (pl.col("in_spend") & (pl.col("in_etenders") | pl.col("in_ted"))).alias("award_and_spend"),
        # realised/award ratio — only meaningful when both sides present and award>0
        pl.when((pl.col("total_award_eur") > 0) & (pl.col("realised_spend_eur") > 0))
        .then(pl.col("realised_spend_eur") / pl.col("total_award_eur"))
        .otherwise(None).alias("spend_to_award_ratio"),
    )

    cols = ["entity", "supplier_name", "company_num", "company_status", "keyed_by_cro",
            "in_spend", "in_etenders", "in_ted", "award_and_spend",
            "realised_spend_eur", "total_award_eur", "etenders_award_eur", "ted_award_eur",
            "spend_to_award_ratio", "spend_rows", "n_spend_publishers", "etenders_awards", "ted_awards",
            "spend_norm"]
    link = link.select([c for c in cols if c in link.columns]).sort("realised_spend_eur", descending=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    link.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)

    both = link.filter(pl.col("award_and_spend"))
    spend_only = link.filter(pl.col("in_spend") & ~pl.col("in_etenders") & ~pl.col("in_ted"))
    print(f"\nentities total: {link.height:,}")
    print(f"  in spend: {int(link['in_spend'].sum()):,} | in eTenders: {int(link['in_etenders'].sum()):,} | in TED: {int(link['in_ted'].sum()):,}")
    print(f"  AWARD+SPEND (linkable): {both.height:,}  -> realised €{both['realised_spend_eur'].sum():,.0f} vs awards €{both['total_award_eur'].sum():,.0f}")
    print(f"  spend-only (no award, sub-threshold long tail): {spend_only.height:,} -> €{spend_only['realised_spend_eur'].sum():,.0f}")
    print(f"  keyed by CRO number: {int(link['keyed_by_cro'].sum()):,} / {link.height:,}")
    print("\nTop award+spend suppliers (realised vs awarded):")
    for r in both.head(10).iter_rows(named=True):
        print(f"  {str(r['supplier_name'])[:34]:<34} spend €{r['realised_spend_eur']:>13,.0f} | award €{r['total_award_eur']:>13,.0f} | x{r['spend_to_award_ratio'] or 0:.2f}")

    summary = {
        "entities_total": link.height,
        "in_spend": int(link["in_spend"].sum()),
        "in_etenders": int(link["in_etenders"].sum()),
        "in_ted": int(link["in_ted"].sum()),
        "award_and_spend_entities": both.height,
        "award_and_spend_realised_eur": float(both["realised_spend_eur"].sum()),
        "award_and_spend_award_eur": float(both["total_award_eur"].sum()),
        "spend_only_entities": spend_only.height,
        "spend_only_eur": float(spend_only["realised_spend_eur"].sum()),
        "keyed_by_cro": int(link["keyed_by_cro"].sum()),
        "clean_spend_total_eur": float(link["realised_spend_eur"].sum()),
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "Award value and realised spend are DIFFERENT lifecycle stages (ceiling vs paid) — "
                  "NEVER sum the value columns together. Spend side EXCLUDES public_body suppliers "
                  "(councils / TII inter-gov transfers) and extraction_confidence=low rows. "
                  "Entity key is CRO company_num where available else normalised name; CRO coverage "
                  "is partial so name-keyed entities may still be the same firm under a variant spelling.",
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT}\nwrote {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
