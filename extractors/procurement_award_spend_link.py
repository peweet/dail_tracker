"""PRE-ETL (sandbox): award <-> realised-spend linkage view.

Joins the three procurement datasets at the SUPPLIER-ENTITY level to answer
"this firm won €X in tenders — how much was it actually paid?". They are DIFFERENT
lifecycle stages (award ceiling vs realised spend) so this is a JOIN, never a sum
(see [[project_procurement_ted_overlap]] — never add the value columns together).

Sides:
  - realised SPEND  : data/silver/parquet/public_payments_fact.parquet (+ bespoke nphdb/seai/nta)
                      value_safe_to_sum rows only; EXCLUDES supplier_class==public_body (councils —
                      the €2.44bn TII "Road Grant" inter-governmental transfers) and the
                      extraction_confidence=='low' blank-supplier rows.
  - eTenders AWARDS : data/gold/parquet/procurement_awards.parquet (value_safe_to_sum)
  - TED AWARDS      : data/silver/parquet/ted_ie_awards.parquet (2023+, API winners) UNION
                      data/silver/parquet/ted_ie_winner_history.parquet (2016-2023, per-notice
                      XML — the "old tenders" the API carries no winners for). Without this second
                      layer ~⅔ of pre-2024 TED award winners are invisible and payments to them
                      look unlinked (money-linkage was ~30%, this lifts it to ~42%).

Entity key is HYBRID: CRO company_num when available (most reliable, merges name variants),
else the normalised name. ALL THREE sides resolve to CRO the SAME way — an exact-unique join of
their normalised name against the CRO register (data/silver/cro/companies.parquet) — so the same
firm gets one company_num everywhere (e.g. Turner & Townsend's "&"/"And"/"Ltd" spellings collapse
to one CRO:102886 entity). This relies on shared.name_norm.name_norm_expr dropping BOTH '&' and
the word 'and' (fixed 2026-06-04); the register's name_norm uses the identical rule.

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
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SPEND_PARQUETS = ["public_payments_fact", "nphdb_payments_fact", "seai_payments_fact", "nta_payments_fact"]
CRO_REGISTER = ROOT / "data/silver/cro/companies.parquet"
ETENDERS = ROOT / "data/gold/parquet/procurement_awards.parquet"
TED = ROOT / "data/silver/parquet/ted_ie_awards.parquet"
TED_WINNER_HISTORY = ROOT / "data/silver/parquet/ted_ie_winner_history.parquet"
OUT = ROOT / "data/sandbox/parquet/procurement_award_spend_link.parquet"
OUT_SUMMARY = ROOT / "data/_meta/procurement_award_spend_link_summary.json"


def load_cro_map() -> pl.DataFrame:
    """EXACT-UNIQUE normalised-name -> CRO company_num map, applied identically to all three sides
    so the SAME firm resolves to the SAME company_num everywhere. Names mapping to >1 company_num
    (dissolved+active duplicates) are dropped — a wrong merge is worse than a missed one.
    Relies on shared.name_norm.name_norm_expr dropping both '&' and the word 'and' so "X & Y" /
    "X And Y" already collapse before this join (the register's name_norm uses the SAME rule)."""
    cro = (
        pl.read_parquet(CRO_REGISTER)
        .select(["name_norm", "company_num", "company_status"])
        .filter(pl.col("name_norm").is_not_null() & (pl.col("name_norm").str.len_chars() >= 4))
    )
    counts = cro.group_by("name_norm").agg(pl.col("company_num").n_unique().alias("n"))
    unique_names = counts.filter(pl.col("n") == 1).select("name_norm")
    return (
        cro.join(unique_names, on="name_norm", how="inner")
        .unique("name_norm")
        .select(["name_norm", "company_num", "company_status"])
    )


def attach_cro(df: pl.DataFrame, norm_col: str, cro_map: pl.DataFrame) -> pl.DataFrame:
    """Left-join a CRO company_num via the normalised-name column."""
    return df.join(cro_map.rename({"name_norm": norm_col}), on=norm_col, how="left")


def _entity(cro_col: str, norm_col: str) -> pl.Expr:
    """Hybrid entity key: CRO number when present, else 'N:'+normalised name."""
    return (
        pl.when(pl.col(cro_col).is_not_null() & (pl.col(cro_col).cast(pl.Utf8).str.strip_chars() != ""))
        .then(pl.lit("CRO:") + pl.col(cro_col).cast(pl.Utf8))
        .otherwise(pl.lit("N:") + pl.col(norm_col))
        .alias("entity")
    )


def load_spend(cro_map: pl.DataFrame) -> pl.DataFrame:
    parts = []
    for p in SPEND_PARQUETS:
        fp = ROOT / f"data/silver/parquet/{p}.parquet"
        if fp.exists():
            d = pl.read_parquet(fp)
            keep = [
                "supplier_normalised",
                "supplier_raw",
                "supplier_class",
                "amount_eur",
                "value_safe_to_sum",
                "extraction_confidence",
                "publisher_id",
            ]
            parts.append(d.select([c for c in keep if c in d.columns]))
    pay = pl.concat(parts, how="vertical_relaxed")
    clean = pay.filter(
        pl.col("value_safe_to_sum")
        & (pl.col("supplier_class") != "public_body")  # councils / inter-gov transfers
        & (pl.col("extraction_confidence") != "low")  # blank-supplier flagged rows
        & pl.col("supplier_normalised").is_not_null()
        & (pl.col("supplier_normalised").str.strip_chars() != "")
    )
    clean = attach_cro(clean, "supplier_normalised", cro_map)
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


def load_etenders(cro_map: pl.DataFrame) -> pl.DataFrame:
    aw = pl.read_parquet(ETENDERS).filter(pl.col("value_safe_to_sum"))
    aw = attach_cro(aw, "supplier_norm", cro_map).with_columns(_entity("company_num", "supplier_norm"))
    return aw.group_by("entity").agg(
        pl.col("supplier").drop_nulls().first().alias("etenders_name"),
        pl.col("value_eur").sum().alias("etenders_award_eur"),
        pl.len().alias("etenders_awards"),
    )


def load_ted(cro_map: pl.DataFrame) -> pl.DataFrame:
    # Union BOTH TED winner layers: ted_ie_awards (2023+, API) and ted_ie_winner_history
    # (2016-2023, per-notice XML). The API returns NO winner for pre-2024 notices, so without the
    # history layer most "old tender" winners are missing and their payments look unlinked. The two
    # layers share the (publication_number, winner_name_norm) grain and overlap on ~1 notice (the
    # 2023 boundary) — dedupe so a notice-winner is never counted twice.
    keep = ["publication_number", "winner_name", "winner_name_norm", "award_value_eur", "value_safe_to_sum"]
    frames = []
    for fp in (TED, TED_WINNER_HISTORY):
        if fp.exists():
            d = pl.read_parquet(fp)
            frames.append(d.select([c for c in keep if c in d.columns]))
    ted = pl.concat(frames, how="vertical_relaxed")
    ted = ted.filter(pl.col("value_safe_to_sum") & pl.col("winner_name_norm").is_not_null()).unique(
        subset=["publication_number", "winner_name_norm"]
    )
    # re-derive CRO from the shared map (not ted's own cro_company_num) so the key matches the
    # other two sides exactly.
    ted = attach_cro(ted.drop("company_num", strict=False), "winner_name_norm", cro_map)
    ted = ted.with_columns(_entity("company_num", "winner_name_norm"))
    return ted.group_by("entity").agg(
        pl.col("winner_name").drop_nulls().first().alias("ted_name"),
        pl.col("award_value_eur").sum().alias("ted_award_eur"),
        pl.len().alias("ted_awards"),
    )


def main() -> None:
    cro_map = load_cro_map()
    spend = load_spend(cro_map)
    et = load_etenders(cro_map)
    ted = load_ted(cro_map)
    print(f"{'=' * 78}\nAWARD <-> SPEND LINKAGE\n{'=' * 78}")
    print(f"CRO exact-unique name->company map: {cro_map.height:,} names")
    print(f"entities — spend {spend.height:,} | eTenders {et.height:,} | TED {ted.height:,}")

    link = spend.join(et, on="entity", how="full", coalesce=True).join(ted, on="entity", how="full", coalesce=True)
    link = (
        link.with_columns(
            [
                pl.col("realised_spend_eur").fill_null(0.0),
                pl.col("etenders_award_eur").fill_null(0.0),
                pl.col("ted_award_eur").fill_null(0.0),
                (pl.col("spend_rows").fill_null(0) > 0).alias("in_spend"),
                (pl.col("etenders_awards").fill_null(0) > 0).alias("in_etenders"),
                (pl.col("ted_awards").fill_null(0) > 0).alias("in_ted"),
                pl.coalesce(["spend_name", "etenders_name", "ted_name"]).alias("supplier_name"),
                pl.col("entity").str.starts_with("CRO:").alias("keyed_by_cro"),
            ]
        )
        .with_columns(
            (pl.col("etenders_award_eur") + pl.col("ted_award_eur")).alias("total_award_eur"),
        )
        .with_columns(
            (pl.col("in_spend") & (pl.col("in_etenders") | pl.col("in_ted"))).alias("award_and_spend"),
            # realised/award ratio — only meaningful when both sides present and award>0
            pl.when((pl.col("total_award_eur") > 0) & (pl.col("realised_spend_eur") > 0))
            .then(pl.col("realised_spend_eur") / pl.col("total_award_eur"))
            .otherwise(None)
            .alias("spend_to_award_ratio"),
        )
    )

    cols = [
        "entity",
        "supplier_name",
        "company_num",
        "company_status",
        "keyed_by_cro",
        "in_spend",
        "in_etenders",
        "in_ted",
        "award_and_spend",
        "realised_spend_eur",
        "total_award_eur",
        "etenders_award_eur",
        "ted_award_eur",
        "spend_to_award_ratio",
        "spend_rows",
        "n_spend_publishers",
        "etenders_awards",
        "ted_awards",
        "spend_norm",
    ]
    link = link.select([c for c in cols if c in link.columns]).sort("realised_spend_eur", descending=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(link, OUT)

    both = link.filter(pl.col("award_and_spend"))
    spend_only = link.filter(pl.col("in_spend") & ~pl.col("in_etenders") & ~pl.col("in_ted"))
    print(f"\nentities total: {link.height:,}")
    print(
        f"  in spend: {int(link['in_spend'].sum()):,} | in eTenders: {int(link['in_etenders'].sum()):,} | in TED: {int(link['in_ted'].sum()):,}"
    )
    print(
        f"  AWARD+SPEND (linkable): {both.height:,}  -> realised €{both['realised_spend_eur'].sum():,.0f} vs awards €{both['total_award_eur'].sum():,.0f}"
    )
    print(
        f"  spend-only (no award, sub-threshold long tail): {spend_only.height:,} -> €{spend_only['realised_spend_eur'].sum():,.0f}"
    )
    print(f"  keyed by CRO number: {int(link['keyed_by_cro'].sum()):,} / {link.height:,}")
    print("\nTop award+spend suppliers (realised vs awarded):")
    for r in both.head(10).iter_rows(named=True):
        print(
            f"  {str(r['supplier_name'])[:34]:<34} spend €{r['realised_spend_eur']:>13,.0f} | award €{r['total_award_eur']:>13,.0f} | x{r['spend_to_award_ratio'] or 0:.2f}"
        )

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
