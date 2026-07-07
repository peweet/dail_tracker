"""Company entity crosswalk -> committed gold (the organisation-360 spine).

Additive, net-new gold. Does NOT modify or re-baseline any existing output: it reads
the per-register gold as-is and re-normalises the entity names to the CANONICAL key
(shared/name_norm.name_norm_expr) at build time, so the divergent-normaliser bug
(corporate/charity keys were lowercase / missing NFD -> a raw join to supplier_norm
yielded ~0) is bridged HERE without touching the four source extractors.

Anchor = the procurement-supplier universe (keyed on ``supplier_norm``) — the same key
the company dossier page (utility/pages_code/company.py, /company?supplier=) is entered
on. For each supplier it LEFT-joins, on the canonical key, its cross-register presence:
CRO identity, lobbying footprint, corporate-notice count, charity status, EPA licence.
That is exactly the fusion the company page tries to show today but under-matches,
because its corporate panel joins on CRO ``company_num`` only (misses notices that never
got a CRO number but whose name matches a known supplier).

v1 scope is procurement-anchored (the page's universe). A fuller union spine (entities
that appear on the corporate/lobbying registers but are NOT procurement suppliers) +
ministerial-diary panel are the documented follow-ons (doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md).

FRAMING (project rule, feedback_no_inference_in_app): co-occurrence by ENTITY only. A
company appearing on several registers is NOT evidence one caused another — there is no
key linking a specific lobby/meeting to a specific contract. Exact normalised-name / CRO
matching UNDERCOUNTS (subsidiary / trading-name variants missed) and short generic names
can collide; treat counts as floors, not verdicts. Sole traders / individuals are already
excluded upstream (supplier_class filter).

Inputs (all committed gold, read-only):
  data/gold/parquet/procurement_awards.parquet
  data/gold/parquet/procurement_supplier_cro_match.parquet
  data/gold/parquet/procurement_lobbying_overlap.parquet
  data/gold/parquet/corporate_notices.parquet
  data/gold/parquet/charities_enriched.parquet
  data/gold/parquet/epa_supplier_compliance.parquet

Outputs (committed gold):
  data/gold/parquet/supplier_entity_xref.parquet
  data/_meta/supplier_entity_xref_coverage.json

Run:  ./.venv/Scripts/python.exe extractors/entity_xref_build.py
"""

from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from shared.name_norm import name_norm_expr  # noqa: E402

GOLD = ROOT / "data/gold/parquet"
AWARDS = GOLD / "procurement_awards.parquet"
CRO_MATCH = GOLD / "procurement_supplier_cro_match.parquet"
OVERLAP = GOLD / "procurement_lobbying_overlap.parquet"
CORP = GOLD / "corporate_notices.parquet"
CHAR = GOLD / "charities_enriched.parquet"
EPA = GOLD / "epa_supplier_compliance.parquet"
OUT = GOLD / "supplier_entity_xref.parquet"
OUT_COV = ROOT / "data/_meta/supplier_entity_xref_coverage.json"

MIN_LEN = 4  # kill single-token collisions (same floor as procurement_lobbying_xref)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def _suppliers() -> pl.DataFrame:
    """The anchor universe: one row per company-class, non-truncated supplier_norm."""
    aw = pl.read_parquet(AWARDS)
    return (
        aw.filter((pl.col("supplier_class") == "company") & ~pl.col("name_truncated"))
        .filter(pl.col("supplier_norm").str.len_chars() >= MIN_LEN)
        .group_by("supplier_norm")
        .agg(
            pl.col("supplier").mode().first().alias("display_name"),
            pl.len().alias("procurement_award_rows"),
            pl.col("value_eur").filter(pl.col("value_safe_to_sum")).sum().alias("awarded_value_safe_eur"),
        )
    )


def _cro() -> pl.DataFrame:
    return pl.read_parquet(CRO_MATCH, columns=["supplier_norm", "company_num"]).unique(subset=["supplier_norm"])


def _lobbying() -> pl.DataFrame:
    """Total distinct lobbying returns per supplier (overlap is one row per lobby entity)."""
    return (
        pl.read_parquet(OVERLAP, columns=["supplier_norm", "n_lobby_returns"])
        .group_by("supplier_norm")
        .agg(pl.col("n_lobby_returns").sum().alias("lobby_returns"))
    )


def _corporate() -> pl.DataFrame:
    """Corporate-notice count per CANONICAL entity key (re-normed from raw entity_name)."""
    return (
        pl.read_parquet(CORP, columns=["entity_name"])
        .with_columns(supplier_norm=name_norm_expr("entity_name"))
        .filter(pl.col("supplier_norm").str.len_chars() >= MIN_LEN)
        .group_by("supplier_norm")
        .agg(pl.len().alias("corporate_notices"))
    )


def _charities() -> pl.DataFrame:
    return (
        pl.read_parquet(CHAR, columns=["registered_charity_name"])
        .with_columns(supplier_norm=name_norm_expr("registered_charity_name"))
        .filter(pl.col("supplier_norm").str.len_chars() >= MIN_LEN)
        .select("supplier_norm")
        .unique()
        .with_columns(is_charity=pl.lit(value=True))
    )


def _epa() -> pl.DataFrame:
    return (
        pl.read_parquet(EPA, columns=["company_num", "n_licences"])
        .filter(pl.col("n_licences") > 0)
        .select("company_num")
        .unique()
        .with_columns(has_epa_licence=pl.lit(value=True))
    )


def main() -> None:
    sup = _suppliers()
    hr("PROCUREMENT SUPPLIER ANCHOR (company-class, matchable)")
    print(f"distinct suppliers: {sup.height:,}")

    xref = (
        sup.join(_cro(), on="supplier_norm", how="left")
        .join(_lobbying(), on="supplier_norm", how="left")
        .join(_corporate(), on="supplier_norm", how="left")
        .join(_charities(), on="supplier_norm", how="left")
        .join(_epa(), on="company_num", how="left")
        .with_columns(
            pl.col("lobby_returns").fill_null(0),
            pl.col("corporate_notices").fill_null(0),
            pl.col("is_charity").fill_null(value=False),
            pl.col("has_epa_licence").fill_null(value=False),
        )
        .with_columns(
            has_cro=pl.col("company_num").is_not_null(),
            on_lobbying_register=pl.col("lobby_returns") > 0,
            has_corporate_notice=pl.col("corporate_notices") > 0,
        )
        .with_columns(
            # How many registers BEYOND procurement this entity co-occurs on (0-4).
            cross_register_count=(
                pl.col("on_lobbying_register").cast(pl.Int32)
                + pl.col("has_corporate_notice").cast(pl.Int32)
                + pl.col("is_charity").cast(pl.Int32)
                + pl.col("has_epa_licence").cast(pl.Int32)
            )
        )
        .select(
            "supplier_norm",
            "display_name",
            "company_num",
            "has_cro",
            "procurement_award_rows",
            "awarded_value_safe_eur",
            "on_lobbying_register",
            "lobby_returns",
            "has_corporate_notice",
            "corporate_notices",
            "is_charity",
            "has_epa_licence",
            "cross_register_count",
        )
        .sort(["cross_register_count", "awarded_value_safe_eur"], descending=True, nulls_last=True)
    )

    # Row floor: the anchor is thousands of suppliers; a tiny frame means a broken input.
    save_parquet(xref, OUT, min_rows=1000)

    n_multi = int((xref["cross_register_count"] >= 2).sum())
    OUT_COV.write_text(
        json.dumps(
            {
                "supplier_entities": xref.height,
                "with_cro": int(xref["has_cro"].sum()),
                "on_lobbying_register": int(xref["on_lobbying_register"].sum()),
                "with_corporate_notice": int(xref["has_corporate_notice"].sum()),
                "is_charity": int(xref["is_charity"].sum()),
                "has_epa_licence": int(xref["has_epa_licence"].sum()),
                "on_2plus_extra_registers": n_multi,
                "anchor": "procurement supplier universe (supplier_norm), company-class + non-truncated",
                "match_method": "exact CANONICAL normalised-name (shared/name_norm.name_norm_expr) + CRO company_num",
                "caveat": "Co-occurrence by ENTITY only — the SAME organisation appears on several public "
                "registers. NOT evidence one caused another; there is no key linking a specific lobby or "
                "meeting to a specific contract. Exact normalised-name / CRO matching UNDERCOUNTS (subsidiary "
                "and trading-name variants missed) and short generic names can collide — counts are floors, "
                "not verdicts. Sole traders / individuals excluded upstream.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    hr("SUPPLIER ENTITY XREF -> gold")
    print(f"rows: {xref.height:,} | with CRO: {int(xref['has_cro'].sum()):,}")
    print(
        f"lobbying: {int(xref['on_lobbying_register'].sum()):,} | "
        f"corporate notice: {int(xref['has_corporate_notice'].sum()):,} | "
        f"charity: {int(xref['is_charity'].sum()):,} | epa: {int(xref['has_epa_licence'].sum()):,}"
    )
    print(f"on >=2 extra registers: {n_multi:,}")
    pl.Config.set_fmt_str_lengths(34)
    pl.Config.set_tbl_rows(12)
    print(
        xref.filter(pl.col("cross_register_count") >= 2)
        .select("display_name", "company_num", "lobby_returns", "corporate_notices", "is_charity", "has_epa_licence")
        .head(12)
    )
    print(f"\nwrote {OUT}\nwrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
