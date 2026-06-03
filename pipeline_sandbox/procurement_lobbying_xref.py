"""procurement x lobbying cross-reference -> committed gold.

Promoted from probe_procurement_lobbying_overlap.py (cbi/cro xref pattern): a
self-contained script run as the `procurement_lobbying` pipeline chain AFTER both
`procurement` (gold awards) and `lobbying` (silver returns) have produced their
outputs. Does the normalised-name join in Polars (name_norm_expr) and bakes the
result to gold, so the SQL views and both pages only ever SELECT.

What it produces (one row per matched lobbying entity, keyed for BOTH consumers):
  - lobby_name        raw lobbying display name (registrant OR client) -> the
                      Lobbying page joins v_lobbying_org_index.lobbyist_name on this
  - supplier_norm     normalised key (= name_norm_expr) -> the future Procurement
                      page joins its supplier rows on this
  - supplier          procurement display name
  - lobby_side        'registrant' | 'client'
  - n_lobby_returns   distinct lobbying returns for that name+side
  - n_award_rows, n_authorities, awarded_value_safe_eur  (procurement side)

Framing (project rule, see feedback_no_inference_in_app): co-occurrence by ENTITY
only. A company appears on BOTH the procurement and lobbying registers. NOT
evidence that lobbying influenced any contract -- no shared key links a specific
lobby to a specific award. Exact-name match undercounts (variants missed).

Inputs:
  data/gold/parquet/procurement_awards.parquet           (procurement chain)
  data/silver/lobbying/parquet/returns_master.parquet    (lobbying chain)
  data/silver/lobbying/parquet/client_company_returns_detail.parquet

Outputs (committed gold):
  data/gold/parquet/procurement_lobbying_overlap.parquet
  data/_meta/procurement_lobbying_overlap_coverage.json

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/procurement_lobbying_xref.py
"""

from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from cro_normalise import name_norm_expr  # noqa: E402

AWARDS = ROOT / "data/gold/parquet/procurement_awards.parquet"
LOBBY_MASTER = ROOT / "data/silver/lobbying/parquet/returns_master.parquet"
LOBBY_CLIENT = ROOT / "data/silver/lobbying/parquet/client_company_returns_detail.parquet"
PROC_COV = ROOT / "data/_meta/procurement_coverage.json"
OUT = ROOT / "data/gold/parquet/procurement_lobbying_overlap.parquet"
OUT_COV = ROOT / "data/_meta/procurement_lobbying_overlap_coverage.json"

# An overlap row joins TWO sources -> cite BOTH so each page's footer can attribute.
SOURCES = {
    "procurement": "see data/_meta/procurement_coverage.json (Office of Government Procurement, CC-BY 4.0)",
    "lobbying": {
        "dataset": "Register of Lobbying returns",
        "publisher": "Standards in Public Office Commission (SIPO)",
        "landing_page": "https://www.lobbying.ie/",
        "license": "Re-use permitted with attribution (lobbying.ie)",
    },
}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def _proc_supplier_summary() -> pl.DataFrame:
    """Per normalised supplier: company-class, non-truncated, with safe-to-sum value."""
    aw = pl.read_parquet(AWARDS)
    return (
        aw.filter((pl.col("supplier_class") == "company") & ~pl.col("name_truncated"))
        .filter(pl.col("supplier_norm").str.len_chars() >= 4)
        .group_by("supplier_norm")
        .agg(
            pl.col("supplier").mode().first().alias("supplier"),
            pl.len().alias("n_award_rows"),
            pl.col("Contracting Authority").n_unique().alias("n_authorities"),
            pl.col("value_eur").filter(pl.col("value_safe_to_sum")).sum().alias("awarded_value_safe_eur"),
        )
    )


def _lobby_side(path: Path, name_col: str, side: str) -> pl.DataFrame:
    """Per raw lobbying display name: normalised key + distinct return count."""
    return (
        pl.read_parquet(path)
        .select(pl.col(name_col).alias("lobby_name"), "primary_key")
        .filter(pl.col("lobby_name").is_not_null() & (pl.col("lobby_name").str.strip_chars() != ""))
        .with_columns(name_norm_expr("lobby_name").alias("norm"))
        .group_by(["lobby_name", "norm"])
        .agg(pl.col("primary_key").n_unique().alias("n_lobby_returns"))
        .with_columns(pl.lit(side).alias("lobby_side"))
    )


def main() -> None:
    proc = _proc_supplier_summary()
    hr("PROCUREMENT SUPPLIERS (company-class, matchable)")
    print(f"distinct suppliers: {proc.height:,}")

    reg = _lobby_side(LOBBY_MASTER, "lobbyist_name", "registrant")
    cli = _lobby_side(LOBBY_CLIENT, "client_name", "client")

    overlap = (
        pl.concat([reg, cli])
        .join(proc, left_on="norm", right_on="supplier_norm", how="inner")
        .rename({"norm": "supplier_norm"})
        .select(
            "lobby_name",
            "lobby_side",
            "supplier_norm",
            "supplier",
            "n_lobby_returns",
            "n_award_rows",
            "n_authorities",
            "awarded_value_safe_eur",
        )
        .sort(["n_award_rows", "n_lobby_returns"], descending=True)
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    overlap.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)

    proc_retrieved = None
    if PROC_COV.exists():
        proc_retrieved = json.loads(PROC_COV.read_text(encoding="utf-8")).get("retrieved_utc")
    OUT_COV.write_text(
        json.dumps(
            {
                "overlap_rows": overlap.height,
                "distinct_suppliers_in_overlap": overlap["supplier_norm"].n_unique(),
                "registrant_matches": int((overlap["lobby_side"] == "registrant").sum()),
                "client_matches": int((overlap["lobby_side"] == "client").sum()),
                "procurement_suppliers_matched_against": proc.height,
                "match_method": "exact normalised-name (name_norm_expr) on company-class, non-truncated suppliers",
                "procurement_retrieved_utc": proc_retrieved,
                "sources": SOURCES,
                "caveat": "Co-occurrence by ENTITY only: a company appears on BOTH the procurement and "
                          "lobbying registers. NOT evidence that lobbying influenced any contract -- there "
                          "is no shared key linking a specific lobby to a specific award. Exact-name match "
                          "undercounts (subsidiary/trading-name variants are missed).",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    hr("OVERLAP -> gold")
    print(f"rows: {overlap.height:,} | distinct suppliers: {overlap['supplier_norm'].n_unique():,}")
    print(overlap.group_by("lobby_side").len().sort("len", descending=True))
    pl.Config.set_fmt_str_lengths(34)
    pl.Config.set_tbl_rows(15)
    print(overlap.select("lobby_name", "lobby_side", "supplier", "n_award_rows", "n_lobby_returns").head(15))
    print(f"\nwrote {OUT}\nwrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
