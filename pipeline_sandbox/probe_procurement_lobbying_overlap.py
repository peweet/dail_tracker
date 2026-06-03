"""SANDBOX PROBE (investigation): procurement x lobbying overlap.

Surfaces a *verifiable disclosure*, not an accusation: distinct procurement
suppliers whose normalised name also appears on the lobbying.ie register, either
as a registered lobbyist (`lobbyist_name` in returns_master) or as a client on
whose behalf a firm lobbied (`client_name` in client_company_returns_detail).

For each overlap we attach BOTH sides:
  procurement -> # award rows, # distinct contracting authorities, top authority
  lobbying    -> # returns, role (lobbyist / client / both), sample policy areas

Framing (project rule, see feedback_no_inference_in_app):
  "Won N contracts AND filed M lobbying returns" is a fact. It is NOT evidence of
  influence or wrongdoing. No causal language; lobbying targets named officials,
  procurement records contracting authorities, so there is no clean body<->body
  key yet -- this is co-occurrence by ENTITY, deliberately conservative.

Inputs:
  data/sandbox/parquet/procurement_awards.parquet            (from procurement_etenders_extract.py)
  data/silver/lobbying/parquet/returns_master.parquet
  data/silver/lobbying/parquet/client_company_returns_detail.parquet

Outputs (sandbox only, NOT wired into pipeline.py):
  data/sandbox/parquet/procurement_lobbying_overlap.parquet

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_lobbying_overlap.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from cro_normalise import name_norm_expr  # noqa: E402

AWARDS = ROOT / "data/sandbox/parquet/procurement_awards.parquet"
LOBBY_MASTER = ROOT / "data/silver/lobbying/parquet/returns_master.parquet"
LOBBY_CLIENT = ROOT / "data/silver/lobbying/parquet/client_company_returns_detail.parquet"
OUT = ROOT / "data/sandbox/parquet/procurement_lobbying_overlap.parquet"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main() -> None:
    aw = pl.read_parquet(AWARDS)
    # procurement side: company-class, non-truncated suppliers only (privacy + data quality)
    proc = (
        aw.filter((pl.col("supplier_class") == "company") & ~pl.col("name_truncated"))
        .group_by("supplier_norm")
        .agg(
            pl.col("supplier").mode().first().alias("supplier"),
            pl.len().alias("n_award_rows"),
            pl.col("Contracting Authority").n_unique().alias("n_authorities"),
            pl.col("Contracting Authority").mode().first().alias("top_authority"),
        )
        .filter(pl.col("supplier_norm").str.len_chars() >= 4)
    )
    hr("PROCUREMENT SUPPLIERS (company-class, matchable)")
    print(f"distinct suppliers: {proc.height:,}")

    # lobbying side: registrants and clients, normalised to the same key
    reg = (
        pl.read_parquet(LOBBY_MASTER)
        .select(pl.col("lobbyist_name").alias("name"), "primary_key")
        .filter(pl.col("name").is_not_null())
        .with_columns(name_norm_expr("name").alias("norm"))
        .group_by("norm")
        .agg(pl.col("primary_key").n_unique().alias("n_lobbyist_returns"))
    )
    cli = (
        pl.read_parquet(LOBBY_CLIENT)
        .select(pl.col("client_name").alias("name"), "primary_key", "policy_areas")
        .filter(pl.col("name").is_not_null())
        .with_columns(name_norm_expr("name").alias("norm"))
        .group_by("norm")
        .agg(
            pl.col("primary_key").n_unique().alias("n_client_returns"),
            pl.col("policy_areas").drop_nulls().unique().str.concat("; ").alias("policy_areas"),
        )
    )

    # join: a supplier may appear as lobbyist, client, or both
    ov = (
        proc.join(reg, left_on="supplier_norm", right_on="norm", how="left")
        .join(cli, left_on="supplier_norm", right_on="norm", how="left")
        .with_columns(
            pl.col("n_lobbyist_returns").fill_null(0),
            pl.col("n_client_returns").fill_null(0),
        )
        .filter((pl.col("n_lobbyist_returns") > 0) | (pl.col("n_client_returns") > 0))
        .with_columns(
            pl.when((pl.col("n_lobbyist_returns") > 0) & (pl.col("n_client_returns") > 0))
            .then(pl.lit("lobbyist_and_client"))
            .when(pl.col("n_lobbyist_returns") > 0)
            .then(pl.lit("registered_lobbyist"))
            .otherwise(pl.lit("lobbying_client"))
            .alias("lobby_role"),
            (pl.col("n_lobbyist_returns") + pl.col("n_client_returns")).alias("n_lobby_returns"),
        )
        .sort(["n_award_rows", "n_lobby_returns"], descending=True)
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    ov.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)

    hr("OVERLAP: suppliers that ALSO appear on the lobbying register")
    print(f"overlapping entities: {ov.height:,} of {proc.height:,} suppliers "
          f"({ov.height / proc.height:.1%})")
    print(ov.group_by("lobby_role").len().sort("len", descending=True))

    hr("TOP OVERLAPS BY CONTRACT ACTIVITY (disclosure, not accusation)")
    pl.Config.set_fmt_str_lengths(38)
    pl.Config.set_tbl_rows(25)
    print(
        ov.select("supplier", "n_award_rows", "n_authorities", "lobby_role", "n_lobby_returns", "top_authority").head(25)
    )
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
