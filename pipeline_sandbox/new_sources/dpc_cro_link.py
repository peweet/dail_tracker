"""DPC decisions → CRO company register link (READ-ONLY, SANDBOX).

DPC enforcement targets are overwhelmingly PRIVATE companies (Airbnb, TikTok,
Meta, WhatsApp, Ryanair…), so the right spine is CRO/Companies, NOT the
public-body crosswalk. Reuses the project's shared company normaliser
(`shared.name_norm.name_norm_expr`) so DPC org names collapse by the IDENTICAL
rule as CRO `company_name` → `name_norm`, which is what makes the exact join land.

Caveat: `name_norm` deliberately drops IRELAND/LIMITED/DAC/UC/GROUP, so it can
collide (many `MEDIAHUIS …` rows fold to `MEDIAHUIS`). We report collisions and
prefer a live ("Normal") company for the display pick — never assert a single
legal entity where the fold is ambiguous.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path("c:/Users/pglyn/PycharmProjects/dail_extractor")))
from _common import now_iso  # noqa: E402
from shared.name_norm import name_norm_expr  # noqa: E402  (reuse the house company rule)

SILVER = Path("c:/tmp/dail_new_sources/silver")
CRO = Path("c:/Users/pglyn/PycharmProjects/dail_extractor/data/silver/cro/companies.parquet")
_PREFIX = re.compile(r"^(?:inquiry|decision)\s+(?:concerning|into|in respect of|relating to)\s+(?:the\s+)?", re.I)
_DATESUFFIX = re.compile(r"\s*[-–]\s*\d{1,2}\s+\w+\s+20\d\d\s*$")


def org_from_title(t: str) -> str:
    t = _PREFIX.sub("", t or "").strip()
    t = _DATESUFFIX.sub("", t).strip()
    return t


def run() -> None:
    dpc = pl.read_parquet(SILVER / "dpc_decisions.parquet")
    dpc = dpc.with_columns(
        pl.col("title").map_elements(org_from_title, return_dtype=pl.Utf8).alias("org_name")
    ).with_columns(name_norm_expr("org_name").alias("org_norm"))

    # CRO register: collapse to name_norm -> best live company (Normal preferred), + count
    cro = (pl.scan_parquet(CRO)
           .select(["company_num", "company_name", "name_norm", "company_status", "company_type"])
           .filter(pl.col("name_norm").is_not_null() & (pl.col("name_norm") != ""))
           .collect())
    cro = cro.with_columns((pl.col("company_status") == "Normal").alias("_live"))
    agg = (cro.sort(["_live", "company_num"], descending=[True, False])
           .group_by("name_norm")
           .agg([
               pl.len().alias("n_cro"),
               pl.col("_live").sum().alias("n_live"),
               pl.col("company_num").first().alias("cro_num"),
               pl.col("company_name").first().alias("cro_name"),
               pl.col("company_status").first().alias("cro_status"),
               pl.col("company_type").first().alias("cro_type"),
           ]))

    linked = dpc.join(agg, left_on="org_norm", right_on="name_norm", how="left")
    matched = linked.filter(pl.col("cro_num").is_not_null())
    dcount = dpc.select("org_norm").unique().height
    dmatched = matched.select("org_norm").unique().height

    print(f"DPC decisions: {dpc.height}  distinct orgs: {dcount}")
    print(f"Matched to CRO (>=1 company): rows {matched.height}/{dpc.height} ({100*matched.height/dpc.height:.0f}%)  "
          f"distinct orgs {dmatched}/{dcount} ({100*dmatched/dcount:.0f}%)")
    print(f"  ambiguous folds (n_cro>1): {matched.filter(pl.col('n_cro')>1).height} rows\n")

    out = (matched.select(["decision_date", "org_name", "org_norm", "cro_num", "cro_name",
                           "cro_status", "cro_type", "n_cro", "n_live", "sector_tags", "source_url"])
           .sort("decision_date", descending=True))
    out.with_columns(pl.lit(now_iso()).alias("linked_at")).write_parquet(
        SILVER / "dpc_cro_matches.parquet", compression="zstd")

    print("  sample DPC → CRO links (most recent):")
    with pl.Config(fmt_str_lengths=34, tbl_rows=14, tbl_cols=-1):
        print(out.select(["decision_date", "org_name", "cro_name", "cro_status", "n_cro"]).head(14))

    unm = linked.filter(pl.col("cro_num").is_null()).select(["org_name"]).unique()
    print(f"\n  unmatched DPC orgs ({unm.height}) — expected for foreign/renamed entities:")
    print("   ", "; ".join(unm["org_name"].head(12).to_list()))


if __name__ == "__main__":
    run()
