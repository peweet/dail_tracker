"""DPC decision routing: public-body spine (RPSB) vs company spine (CRO).

Shows that DPC targets split cleanly by type — Public Authorities route to the
public-body crosswalk, private companies to CRO — using the `sector_tags` the
DPC site already provides as the natural router. Read-only.
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_body_crosswalk import load_reference, match_one, norm_body  # noqa: E402
from dpc_cro_link import org_from_title  # noqa: E402

SILVER = Path("c:/tmp/dail_new_sources/silver")


def bucket(tags: str) -> str:
    t = (tags or "").lower()
    if "public" in t or "government" in t or "university" in t or "garda" in t or "health sector/public" in t:
        return "Public authority"
    if "private" in t or "bank" in t or "insurance" in t or "company" in t:
        return "Private company"
    return "Other/unclear"


def main() -> None:
    _ref, idx, dept_aliases = load_reference()
    dpc = pl.read_parquet(SILVER / "dpc_decisions.parquet")
    cro_urls = set(pl.read_parquet(SILVER / "dpc_cro_matches.parquet")["source_url"].to_list())

    recs = []
    for r in dpc.iter_rows(named=True):
        org = org_from_title(r["title"])
        body_hit = match_one(org, idx, dept_aliases)["match_tier"] is not None
        recs.append({
            "sector_bucket": bucket(r["sector_tags"]),
            "on_body_spine": body_hit,
            "on_cro_spine": r["source_url"] in cro_urls,
        })
    d = pl.DataFrame(recs).with_columns(
        (pl.col("on_body_spine") | pl.col("on_cro_spine")).alias("routed")
    )
    n = d.height
    print(f"DPC decisions: {n}")
    print(f"  routed to a spine (body OR cro): {d['routed'].sum()} ({100*d['routed'].sum()/n:.0f}%)")
    print(f"    · body spine only: {d.filter(pl.col('on_body_spine') & ~pl.col('on_cro_spine')).height}")
    print(f"    · cro spine only:  {d.filter(~pl.col('on_body_spine') & pl.col('on_cro_spine')).height}")
    print(f"    · both:            {d.filter(pl.col('on_body_spine') & pl.col('on_cro_spine')).height}")
    print(f"    · neither (foreign/renamed): {d.filter(~pl.col('routed')).height}")
    print("\n  routing by DPC sector tag:")
    ct = (d.group_by("sector_bucket").agg([
        pl.len().alias("n"),
        pl.col("on_body_spine").sum().alias("body"),
        pl.col("on_cro_spine").sum().alias("cro"),
        pl.col("routed").sum().alias("routed"),
    ]).sort("n", descending=True))
    for r in ct.to_dicts():
        print(f"    {r['sector_bucket']:18} n={r['n']:>2}  body={r['body']:>2}  cro={r['cro']:>2}  routed={r['routed']:>2}")


if __name__ == "__main__":
    main()
