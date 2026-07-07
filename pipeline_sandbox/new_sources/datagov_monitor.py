"""P0-6 — data.gov.ie CKAN metadata monitor (SANDBOX).

Sweeps the CKAN package_search API, snapshots dataset metadata (NOT the datasets
themselves), and emits a discovery/link-rot catalogue. Operational layer, never
an analytical fact. Open CKAN API; no auth.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT, fetch, now_iso, write_silver  # noqa: E402

API = "https://data.gov.ie/api/3/action/package_search"
PAGE = 1000
MAX_PAGES = 30  # safety bound (≈30k datasets); ~22.3k expected


def run() -> None:
    rows: list[dict] = []
    total = None
    fetched_at = now_iso()
    for page in range(MAX_PAGES):
        start = page * PAGE
        payload, meta = fetch(API, params={"rows": PAGE, "start": start})
        data = json.loads(payload)
        if not data.get("success"):
            print(f"  page {page}: API success=false, stopping")
            break
        res = data["result"]
        total = res["count"]
        batch = res["results"]
        if not batch:
            break
        for d in batch:
            org = (d.get("organization") or {})
            resources = d.get("resources") or []
            fmts = sorted({(r.get("format") or "").upper() for r in resources if r.get("format")})
            rows.append({
                "dataset_id": d.get("id"),
                "name": d.get("name"),
                "title": d.get("title"),
                "publisher": org.get("title"),
                "publisher_norm": (org.get("name") or "").lower(),
                "licence_id": d.get("license_id"),
                "metadata_modified": d.get("metadata_modified"),
                "metadata_created": d.get("metadata_created"),
                "num_resources": len(resources),
                "formats": ";".join(fmts),
                "n_tags": len(d.get("tags") or []),
                "source_url": f"https://data.gov.ie/dataset/{d.get('name')}",
                "fetched_at": fetched_at,
                "extraction_method": "api",
                "confidence": "high",
                "privacy_tier": "public",
            })
        print(f"  page {page}: +{len(batch)} (running {len(rows)}/{total})")
        if start + PAGE >= total:
            break

    df = pl.DataFrame(rows)
    out = write_silver("datagov_catalogue", df)

    # Discovery/monitor summaries.
    by_pub = (df.group_by("publisher").len().sort("len", descending=True).head(25))
    fmt_hist = (
        df.select(pl.col("formats").str.split(";").alias("f")).explode("f")
        .filter(pl.col("f") != "").group_by("f").len().sort("len", descending=True).head(20)
    )
    lic_hist = df.group_by("licence_id").len().sort("len", descending=True).head(15)
    stale = (
        df.filter(pl.col("metadata_modified") < "2024-01-01")
        .select(["title", "publisher", "metadata_modified"]).head(50)
    )
    summary = {
        "fetched_at": fetched_at,
        "total_datasets_reported": total,
        "rows_captured": df.height,
        "distinct_publishers": df["publisher"].n_unique(),
        "top_publishers": by_pub.to_dicts(),
        "format_histogram": fmt_hist.to_dicts(),
        "licence_histogram": lic_hist.to_dicts(),
        "stale_sample_pre_2024": stale.to_dicts(),
    }
    sp = ROOT / "datagov_catalogue_summary.json"
    sp.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print(f"\nSILVER: {out}  rows={df.height}")
    print(f"SUMMARY: {sp}")
    print(f"  total datasets reported by CKAN: {total}")
    print(f"  distinct publishers: {df['publisher'].n_unique()}")
    print("  top 5 publishers:")
    for r in by_pub.head(5).to_dicts():
        print(f"    {r['len']:>5}  {r['publisher']}")
    print("  top formats:", ", ".join(f"{r['f']}({r['len']})" for r in fmt_hist.head(8).to_dicts()))


if __name__ == "__main__":
    run()
