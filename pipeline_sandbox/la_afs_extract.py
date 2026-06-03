"""INGEST (Phase 0, sandbox): PER-LA Annual Financial Statements — Income & Expenditure
by SERVICE DIVISION, one row per (council, year, division).

The per-council counterpart to the national amalgamated layer
(afs_amalgamated_extract.py → afs_amalgamated_divisions.parquet, all-31-summed). Per-LA is
the per-constituency prize: the gov.ie/datacatalogue dataset is amalgamated-only
(Open Data: No), so each council's own audited AFS PDF must be harvested off its own site.
Feasibility census + plan: doc/PER_LA_AFS_BUILD_PLAN.md.

PHASE 0 = the 9 councils whose I&E-by-division page the (strict) national finder already
locates. Reuses afs_amalgamated_extract wholesale (to_num, DIVISIONS, parse_ie,
find_ie_page) + procurement_la_seed fetch (requests + curl fallback for WAF/TLS). Per
council: harvest AFS PDFs from the landing page → select (audited > unaudited, latest
title-year, ≥30pp) → download to bronze → find the I&E page → parse 8 divisions →
reconcile Σ gross vs the printed total → emit rows tagged scope=single-LA.

Distinct fact from afs_amalgamated_divisions (national) and la_payments_fact (cash-PO
grain) — accrual net-expenditure by division; a third, NON-UNIONED fact. Do not reconcile
across the three (different grains).

Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/la_afs_extract.py            # full Phase-0 ingest
  ./.venv/Scripts/python.exe pipeline_sandbox/la_afs_extract.py --only meath,donegal
  ./.venv/Scripts/python.exe pipeline_sandbox/la_afs_extract.py --list     # harvest/select only
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import config  # noqa: E402

# reuse the amalgamated extractor wholesale — same statement, same parser
from afs_amalgamated_extract import DIVISIONS, find_ie_page, parse_ie  # noqa: E402
from procurement_la_seed import HREF_RE, fetch_bytes, fetch_text  # noqa: E402

CACHE = config.BRONZE_PDF_DIR / "la_afs"
OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_afs_divisions.parquet"
OUT_COV = ROOT / "data/_meta/la_afs_coverage.json"

YEAR_RE = re.compile(r"20[12]\d")
AFS_LINK = re.compile(r"(annual[-_ %]?financial|\bafs\b|financial[-_ %]?statement)", re.I)
NAV_AFS = re.compile(r"financ|statement|afs|budget|publication|account", re.I)

# council -> harvest config. landing[] tried in order (first that yields AFS pdfs wins);
# Galway County via gaillimh.ie alt (galwaycoco WAF); Meath reached by curl fallback.
# entity: county | city | merged. These 9 = Phase-0 (strict finder already passes).
REGISTRY: list[dict] = [
    {"council": "South Dublin", "slug": "south_dublin", "entity": "county", "region": "Dublin",
     "landing": ["https://www.sdcc.ie/en/services/our-council/policies-and-plans/budgets-and-spending/financial-statements/"]},
    {"council": "Cork City", "slug": "cork_city", "entity": "city", "region": "Munster",
     "landing": ["https://www.corkcity.ie/en/council-services/public-info/spending-and-revenue/"]},
    {"council": "Cork County", "slug": "cork_county", "entity": "county", "region": "Munster",
     "landing": ["https://www.corkcoco.ie/en/council/accessibility-maps-and-publications/annual-financial-statements"]},
    {"council": "Westmeath", "slug": "westmeath", "entity": "county", "region": "Leinster",
     "landing": ["https://www.westmeathcoco.ie/en/ourservices/finance/"]},
    {"council": "Galway City", "slug": "galway_city", "entity": "city", "region": "Connacht",
     "landing": ["https://www.galwaycity.ie/services/finance-services/budgets-and-financial-publications"]},
    {"council": "Galway County", "slug": "galway_county", "entity": "county", "region": "Connacht",
     "landing": ["https://www.gaillimh.ie/en/finance/financial-publications/annual-financial-statements"]},
    {"council": "Meath", "slug": "meath", "entity": "county", "region": "Leinster",
     "landing": ["https://www.meath.ie/council/your-council/finance-and-procurement"]},
    {"council": "Donegal", "slug": "donegal", "entity": "county", "region": "Ulster",
     "landing": ["https://www.donegalcoco.ie/services/other-services/finance/"]},
    {"council": "Tipperary", "slug": "tipperary", "entity": "county", "region": "Munster",
     "landing": ["https://www.tipperarycoco.ie/finance/financial-reports"]},
]


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def title_year(url: str) -> int:
    """Year from the FILENAME (the statement's year), not the folder path — a 2026-07
    upload folder can hold the 'AFS 2024' doc. Take the year token in the basename."""
    name = url.rsplit("/", 1)[-1]
    yrs = [int(y) for y in YEAR_RE.findall(name)]
    return max(yrs) if yrs else 0


def harvest_afs(landing: str) -> list[str]:
    """AFS PDF links from the landing page; else one-hop crawl of finance/statement nav."""
    html = fetch_text(landing)
    if not html:
        return []

    def scan(h: str, base: str) -> list[str]:
        out = []
        for href in HREF_RE.findall(h):
            low = href.lower().split("?")[0]
            if low.endswith(".pdf") and (AFS_LINK.search(href) or ("statement" in low and YEAR_RE.search(href))):
                out.append(urljoin(base, href))
        return out

    hits = scan(html, landing)
    if not hits:
        host = urlparse(landing).netloc
        subs, seen = [], set()
        for href in HREF_RE.findall(html):
            full = urljoin(landing, href)
            if urlparse(full).netloc != host or full == landing or full.lower().split("?")[0].endswith(".pdf"):
                continue
            if NAV_AFS.search(href) and full not in seen:
                seen.add(full)
                subs.append(full)
        for s in subs[:8]:
            sh = fetch_text(s)
            if sh:
                hits.extend(scan(sh, s))
    seen, uniq = set(), []
    for u in hits:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def select_afs(urls: list[str]) -> str | None:
    """Prefer AUDITED over unaudited, then the latest title-year. (Page-count ≥30 is
    enforced post-download in ingest_council.)"""
    if not urls:
        return None
    def key(u: str) -> tuple[int, int]:
        audited = 1 if re.search(r"audited", u, re.I) and not re.search(r"unaudited", u, re.I) else 0
        return (audited, title_year(u))
    return max(urls, key=key)


def download(slug: str, url: str, year: int) -> Path | None:
    dest = CACHE / slug / f"{year or 'latest'}.pdf"
    if dest.exists() and dest.stat().st_size > 20000:
        return dest
    b = fetch_bytes(url)
    if not b or b[:4] != b"%PDF":
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b)
    return dest


def ingest_council(cf: dict, list_only: bool) -> tuple[list[dict], dict]:
    urls: list[str] = []
    for landing in cf["landing"]:
        urls = harvest_afs(landing)
        if urls:
            break
    picked = select_afs(urls)
    stat = {"council": cf["council"], "n_afs": len(urls), "picked": picked,
            "year": title_year(picked) if picked else None}
    if list_only or not picked:
        stat["status"] = "no-afs" if not picked else "listed"
        return [], stat

    year = title_year(picked)
    p = download(cf["slug"], picked, year)
    if not p:
        stat["status"] = "download-fail(WAF?)"
        return [], stat
    doc = fitz.open(p)
    npages = doc.page_count
    if npages < 30:  # AFS file-selector guard — a short doc is a summary, not the AFS
        doc.close()
        stat["status"] = f"too-short({npages}pp)"
        return [], stat
    pg = find_ie_page(doc)
    if pg is None:
        doc.close()
        stat["status"] = "no-IE-page"
        return [], stat
    ie, total = parse_ie(doc[pg].get_text("text"))
    doc.close()
    gross_sum = sum(v[0] for v in ie.values() if v[0])
    reconciled = bool(total and abs(gross_sum - total[0]) < 100_000)
    rows = [{
        "council": cf["council"], "slug": cf["slug"], "entity": cf["entity"],
        "region": cf["region"], "year": year, "division": canon,
        "gross_expenditure": v[0], "income": v[1], "net_expenditure": v[2],
        "net_expenditure_prior_yr": v[3], "source_file_url": picked,
        "source_page_number": pg, "printed_total_eur": (total[0] if total else None),
        "reconciled": reconciled,
    } for canon, v in ie.items()]
    stat.update(status="ok" if len(ie) == 8 else f"{len(ie)}/8", divisions=len(ie),
                gross_sum=gross_sum, reconciled=reconciled, pages=npages, ie_page=pg)
    return rows, stat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="comma-separated slugs")
    ap.add_argument("--list", action="store_true", help="harvest/select only, no parse")
    args = ap.parse_args()
    only = {x.strip().lower() for x in args.only.split(",") if x.strip()} or None
    pubs = [c for c in REGISTRY if not only or c["slug"] in only or c["council"].lower() in only]

    hr(f"PER-LA AFS — Phase 0 ingest ({len(pubs)} councils)")
    all_rows, stats = [], []
    for cf in pubs:
        rows, stat = ingest_council(cf, args.list)
        all_rows.extend(rows)
        stats.append(stat)
        recon = "EXACT/✓" if stat.get("reconciled") else ("✗" if "gross_sum" in stat else "-")
        print(f"  {cf['council']:<16} {stat['status']:<16} "
              f"div={stat.get('divisions','-')}/8  recon={recon}  "
              f"yr={stat.get('year','-')}  pg={stat.get('ie_page','-')}")

    if args.list or not all_rows:
        if not all_rows and not args.list:
            print("\nno rows extracted")
        return

    df = pl.DataFrame(all_rows).with_columns(
        pl.lit("SPENT").alias("realisation_tier"),
        pl.lit("net_expenditure_actual").alias("value_kind"),
        pl.lit("single-LA (per-council audited AFS)").alias("scope"),
        pl.lit("Local Authority audited AFS (own website), Local Government Audit Service").alias("source"),
    ).sort(["council", "year", "division"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)

    hr("DATA-QUALITY — per council")
    ok = [s for s in stats if s.get("status") == "ok"]
    recon_ok = [s for s in stats if s.get("reconciled")]
    print(f"  councils with 8/8 divisions : {len(ok)}/{len(pubs)}")
    print(f"  councils reconciled to print : {len(recon_ok)}/{len(pubs)}")
    print(f"  rows: {df.height}  ({df['council'].n_unique()} councils × ~8 divisions)")

    cov = {
        "councils_attempted": len(pubs),
        "councils_with_rows": df["council"].n_unique(),
        "councils_8of8": len(ok),
        "councils_reconciled": len(recon_ok),
        "rows": df.height,
        "phase": 0,
        "by_council": stats,
        "realisation_tier": "SPENT", "value_kind": "net_expenditure_actual",
        "scope": "per-council audited AFS (not amalgamated, not cash-PO)",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "Per-LA accrual net-expenditure by service division. Sum only within a "
                  "(council, year). NEVER reconcile against afs_amalgamated_divisions "
                  "(national) or la_payments_fact (cash-PO/payment grain) — different grains.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {OUT_PARQUET}\n      {OUT_COV}")


if __name__ == "__main__":
    main()
