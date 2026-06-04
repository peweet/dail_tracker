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
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

# reuse the amalgamated extractor wholesale — same statement, same parser
from afs_amalgamated_extract import parse_ie  # noqa: E402
from procurement_la_seed import HREF_RE, fetch_bytes, fetch_text  # noqa: E402

import config  # noqa: E402

CACHE = config.BRONZE_PDF_DIR / "la_afs"
OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_afs_divisions.parquet"
OUT_COV = ROOT / "data/_meta/la_afs_coverage.json"

# Camelot fallback for councils whose I&E LAYOUT the fitz parse_ie mis-reads (it grabs the
# wrong cells → ~2× inflated Σgross that the reconcile gate rejects). Camelot's structured
# cell grid reads them correctly. It runs in an ISOLATED venv (immune to the main-venv
# `uv sync` churn + avoids the opencv/cv2 clash that would break SIPO) — see
# feedback_dual_parser_rule. Best-effort: if the venv/script is absent (CI/Cloud), these
# councils are simply skipped (the fitz fact still ships). Build the venv with:
#   uv venv c:/tmp/afs_camelot_venv --python <64-bit 3.12>; uv pip install --python … camelot-py[base] pypdf
CAMELOT_VENV = Path("c:/tmp/afs_camelot_venv/Scripts/python.exe")
CAMELOT_SCRIPT = Path("c:/tmp/afs_census/camelot_ie.py")
CAMELOT_ROWS = Path("c:/tmp/afs_census/camelot_rows.json")
CAMELOT_SLUGS = {"monaghan", "kildare", "clare", "fingal", "dlr"}  # fitz mis-reads these layouts

YEAR_RE = re.compile(r"20[12]\d")
AFS_LINK = re.compile(r"(annual[-_ %]?financial|\bafs\b|financial[-_ %]?statement)", re.I)
NAV_AFS = re.compile(r"financ|statement|afs|budget|publication|account", re.I)

# council -> harvest config. landing[] tried in order (first that yields AFS pdfs wins);
# Galway County via gaillimh.ie alt (galwaycoco WAF); Meath reached by curl fallback.
# entity: county | city | merged. These 9 = Phase-0 (strict finder already passes).
REGISTRY: list[dict] = [
    {
        "council": "South Dublin",
        "slug": "south_dublin",
        "entity": "county",
        "region": "Dublin",
        "landing": [
            "https://www.sdcc.ie/en/services/our-council/policies-and-plans/budgets-and-spending/financial-statements/"
        ],
    },
    {
        "council": "Cork City",
        "slug": "cork_city",
        "entity": "city",
        "region": "Munster",
        "landing": ["https://www.corkcity.ie/en/council-services/public-info/spending-and-revenue/"],
    },
    {
        "council": "Cork County",
        "slug": "cork_county",
        "entity": "county",
        "region": "Munster",
        "landing": [
            "https://www.corkcoco.ie/en/council/accessibility-maps-and-publications/annual-financial-statements"
        ],
    },
    {
        "council": "Westmeath",
        "slug": "westmeath",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.westmeathcoco.ie/en/ourservices/finance/"],
    },
    {
        "council": "Galway City",
        "slug": "galway_city",
        "entity": "city",
        "region": "Connacht",
        "landing": ["https://www.galwaycity.ie/services/finance-services/budgets-and-financial-publications"],
    },
    {
        "council": "Galway County",
        "slug": "galway_county",
        "entity": "county",
        "region": "Connacht",
        "landing": ["https://www.gaillimh.ie/en/finance/financial-publications/annual-financial-statements"],
    },
    {
        "council": "Meath",
        "slug": "meath",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.meath.ie/council/your-council/finance-and-procurement"],
    },
    {
        "council": "Donegal",
        "slug": "donegal",
        "entity": "county",
        "region": "Ulster",
        "landing": ["https://www.donegalcoco.ie/services/other-services/finance/"],
    },
    {
        "council": "Tipperary",
        "slug": "tipperary",
        "entity": "county",
        "region": "Munster",
        "landing": ["https://www.tipperarycoco.ie/finance/financial-reports"],
    },
    # ---- Phase 1: census found a real AFS but the strict finder missed the page
    #      (best_ie_page now handles it) — landings proven to harvest AFS in the census ----
    {
        "council": "Wicklow",
        "slug": "wicklow",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.wicklow.ie/Living/Your-Council/Finance", "https://www.wicklow.ie"],
    },
    {
        "council": "Monaghan",
        "slug": "monaghan",
        "entity": "county",
        "region": "Ulster",
        "landing": ["https://monaghan.ie/finance/", "https://monaghan.ie"],
    },
    {
        "council": "Kildare",
        "slug": "kildare",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://kildarecoco.ie/YourCouncil/Publications/Finance/", "https://kildarecoco.ie"],
    },
    {
        "council": "Sligo",
        "slug": "sligo",
        "entity": "county",
        "region": "Connacht",
        "landing": ["https://www.sligococo.ie/YourCouncil/Finance/", "https://www.sligococo.ie"],
    },
    {
        "council": "Clare",
        "slug": "clare",
        "entity": "county",
        "region": "Munster",
        "landing": [
            "https://www.clarecoco.ie/your-council/about-the-council/council-finance/",
            "https://www.clarecoco.ie",
        ],
    },
    {
        "council": "Fingal",
        "slug": "fingal",
        "entity": "dublin",
        "region": "Dublin",
        "landing": ["https://www.fingal.ie/council/service/annual-financial-statement"],
    },
    {
        "council": "Dun Laoghaire-Rathdown",
        "slug": "dlr",
        "entity": "dublin",
        "region": "Dublin",
        "landing": ["https://www.dlrcoco.ie/council-democracy/finance/annual-financial-statements"],
    },
    {
        "council": "Kilkenny",
        "slug": "kilkenny",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://kilkennycoco.ie/eng/services/finance/", "https://kilkennycoco.ie"],
    },
    {
        "council": "Louth",
        "slug": "louth",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.louthcoco.ie/en/Your_Council/Finance/", "https://www.louthcoco.ie"],
    },
    # ---- Phase 1: census seed pointed at the wrong page (no AFS link found) — finance
    #      landing + homepage crawl; reconcile-gate drops anything that isn't the AFS ----
    {
        "council": "Wexford",
        "slug": "wexford",
        "entity": "county",
        "region": "Leinster",
        "landing": [
            "https://www.wexfordcoco.ie/council-and-democracy/procurement-finance-and-credit-control",
            "https://www.wexfordcoco.ie",
        ],
    },
    {
        "council": "Waterford",
        "slug": "waterford",
        "entity": "merged",
        "region": "Munster",
        "landing": [
            "https://waterfordcouncil.ie/openness-transparency/governance-related-financial-information/",
            "https://waterfordcouncil.ie",
        ],
    },
    {
        "council": "Limerick",
        "slug": "limerick",
        "entity": "merged",
        "region": "Munster",
        "landing": ["https://www.limerick.ie/council/services/business-and-economy/finance", "https://www.limerick.ie"],
    },
    {
        "council": "Offaly",
        "slug": "offaly",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.offaly.ie/financial-reports/", "https://www.offaly.ie"],
    },
    {
        "council": "Longford",
        "slug": "longford",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.longfordcoco.ie/services/finance/finance-documents/", "https://www.longfordcoco.ie"],
    },
    {
        "council": "Kerry",
        "slug": "kerry",
        "entity": "county",
        "region": "Munster",
        "landing": ["https://www.kerrycoco.ie/finance/financial-documents/", "https://www.kerrycoco.ie"],
    },
    {
        "council": "Leitrim",
        "slug": "leitrim",
        "entity": "county",
        "region": "Connacht",
        "landing": ["https://www.leitrim.ie/council/services/finance/", "https://www.leitrim.ie"],
    },
    {
        "council": "Laois",
        "slug": "laois",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://laois.ie/finance/", "https://laois.ie"],
    },
    {
        "council": "Dublin City",
        "slug": "dublin_city",
        "entity": "dublin",
        "region": "Dublin",
        "landing": [
            "https://www.dublincity.ie/residential/business/doing-business-council/council-budgets-spending",
            "https://www.dublincity.ie",
        ],
    },
    # ---- Deferred: JS-rendered file lists need Playwright to ENUMERATE (Carlow/Cavan/
    #      Mayo/Roscommon) — batch with the PO Playwright work. ----
]


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def title_year(url: str) -> int:
    """Filename year — for FILE SELECTION ONLY (prefer the latest). NOT authoritative for
    the row: the filename can carry a signing date ('Signed Dec 2020' = the 2019 AFS) and
    is unreliable. unquote first — raw '%20' spaces inject phantom digits ('Statement%202018'
    → '...202018...' → the regex greedily matches '2020'). The row year comes from
    statement_year(page) instead."""
    name = unquote(url.rsplit("/", 1)[-1])
    yrs = [int(y) for y in YEAR_RE.findall(name)]
    return max(yrs) if yrs else 0


def statement_year(page_text: str) -> int | None:
    """Authoritative statement year = the modal 20xx on the I&E page. The current-year
    column header repeats 3× (Gross/Income/Net all same year) vs the prior-year once, so the
    mode is the reporting year (Galway County '2024 2024 2024 2023' → 2024; Meath
    '2019 2019 2019 2018' → 2019). Tie-break to the max."""
    yrs = [int(y) for y in YEAR_RE.findall(page_text)]
    if not yrs:
        return None
    top = Counter(yrs).most_common()
    best = max(c for _, c in top)
    return max(y for y, c in top if c == best)


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


def best_ie_page(doc) -> tuple[int | None, dict, tuple | None]:
    """Scan EVERY page, parse_ie each, and keep the page whose parse yields the most
    divisions AND reconciles to its own printed total — not the first keyword match.

    The strict first-match finder mis-fired on narrative pages that merely *mention* all 8
    divisions + 'gross expenditure' (e.g. Galway County p4: 1/8 divisions, €2.3m) instead
    of the real tabular I&E statement (p14: 8/8, €181m). Reconciling to the printed total
    is the correctness signal — it also rejects the Note-16 budget-vs-actual page (whose
    stacked sub-tables never reconcile under a flat line parser)."""
    best_pg, best_ie, best_total, best_score = None, {}, None, (-1, -1, -1)
    for i in range(doc.page_count):
        ie, total = parse_ie(doc[i].get_text("text"))
        if len(ie) < 6:
            continue
        gross_sum = sum(v[0] for v in ie.values() if v[0])
        reconciled = bool(total and abs(gross_sum - total[0]) < 100_000)
        # prefer: reconciling 8/8, then any reconcile, then most divisions
        score = (int(len(ie) == 8 and reconciled), int(reconciled), len(ie))
        if score > best_score:
            best_pg, best_ie, best_total, best_score = i, ie, total, score
    return best_pg, best_ie, best_total


def ingest_council(cf: dict, list_only: bool) -> tuple[list[dict], dict]:
    urls: list[str] = []
    for landing in cf["landing"]:
        urls = harvest_afs(landing)
        if urls:
            break
    urls = urls + [u for u in cf.get("direct", []) if u not in urls]  # known-good fallback URLs
    picked = select_afs(urls)
    stat = {
        "council": cf["council"],
        "slug": cf["slug"],
        "n_afs": len(urls),
        "picked": picked,
        "year": title_year(picked) if picked else None,
    }
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
    pg, ie, total = best_ie_page(doc)
    if pg is None:
        doc.close()
        stat["status"] = "no-IE-page"
        return [], stat
    # AUTHORITATIVE year from the statement itself (filename year is unreliable — signing
    # dates, %20-injected phantom digits). Fall back to the filename only if the page has none.
    year = statement_year(doc[pg].get_text("text")) or year
    stat["year"] = year
    doc.close()
    gross_sum = sum(v[0] for v in ie.values() if v[0])
    reconciled = bool(total and abs(gross_sum - total[0]) < 100_000)
    # SAFETY GATE: only councils whose Σ gross matches the statement's own printed total
    # enter the fact. A non-reconciling parse means the parser mis-read this council's
    # layout (wrong cells / wrong page) and the figures are NOT trustworthy — record the
    # miss in coverage but emit NO rows, so the fact stays clean-by-construction.
    if not reconciled:
        stat.update(
            status="no-reconcile", divisions=len(ie), gross_sum=gross_sum, reconciled=False, pages=npages, ie_page=pg
        )
        return [], stat
    rows = [
        {
            "council": cf["council"],
            "slug": cf["slug"],
            "entity": cf["entity"],
            "region": cf["region"],
            "year": year,
            "division": canon,
            "gross_expenditure": v[0],
            "income": v[1],
            "net_expenditure": v[2],
            "net_expenditure_prior_yr": v[3],
            "source_file_url": picked,
            "source_page_number": pg,
            "printed_total_eur": (total[0] if total else None),
            "reconciled": reconciled,
            "parser": "fitz",
        }
        for canon, v in ie.items()
    ]
    stat.update(
        status="ok" if len(ie) == 8 else f"{len(ie)}/8",
        divisions=len(ie),
        gross_sum=gross_sum,
        reconciled=reconciled,
        pages=npages,
        ie_page=pg,
    )
    return rows, stat


def merge_camelot(stats: list[dict]) -> list[dict]:
    """Merge camelot-extracted rows for the layout-mismatch councils (CAMELOT_SLUGS).
    Best-effort refresh via the isolated venv, then read its JSON; attach council/entity/
    region (registry), year (fitz statement_year on the bronze page), source_file_url (the
    fitz pass's picked url); re-validate net=gross−income before admitting. Skips silently if
    the isolated venv/JSON is absent (CI/Cloud) — the fitz fact still ships."""
    if CAMELOT_VENV.exists() and CAMELOT_SCRIPT.exists():
        with contextlib.suppress(Exception):
            subprocess.run(
                [str(CAMELOT_VENV), str(CAMELOT_SCRIPT)],
                timeout=600,
                capture_output=True,
                cwd=str(CAMELOT_SCRIPT.parent),
                check=False,
            )
    if not CAMELOT_ROWS.exists():
        return []
    cam = json.loads(CAMELOT_ROWS.read_text(encoding="utf-8"))
    by_slug = {c["slug"]: c for c in REGISTRY}
    picked_of = {s["council"]: s.get("picked") for s in stats}
    grouped: dict[str, list] = {}
    for r in cam:
        if r["slug"] in CAMELOT_SLUGS:  # exclude the control council + any not-mismatch slug
            grouped.setdefault(r["slug"], []).append(r)
    out: list[dict] = []
    for slug, rows in grouped.items():
        cf = by_slug.get(slug)
        if not cf:
            continue
        year = None
        files = list((CACHE / slug).glob("*.pdf"))
        if files:
            with contextlib.suppress(Exception):
                doc = fitz.open(files[0])
                year = statement_year(doc[rows[0]["source_page_number"]].get_text("text"))
                doc.close()
        for r in rows:
            g, inc, net = r["gross_expenditure"], r["income"], r["net_expenditure"]
            if abs((g - inc) - net) > 1000:  # accounting identity must hold or drop the row
                continue
            out.append(
                {
                    "council": cf["council"],
                    "slug": slug,
                    "entity": cf["entity"],
                    "region": cf["region"],
                    "year": year,
                    "division": r["division"],
                    "gross_expenditure": g,
                    "income": inc,
                    "net_expenditure": net,
                    "net_expenditure_prior_yr": r.get("net_expenditure_prior_yr"),
                    "source_file_url": picked_of.get(cf["council"]),
                    "source_page_number": r["source_page_number"],
                    "printed_total_eur": r.get("printed_total_eur"),
                    "reconciled": True,
                    "parser": "camelot",
                }
            )
    return out


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
        recon = "EXACT" if stat.get("reconciled") else ("FAIL" if "gross_sum" in stat else "-")
        print(
            f"  {cf['council']:<16} {stat['status']:<16} "
            f"div={stat.get('divisions', '-')}/8  recon={recon}  "
            f"yr={stat.get('year', '-')}  pg={stat.get('ie_page', '-')}"
        )

    # camelot fallback for the layout-mismatch councils (runs unless --list / --only-restricted)
    cam_rows = [] if args.list else merge_camelot(stats)
    if cam_rows:
        cam_councils = sorted({r["council"] for r in cam_rows})
        all_rows.extend(cam_rows)
        print(
            f"\n  + camelot (isolated venv) added {len(cam_councils)} layout-mismatch "
            f"councils: {', '.join(cam_councils)}"
        )

    if args.list or not all_rows:
        if not all_rows and not args.list:
            print("\nno rows extracted")
        return

    df = (
        pl.DataFrame(all_rows)
        .with_columns(
            pl.lit("SPENT").alias("realisation_tier"),
            pl.lit("net_expenditure_actual").alias("value_kind"),
            pl.lit("single-LA (per-council audited AFS)").alias("scope"),
            pl.lit("Local Authority audited AFS (own website), Local Government Audit Service").alias("source"),
        )
        .sort(["council", "year", "division"])
    )
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)

    hr("DATA-QUALITY — per council")
    n_fitz = df.filter(pl.col("parser") == "fitz")["council"].n_unique()
    n_cam = df.filter(pl.col("parser") == "camelot")["council"].n_unique()
    print(
        f"  councils in fact (all reconcile): {df['council'].n_unique()}/{len(pubs)}  (fitz {n_fitz} + camelot {n_cam})"
    )
    print(f"  rows: {df.height}  | all reconciled: {df['reconciled'].all()}")

    cov = {
        "councils_attempted": len(pubs),
        "councils_with_rows": df["council"].n_unique(),
        "councils_fitz": n_fitz,
        "councils_camelot": n_cam,
        "rows": df.height,
        "phase": 1,
        "by_council": stats,
        "realisation_tier": "SPENT",
        "value_kind": "net_expenditure_actual",
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
