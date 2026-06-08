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
  ./.venv/Scripts/python.exe extractors/la_afs_extract.py            # full Phase-0 ingest
  ./.venv/Scripts/python.exe extractors/la_afs_extract.py --only meath,donegal
  ./.venv/Scripts/python.exe extractors/la_afs_extract.py --list     # harvest/select only
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
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
sys.path.insert(0, str(ROOT / "extractors"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

# reuse the amalgamated extractor wholesale — same statement, same parser
from afs_amalgamated_extract import parse_ie  # noqa: E402
from procurement_la_seed import HREF_RE, fetch_bytes, fetch_text  # noqa: E402

import config  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

CACHE = config.BRONZE_PDF_DIR / "la_afs"
OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_afs_divisions.parquet"
OUT_COV = ROOT / "data/_meta/la_afs_coverage.json"

# Camelot fallback for councils whose I&E LAYOUT the fitz parse_ie mis-reads (it grabs the
# wrong cells → ~2× inflated Σgross that the reconcile gate rejects). Camelot's structured
# cell grid reads them correctly. The script lives in the REPO (la_afs_camelot_ie.py) but
# RUNS in an ISOLATED venv (immune to the main-venv `uv sync` churn + avoids the opencv/cv2
# clash that would break SIPO) — see feedback_dual_parser_rule. Best-effort: if the venv is
# absent (CI/Cloud/fresh machine), these councils are skipped and the fitz fact still ships.
# Rebuild the venv (then re-run): the recipe is in la_afs_camelot_ie.py's docstring. The venv
# path is overridable via $AFS_CAMELOT_VENV so a fresh clone can point at its own.
CAMELOT_VENV = Path(os.environ.get("AFS_CAMELOT_VENV", "c:/tmp/afs_camelot_venv/Scripts/python.exe"))
CAMELOT_SCRIPT = ROOT / "extractors" / "la_afs_camelot_ie.py"
CAMELOT_ROWS = ROOT / "data" / "_meta" / "la_afs_camelot_rows.json"

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
            "https://www.corkcoco.ie/en/council/accessibility-maps-and-publications/annual-financial-statement"
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
        "landing": [
            "https://kilkennycoco.ie/eng/services/finance/annual-financial-statements/",
            "https://kilkennycoco.ie/eng/publications/annual-financial-statements/",
        ],
        "direct": [
            "https://kilkennycoco.ie/eng/publications/annual-financial-statements/final-afs-2023-for-website11.pdf"
        ],
    },
    {
        "council": "Louth",
        "slug": "louth",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.louthcoco.ie/en/publications/finance_reports/afs/"],
    },
    # ---- Phase 1: AFS landing URLs found via web search (census seed had pointed at the
    #      wrong page); `direct` = known-good audited PDF as a fallback if harvest misses ----
    {
        "council": "Wexford",
        "slug": "wexford",
        "entity": "county",
        "region": "Leinster",
        "landing": [
            "https://www.wexfordcoco.ie/council-and-democracy/council-minutes-plans-publications-and-reports/annual-financial-statements"
        ],
    },
    {
        "council": "Waterford",
        "slug": "waterford",
        "entity": "merged",
        "region": "Munster",
        "landing": ["https://waterfordcouncil.ie/documents/annual-reports/", "https://waterfordcouncil.ie/documents/"],
    },
    {
        "council": "Limerick",
        "slug": "limerick",
        "entity": "merged",
        "region": "Munster",
        "landing": [
            "https://www.limerick.ie/council/services/your-council/budgets-expenditure-and-financial-statements/annual-financial"
        ],
        "direct": [
            "https://www.limerick.ie/sites/default/files/media/documents/2026-02/limerick-city-and-county-council-audited-annual-financial-statement-2024.pdf"
        ],
    },
    {
        "council": "Offaly",
        "slug": "offaly",
        "entity": "county",
        "region": "Leinster",
        "landing": [
            "https://www.offaly.ie/annual-financial-statement-publication-2/",
            "https://www.offaly.ie/financial-reports/",
        ],
        "direct": ["https://www.offaly.ie/app/uploads/Council/Council_Services_A-Z/Finance/Audited-AFS-2022.pdf"],
    },
    {
        "council": "Longford",
        "slug": "longford",
        "entity": "county",
        "region": "Leinster",
        "landing": ["https://www.longfordcoco.ie/services/finance/annual-financial-statements/"],
        "direct": [
            "https://www.longfordcoco.ie/services/finance/annual-financial-statements/annual-financial-statement-2024.pdf"
        ],
    },
    {
        "council": "Kerry",
        "slug": "kerry",
        "entity": "county",
        "region": "Munster",
        "landing": [
            "https://www.kerrycoco.ie/finance/financial-documents/",
            "https://www.kerrycoco.ie/publications/",
            "http://reports.kerrycoco.ie",
        ],
    },
    {
        "council": "Leitrim",
        "slug": "leitrim",
        "entity": "county",
        "region": "Connacht",
        "landing": [
            "https://www.leitrim.ie/council/services/finance/finance-publications/annual-financial-statements/"
        ],
        "direct": [
            "https://www.leitrim.ie/council/services/finance/finance-publications/annual-financial-statements/2023-audited-afs.pdf"
        ],
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
        "landing": ["https://www.dublincity.ie/council/budgets-and-finance/financial-accounting-services-council"],
        "direct": ["https://www.dublincity.ie/sites/default/files/2025-04/dcc-afs-accounts-for-publication-2024.pdf"],
    },
    # ---- Deferred: JS-rendered file lists need Playwright to ENUMERATE (Carlow/Cavan/
    #      Mayo/Roscommon) — batch with the PO Playwright work. ----
]


# Councils NOT in the harvest loop above — their AFS file list is JS-rendered and needs
# Playwright to enumerate (deferred). Listed so coverage accounts for all 31 LAs.
DEFERRED_COUNCILS: list[dict] = [
    {"council": "Carlow", "slug": "carlow", "entity": "county", "region": "Leinster", "landing": ["https://carlow.ie"]},
    {
        "council": "Cavan",
        "slug": "cavan",
        "entity": "county",
        "region": "Ulster",
        "landing": ["https://www.cavancoco.ie"],
    },
    {"council": "Mayo", "slug": "mayo", "entity": "county", "region": "Connacht", "landing": ["https://www.mayo.ie"]},
    {
        "council": "Roscommon",
        "slug": "roscommon",
        "entity": "county",
        "region": "Connacht",
        "landing": ["https://www.roscommoncoco.ie"],
    },
]

# Plain-English reason a council's AFS is NOT yet in the fact, for surfacing to end users
# (factual availability only — no inference). Anything not listed gets a generic message.
_SCANNED = (
    "scanned_image",
    "This council publishes its statement only as a scanned image, which is not yet machine-readable.",
)
_VIEWER = (
    "interactive_viewer",
    "This council publishes its statement through an interactive online viewer we cannot yet read automatically.",
)
_LAYOUT = ("unusual_layout", "This council's statement uses a layout we cannot yet read reliably.")
_NOTFOUND = ("not_located", "We could not locate this council's published audited statement online.")
UNAVAILABLE_REASON: dict[str, tuple[str, str]] = {
    "wexford": _SCANNED,
    "waterford": _SCANNED,
    "laois": _SCANNED,
    "wicklow": _LAYOUT,
    "louth": _LAYOUT,
    "kerry": _NOTFOUND,
    "carlow": _VIEWER,
    "cavan": _VIEWER,
    "mayo": _VIEWER,
    "roscommon": _VIEWER,
}


def coverage_by_council(df: pl.DataFrame) -> list[dict]:
    """One entry per ALL 31 LAs: available (with year + parser) or flagged with a plain-English
    reason — the structure an end-user UI reads to show "21 of 31 available; the rest because…"."""
    meta = {
        r["slug"]: r
        for r in df.group_by("slug")
        .agg(pl.col("council").first(), pl.col("region").first(), pl.col("year").first(), pl.col("parser").first())
        .iter_rows(named=True)
    }
    out = []
    for cf in REGISTRY + DEFERRED_COUNCILS:
        slug = cf["slug"]
        if slug in meta:
            out.append(
                {
                    "council": cf["council"],
                    "slug": slug,
                    "region": cf["region"],
                    "available": True,
                    "year": meta[slug]["year"],
                    "parser": meta[slug]["parser"],
                }
            )
        else:
            cat, msg = UNAVAILABLE_REASON.get(slug, ("unavailable", "Not yet available."))
            out.append(
                {
                    "council": cf["council"],
                    "slug": slug,
                    "region": cf["region"],
                    "available": False,
                    "reason_category": cat,
                    "reason": msg,
                }
            )
    return sorted(out, key=lambda r: (not r["available"], r["council"]))


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
            # require an AFS-specific token (afs / annual financial / financial statement) —
            # a bare 'statement'+year matches non-AFS docs (child-safeguarding, AA-conclusion,
            # development-plan statements) that the file-selector would then wrongly pick.
            # unquote first: 'Annual%20Financial%20Statement.pdf' must decode to spaces or the
            # %20 (3 chars) breaks AFS_LINK's single-separator class.
            if low.endswith(".pdf") and AFS_LINK.search(unquote(href)):
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


def _browser_curl(url: str) -> bytes | None:
    """curl with a real-browser UA — some council WAFs (Sligo) block the research UA used by
    fetch_bytes/_curl but serve a normal browser fine."""
    with contextlib.suppress(Exception):
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        url = url.replace(" ", "%20")  # curl won't auto-encode a literal space (Sligo) → 404
        p = subprocess.run(
            ["curl", "-sS", "-k", "-L", "--max-time", "60", "-A", ua, url], capture_output=True, timeout=90, check=False
        )
        return p.stdout if p.returncode == 0 and p.stdout[:4] == b"%PDF" else None
    return None


def download(slug: str, url: str, year: int) -> Path | None:
    dest = CACHE / slug / f"{year or 'latest'}.pdf"
    if dest.exists() and dest.stat().st_size > 20000:
        return dest
    b = fetch_bytes(url)
    if not b or b[:4] != b"%PDF":
        b = _browser_curl(url)  # WAF fallback (e.g. Sligo)
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
        # a single malformed page must never abort a multi-year, multi-council run — skip it
        try:
            ie, total = parse_ie(doc[i].get_text("text"))
        except Exception:
            continue
        if len(ie) < 6:
            continue
        gross_sum = sum(v[0] for v in ie.values() if v[0])
        reconciled = bool(total and abs(gross_sum - total[0]) < 100_000)
        # prefer: reconciling 8/8, then any reconcile, then most divisions
        score = (int(len(ie) == 8 and reconciled), int(reconciled), len(ie))
        if score > best_score:
            best_pg, best_ie, best_total, best_score = i, ie, total, score
    return best_pg, best_ie, best_total


MIN_AFS_YEAR = 2016  # the modern 8-division I&E format; pre-2016 used programme-group names


def select_afs_years(urls: list[str]) -> list[str]:
    """One AFS per title-year (AUDITED preferred), years >= MIN_AFS_YEAR, newest first.

    Multi-year backfill (2026-06-08): councils file an AFS annually and keep the archive, but
    `select_afs` took only the LATEST — so the fact was a 1-year snapshot. This returns the whole
    post-2016 run so the per-LA fact becomes a by-division time series (the AFS twin of the PO
    deep-history lift). pre-2016 statements use the old programme-group layout the parser can't
    read, so they're excluded."""

    def is_audited(u: str) -> bool:
        return bool(re.search(r"audited", u, re.I) and not re.search(r"unaudited", u, re.I))

    by_year: dict[int, str] = {}
    for u in urls:
        y = title_year(u)
        if not y or y < MIN_AFS_YEAR:
            continue
        cur = by_year.get(y)
        if cur is None or (is_audited(u) and not is_audited(cur)):
            by_year[y] = u
    return [by_year[y] for y in sorted(by_year, reverse=True)]


def _parse_one_afs(cf: dict, picked: str) -> tuple[list[dict], dict]:
    """Download + reconcile ONE AFS PDF → (rows, stat). Factored out of ingest_council so the
    council loop can run it once per year. Same safety gate (emit rows only if Σ gross matches
    the statement's own printed total)."""
    year = title_year(picked)
    p = download(cf["slug"], picked, year)
    if not p:
        return [], {"council": cf["council"], "slug": cf["slug"], "picked": picked, "year": year, "status": "download-fail(WAF?)"}
    return _parse_pdf(cf, p, picked, year)


def _parse_pdf(cf: dict, p: Path, source_url: str | None, year_guess: int) -> tuple[list[dict], dict]:
    """Reconcile ONE already-on-disk AFS PDF → (rows, stat). Shared by the live path
    (_parse_one_afs, source_url = the harvested URL) and the bronze last-resort
    (source_url = None, p = a cached file). Emits rows only if Σ gross matches the
    statement's own printed total."""
    year = year_guess
    stat = {"council": cf["council"], "slug": cf["slug"], "picked": source_url, "year": year}
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
    # AUTHORITATIVE year from the statement itself (filename year is unreliable).
    year = statement_year(doc[pg].get_text("text")) or year
    stat["year"] = year
    doc.close()
    gross_sum = sum(v[0] for v in ie.values() if v[0])
    reconciled = bool(total and abs(gross_sum - total[0]) < 100_000)
    if not reconciled:
        stat.update(status="no-reconcile", divisions=len(ie), gross_sum=gross_sum, reconciled=False, pages=npages, ie_page=pg)
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
            "source_file_url": source_url,
            "source_page_number": pg,
            "printed_total_eur": (total[0] if total else None),
            "reconciled": reconciled,
            "parser": "fitz",
        }
        for canon, v in ie.items()
    ]
    stat.update(status="ok" if len(ie) == 8 else f"{len(ie)}/8", divisions=len(ie),
                gross_sum=gross_sum, reconciled=reconciled, pages=npages, ie_page=pg)
    return rows, stat


def bronze_picks(slug: str) -> list[Path]:
    """Cached AFS PDFs on disk for a council (named <year>.pdf by download()), years
    >= MIN_AFS_YEAR, newest first. The PROVEN recovery path: councils keep their archive
    and prior runs already cached many years — so a transient live-harvest failure should
    not drop a council whose statements we already hold."""
    d = CACHE / slug
    if not d.is_dir():
        return []
    cached = [(int(p.stem), p) for p in d.glob("*.pdf") if p.stem.isdigit() and int(p.stem) >= MIN_AFS_YEAR]
    return [p for _, p in sorted(cached, reverse=True)]


def ingest_council(cf: dict, list_only: bool) -> tuple[list[dict], dict]:
    # Retry the live harvest: fetch_text/fetch_bytes do ONE requests + ONE curl attempt, so a
    # single transient WAF/timeout returns no URLs and would silently drop the whole council
    # (and corrupt the single-run coverage). Retrying recovers it; the bronze last-resort below
    # is the final safety net if the site is down for the whole run.
    urls: list[str] = []
    for _ in range(3):
        for landing in cf["landing"]:
            urls = harvest_afs(landing)
            if urls:
                break
        if urls:
            break
    urls = urls + [u for u in cf.get("direct", []) if u not in urls]  # known-good fallback URLs
    picks = select_afs_years(urls)
    avail = sorted({title_year(u) for u in urls if title_year(u)}, reverse=True)
    stat = {
        "council": cf["council"],
        "slug": cf["slug"],
        "n_afs": len(urls),
        "picked": picks[0] if picks else None,
        "years_available": avail,
        "year": title_year(picks[0]) if picks else None,
    }
    if list_only:
        stat["status"] = "listed" if picks else "no-afs"
        return [], stat
    # Loop every post-2016 statement → the fact becomes a multi-year by-division series. Each
    # year passes its OWN reconcile gate independently (a layout-drifted old year is skipped, the
    # clean recent years still land).
    all_rows: list[dict] = []
    years_done: list[int] = []
    last_status = "no-reconcile"
    for picked in picks:
        rows, sub = _parse_one_afs(cf, picked)
        last_status = sub.get("status", last_status)
        if rows:
            all_rows.extend(rows)
            years_done.append(sub["year"])
    # BRONZE GAP-FILL: many council landings only surface the latest one/few AFS, but prior runs
    # already cached the whole archive (data/bronze/pdfs/la_afs/<slug>/<year>.pdf) — the proven
    # recovery path. Parse every cached year NOT already covered by a live pick (keyed by the
    # file's title-year = how download() names it, so live-covered files aren't re-parsed). Same
    # reconcile gate; if live harvest failed entirely this also recovers the council outright.
    picked_title_years = {title_year(u) for u in picks}
    bronze_added = 0
    for p in bronze_picks(cf["slug"]):
        if int(p.stem) in picked_title_years:
            continue
        rows, sub = _parse_pdf(cf, p, None, int(p.stem))
        last_status = sub.get("status", last_status)
        if rows:
            all_rows.extend(rows)
            years_done.append(sub["year"])
            bronze_added += 1
    # Drop any (year, division) a bronze file duplicated from a live pick (filename year can lag
    # the statement year — 'signed Dec 2020' = the 2019 AFS), preferring the live-URL row.
    if all_rows:
        seen: set[tuple[int, str]] = set()
        deduped = []
        for r in sorted(all_rows, key=lambda r: r["source_file_url"] is None):  # live (url) first
            k = (r["year"], r["division"])
            if k in seen:
                continue
            seen.add(k)
            deduped.append(r)
        all_rows = deduped
        years_done = sorted({r["year"] for r in all_rows})
    if not all_rows and not picks:
        stat["status"] = "no-afs"
        return [], stat
    stat["status"] = "ok" if all_rows else last_status  # 'ok' if ANY year landed → no camelot retry
    stat["bronze_added"] = bronze_added
    stat["years"] = sorted(set(years_done))
    stat["n_years"] = len(set(years_done))
    return all_rows, stat


def merge_camelot(stats: list[dict]) -> list[dict]:
    """Merge camelot-extracted rows for the layout-mismatch councils (the dynamic fitz fail-set).
    Best-effort refresh via the isolated venv, then read its JSON; attach council/entity/
    region (registry), year (fitz statement_year on the bronze page), source_file_url (the
    fitz pass's picked url); re-validate net=gross−income before admitting. Skips silently if
    the isolated venv/JSON is absent (CI/Cloud) — the fitz fact still ships."""
    # DYNAMIC fail-set: every council fitz downloaded a real AFS for but could NOT turn into
    # reconciling rows — either it mis-read the layout (no-reconcile) or couldn't even locate
    # the I&E page (no-IE-page, e.g. the table has no 'gross expenditure' text fitz keys on).
    # Camelot's cell grid handles both. Hand those slugs to camelot — not a hardcoded list.
    fail_slugs = sorted({s["slug"] for s in stats if s.get("status") in ("no-reconcile", "no-IE-page")})
    if not fail_slugs:
        return []
    if CAMELOT_VENV.exists() and CAMELOT_SCRIPT.exists():
        with contextlib.suppress(Exception):
            subprocess.run(
                [str(CAMELOT_VENV), str(CAMELOT_SCRIPT), *fail_slugs],
                timeout=900,
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
        if r["slug"] in fail_slugs:  # only councils fitz actually failed this run
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
    save_parquet(df, OUT_PARQUET)

    hr("DATA-QUALITY — per council")
    n_fitz = df.filter(pl.col("parser") == "fitz")["council"].n_unique()
    n_cam = df.filter(pl.col("parser") == "camelot")["council"].n_unique()
    print(
        f"  councils in fact (all reconcile): {df['council'].n_unique()}/{len(pubs)}  (fitz {n_fitz} + camelot {n_cam})"
    )
    print(f"  rows: {df.height}  | all reconciled: {df['reconciled'].all()}")

    manifest = coverage_by_council(df)
    n_total = len(REGISTRY) + len(DEFERRED_COUNCILS)
    unavailable = [m for m in manifest if not m["available"]]
    cov = {
        "councils_total": n_total,
        "councils_available": df["council"].n_unique(),
        "councils_unavailable": len(unavailable),
        "councils_fitz": n_fitz,
        "councils_camelot": n_cam,
        "rows": df.height,
        "phase": 1,
        "coverage_by_council": manifest,  # all 31 LAs, available or flagged with a plain-English reason
        "unavailable_by_reason": {
            cat: [m["council"] for m in unavailable if m.get("reason_category") == cat]
            for cat in {m.get("reason_category") for m in unavailable}
        },
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
    print(
        f"\n  coverage: {df['council'].n_unique()}/{n_total} councils available; "
        f"{len(unavailable)} flagged ({', '.join(sorted({m['reason_category'] for m in unavailable}))})"
    )
    print(f"wrote {OUT_PARQUET}\n      {OUT_COV}")


if __name__ == "__main__":
    main()
