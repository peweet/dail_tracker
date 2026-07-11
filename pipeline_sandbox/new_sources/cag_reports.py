"""P0-1 — C&AG audit reports index (SANDBOX). Hardened 2026-07-11.

Walks the three Comptroller & Auditor General publication categories on
audit.gov.ie, captures the report index (type, number, title, URL), then
enriches EVERY report with publication date + PDF links from its detail page.

Hardening over the 2026-06-28 run (STATUS_LEDGER defects):
1. Every fetched listing page and detail page is persisted to bronze via
   cache_raw(); each row carries source_document_hash = SHA-256 of the detail
   HTML it was parsed from (silent-reissue defence, plan §4).
2. MAX_DETAIL cap removed — all reports get a detail parse + PDF URL(s).
3. Publication date parse is scoped and graded. Inspection of real detail
   pages (special reports 32/99/119/120, appropriation accounts 1922-23/
   1973-74/2021/2024, reports-on-accounts 1996/2017/2024) found:
     - SOME pages carry an explicit first-paragraph label
       ``<p class="first-child"><em><strong>Published on 07 November 2017``
       inside article#content — parsed specifically -> date_confidence=high.
     - MOST pages (all appropriation accounts, many special reports) have NO
       publication-date element anywhere (no <time>, no date meta, no label).
       Fallback = first day-month-year date in the article body text only
       (nav/sidebar excluded), skipping "year/period ended ..." accounting
       period references -> date_confidence=low (narrative dates, e.g. SR119's
       Article-50 date, are preserved as-is but flagged, never trusted).
     - Site sitemap <lastmod> and PDF Last-Modified headers are 2026 CMS
       migration timestamps — recorded as source_last_modified, NOT dates.
   report_year (accounts period) is additionally parsed from the trailing
   year in appropriation-accounts / report-on-accounts titles (factual).

PDF corpus: report PDFs for reports published >= PDF_YEAR_FLOOR (2020) are
downloaded to bronze/cag_reports/pdf/ with SHA-256 (manifest written to
silver/cag_report_pdfs.parquet). Eligibility year = parsed publication year,
else title report_year; low-confidence dates can misclassify a few special
reports (flagged in the run report, not silently fixed). PDF CONTENTS ARE NOT
PARSED here — index level only.

Licence (graduation gate): audit.gov.ie states all Office open data is
published under CC BY 4.0 and complies with the Open Data Directive (EU)
2019/1024 — https://www.audit.gov.ie/en/open-data-and-the-reuse-of-public-sector-information/

Open public record. Money figures are NEVER summed; any cost captured is
value_safe_to_sum=False.
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    BRONZE,
    cache_raw,
    fetch,
    now_iso,
    sha256_bytes,
    write_silver,
)

SOURCE = "cag_reports"
BASE = "https://www.audit.gov.ie"
CATEGORIES = {
    "special_report": "/en/publications/special-reports/",
    "report_on_accounts": "/en/publications/report-on-the-accounts-of-the-public-services/",
    "appropriation_accounts": "/en/publications/appropriation-accounts/",
}
MAX_PAGES = 25
PDF_YEAR_FLOOR = 2020  # bounded PDF corpus for the future appropriation-accounts parse
PDF_SUSPECT_BYTES = 10_240  # < 10 KB smells like an error page, flag it

BRONZE_DIR = BRONZE / SOURCE
PDF_DIR = BRONZE_DIR / "pdf"

MONTHS = ("January|February|March|April|May|June|July|August|September|"
          "October|November|December")
DATE_RE = re.compile(rf"\b\d{{1,2}}\s+(?:{MONTHS})\s+(?:19|20)\d\d\b")
PUBLISHED_RE = re.compile(
    rf"Published\s+on\s+(\d{{1,2}}\s+(?:{MONTHS})\s+(?:19|20)\d\d)", re.I)
PERIOD_LABEL_RE = re.compile(r"(?:year|period)\s+end(?:ed|ing)\s*$", re.I)
TRAILING_YEAR_RE = re.compile(r"\b((?:19|20)\d\d)(?:[-/]\d{2})?\s*$")


# ---------------------------------------------------------------- fetch layer

def cached_fetch(url: str, bronze_name: str, refresh: bool = False):
    """Fetch a page, persisting the raw bytes to bronze. Reuses the bronze
    cache on re-runs (resume support) unless refresh=True.

    Returns (html_text, meta) where meta always carries source_document_hash,
    fetched_at, bronze_path, from_cache and source_last_modified (if any).
    """
    p = BRONZE_DIR / bronze_name
    if p.exists() and not refresh:
        raw = p.read_bytes()
        return raw.decode("utf-8", errors="replace"), {
            "source_document_hash": sha256_bytes(raw),
            "fetched_at": datetime.fromtimestamp(
                p.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_last_modified": None,
            "bronze_path": str(p),
            "from_cache": True,
        }
    payload, meta = fetch(url, binary=True)
    path, digest = cache_raw(SOURCE, bronze_name, payload)
    meta["source_document_hash"] = digest  # hash of exactly the cached bytes
    meta["bronze_path"] = str(path)
    meta["from_cache"] = False
    return payload.decode("utf-8", errors="replace"), meta


# ------------------------------------------------------------------- listing

def collect_index(refresh: bool = False) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    for rtype, path in CATEGORIES.items():
        cat_slug = path.strip("/").split("/")[-1]
        for page in range(1, MAX_PAGES + 1):
            url = BASE + path + (f"?pageNumber={page}" if page > 1 else "")
            try:
                html, _m = cached_fetch(
                    url, f"listing_{cat_slug}_p{page}.html", refresh=refresh)
            except Exception as e:  # noqa: BLE001
                print(f"  {rtype} p{page}: {type(e).__name__} {e}")
                break
            s = BeautifulSoup(html, "html.parser")
            new = 0
            for a in s.find_all("a", href=True):
                h = a["href"]
                if f"/publications/{cat_slug}/" in h and h.rstrip("/").split("/")[-1] != cat_slug:
                    full = BASE + h if h.startswith("/") else h
                    if full in seen:
                        continue
                    seen.add(full)
                    title = a.get_text(" ", strip=True)
                    if not title or len(title) < 5:
                        continue
                    num = re.search(r"(?:Special\s+)?Report\s+(\d+)", title)
                    rows.append({
                        "report_type": rtype,
                        "report_number": int(num.group(1)) if num else None,
                        "title": title,
                        "source_url": full,
                        "list_page": page,
                    })
                    new += 1
            if new == 0:
                break
            print(f"  {rtype} p{page}: +{new} (running {len(rows)})")
    return rows


# -------------------------------------------------------------- detail parse

def parse_published_date(article_text: str) -> tuple[str | None, str | None, str | None]:
    """Return (raw_date, iso_date, confidence) from article body text only.

    high = explicit 'Published on <date>' label (verified element on real pages)
    low  = first narrative date in the article body (preserved, not trusted);
           dates directly following 'year/period ended' are accounting-period
           references, not publication dates, and are skipped.
    """
    m = PUBLISHED_RE.search(article_text)
    if m:
        return m.group(1), _to_iso(m.group(1)), "high"
    for m in DATE_RE.finditer(article_text):
        ctx = article_text[max(0, m.start() - 30):m.start()]
        if PERIOD_LABEL_RE.search(ctx.strip()):
            continue
        return m.group(0), _to_iso(m.group(0)), "low"
    return None, None, None


def _to_iso(raw: str) -> str | None:
    try:
        return datetime.strptime(raw, "%d %B %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_detail(html: str) -> dict:
    s = BeautifulSoup(html, "html.parser")
    art = s.find("article") or s.find("main") or s
    text = art.get_text(" ", strip=True)
    raw, iso, conf = parse_published_date(text)
    pdfs = []
    for a in art.find_all("a", href=True):
        h = a["href"]
        if h.split("?")[0].lower().endswith(".pdf"):
            full = BASE + h if h.startswith("/") else h
            if full not in pdfs:
                pdfs.append(full)
    return {
        "source_published_date_raw": raw,
        "source_published_date": iso,
        "date_confidence": conf,
        "pdf_url": pdfs[0] if pdfs else None,
        "pdf_count": len(pdfs),
        "pdf_urls": ";".join(pdfs) if pdfs else None,
    }


def enrich_detail(rows: list[dict], refresh: bool = False) -> None:
    n = len(rows)
    for i, r in enumerate(rows, 1):
        slug = r["source_url"].rstrip("/").split("/")[-1]
        try:
            html, meta = cached_fetch(
                r["source_url"], f"detail_{r['report_type']}_{slug}.html",
                refresh=refresh)
        except Exception as e:  # noqa: BLE001
            r["detail_error"] = type(e).__name__
            print(f"  [{i}/{n}] DETAIL ERROR {type(e).__name__}: {r['source_url']}")
            continue
        r["detail_error"] = None
        r.update(parse_detail(html))
        r["source_document_hash"] = meta["source_document_hash"]
        r["bronze_path"] = meta["bronze_path"]
        r["fetched_at"] = meta["fetched_at"]
        r["source_last_modified"] = meta.get("source_last_modified")
        if i % 25 == 0 or i == n:
            print(f"  [{i}/{n}] details parsed (last: {slug[:60]})")


def derive_report_year(r: dict) -> int | None:
    """Accounts-period year from the structural trailing year in
    appropriation-accounts / report-on-accounts titles (factual, from title).
    Special-report titles have no structural year — left null."""
    if r["report_type"] not in ("appropriation_accounts", "report_on_accounts"):
        return None
    m = TRAILING_YEAR_RE.search(r["title"].strip())
    return int(m.group(1)) if m else None


# ------------------------------------------------------------------ PDF pull

def pdf_eligible_year(r: dict) -> int | None:
    """Publication-year proxy for the PDF corpus bound: parsed publication
    year when present, else the title accounts year. Low-confidence dates can
    misclassify (reported, not fixed)."""
    d = r.get("source_published_date")
    if d:
        return int(d[:4])
    return r.get("report_year")


def download_pdfs(rows: list[dict]) -> list[dict]:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    targets: list[tuple[dict, str]] = []
    for r in rows:
        y = pdf_eligible_year(r)
        if y is None or y < PDF_YEAR_FLOOR or not r.get("pdf_urls"):
            continue
        for u in r["pdf_urls"].split(";"):
            targets.append((r, u))
    print(f"\nPDF corpus (published >= {PDF_YEAR_FLOOR}): "
          f"{len(targets)} files across "
          f"{len({id(r) for r, _ in targets})} reports")
    manifest: list[dict] = []
    total_bytes = 0
    fresh = 0
    for i, (r, url) in enumerate(targets, 1):
        parts = url.split("?")[0].rstrip("/").split("/")
        name = f"{parts[-2]}_{parts[-1]}" if len(parts) >= 2 else parts[-1]
        p = PDF_DIR / name
        if p.exists():  # cached by unique media-token name; hash from disk
            raw = p.read_bytes()
            fetched = datetime.fromtimestamp(
                p.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            cached = True
        else:
            try:
                raw, meta = fetch(url, binary=True)
            except Exception as e:  # noqa: BLE001
                print(f"  [{i}/{len(targets)}] PDF ERROR {type(e).__name__}: {url}")
                manifest.append({
                    "report_source_url": r["source_url"], "pdf_url": url,
                    "file_name": name, "sha256": None, "size_bytes": None,
                    "fetched_at": now_iso(), "download_error": type(e).__name__,
                })
                continue
            p.write_bytes(raw)
            fetched = meta["fetched_at"]
            cached = False
            fresh += 1
        mb = len(raw) / 1_048_576
        suspect = len(raw) < PDF_SUSPECT_BYTES
        total_bytes += len(raw)
        print(f"  [{i}/{len(targets)}] {'cache' if cached else 'GET  '} "
              f"{mb:7.2f} MB  {name[:70]}{'  !! <10KB SUSPECT' if suspect else ''}")
        manifest.append({
            "report_source_url": r["source_url"], "pdf_url": url,
            "file_name": name, "sha256": sha256_bytes(raw),
            "size_bytes": len(raw), "fetched_at": fetched,
            "download_error": None,
        })
    print(f"PDF cache: {len(manifest)} files, {total_bytes / 1_048_576:.1f} MB "
          f"({fresh} newly downloaded this run)")
    return manifest


# ----------------------------------------------------------------------- run

def run(skip_pdfs: bool = False, refresh: bool = False) -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    rows = collect_index(refresh=refresh)
    if not rows:
        print("No reports collected (site structure may have changed).")
        return
    print(f"\nEnriching detail for ALL {len(rows)} reports…")
    enrich_detail(rows, refresh=refresh)
    fetched_default = now_iso()
    for r in rows:
        r["report_year"] = derive_report_year(r)
        r.setdefault("fetched_at", fetched_default)
        r.setdefault("source_document_hash", None)
        r.setdefault("bronze_path", None)
        r.setdefault("source_last_modified", None)
        r.setdefault("source_published_date_raw", None)
        r.setdefault("source_published_date", None)
        r.setdefault("date_confidence", None)
        r.setdefault("pdf_url", None)
        r.setdefault("pdf_count", 0)
        r.setdefault("pdf_urls", None)
        r.update({
            "extraction_method": "html_scrape",
            "confidence": "high" if not r.get("detail_error") else "low",
            "privacy_tier": "public",
            "value_safe_to_sum": False,  # no money cols; keep the guard flag
        })

    if not skip_pdfs:
        manifest = download_pdfs(rows)
        if manifest:
            mdf = pl.DataFrame(manifest)
            mout = write_silver("cag_report_pdfs", mdf)
            print(f"PDF MANIFEST: {mout}  rows={mdf.height}")

    df = pl.DataFrame(rows)
    out = write_silver(SOURCE, df)

    # ---- compact profile ----------------------------------------------
    print(f"\nSILVER: {out}  rows={df.height}")
    for t in df.group_by("report_type").len().sort("len", descending=True).to_dicts():
        print(f"  {t['len']:>4}  {t['report_type']}")
    print("date_confidence:",
          {d["date_confidence"]: d["len"]
           for d in df.group_by("date_confidence").len().to_dicts()})
    print("pdf_url coverage: "
          f"{df.filter(pl.col('pdf_url').is_not_null()).height}/{df.height}")
    print("detail errors:", df.filter(pl.col("detail_error").is_not_null()).height)
    key_nulls = {c: df[c].null_count()
                 for c in ("title", "source_url", "source_document_hash",
                           "source_published_date", "report_year", "pdf_url")}
    print("null counts:", key_nulls)
    yr = (df.with_columns(
            pl.coalesce(pl.col("source_published_date").str.slice(0, 4).cast(pl.Int32, strict=False),
                        pl.col("report_year").cast(pl.Int32)).alias("best_year"))
            .filter(pl.col("best_year").is_not_null()))
    if yr.height:
        print(f"best-known year range: {yr['best_year'].min()}–{yr['best_year'].max()} "
              f"(known for {yr.height}/{df.height} rows)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--skip-pdfs", action="store_true",
                    help="index + detail only; skip the 2020+ PDF corpus pull")
    ap.add_argument("--refresh", action="store_true",
                    help="refetch pages even if cached in bronze")
    args = ap.parse_args()
    run(skip_pdfs=args.skip_pdfs, refresh=args.refresh)
