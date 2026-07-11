"""HIQA inspection/monitoring reports for IPAS centres (SANDBOX).

HIQA began monitoring International Protection Accommodation Service (IPAS)
centres in 2024 under SI 649/2023. This ingests the report *index* only —
one row per published inspection report — and caches the report PDFs to
bronze. PDF contents are NOT parsed in this pass.

Source (statically rendered Drupal view, facet-filtered):
  https://www.hiqa.ie/reports-and-publications/inspection-reports
      ?term_node_tid_depth=203        <- "International Protection Accommodation"
      &page=N                         <- 0-indexed pager

Per-row fields the source actually publishes (listing tile):
  * title "{centre_id}, {centre_name}, {inspection_date}"
  * publication date (<time datetime=...>)
  * report PDF URL, centre detail-page URL
County comes from the centre detail page
(div.views-field-field-county). HIQA does NOT publish the provider/operator
name, address or capacity on the listing or centre page — operator identity
is only inside the PDF text (out of scope this pass), so ``provider_name``
is a deliberate all-null placeholder column.

PRIVACY: reports concern facilities and providers, never residents. Only
facility-level metadata is ingested. JOIN CAVEAT (do not act here): the
project's accommodation-providers view gates provider names behind
``public_display`` — any join of this table to provider spend must inherit
that gating at join time.

LICENCE: "All information contained in this website is the copyright of the
Health Information and Quality Authority. ... You may re-use the information
on this website free of charge in any format ... subject to the latest PSI
licence available at www.psi.gov.ie." (hiqa.ie/disclaimer)
"""
from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import polars as pl
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import BRONZE, cache_raw, fetch, now_iso, sha256_bytes, write_silver  # noqa: E402

SOURCE = "hiqa_ipas"
BASE = "https://www.hiqa.ie/reports-and-publications/inspection-reports"
IPAS_FACET = "203"  # term_node_tid_depth value for International Protection Accommodation
MAX_PAGES = 40      # hard cap; real pager was 9 pages at recon time (2026-07-11)
PDF_CAP = 300       # corpus should be small — stop and report if it balloons past this

# hiqa.ie throttles hard: ~25 quick requests earn a 429 storm. Extra delay on
# top of _common's 0.4s, plus Retry-After-aware backoff; both stages resume
# from bronze so repeated runs converge without re-fetching.
EXTRA_DELAY_S = 2.6
BACKOFFS_S = (30, 75, 180)


def polite_fetch(url: str, params: dict | None = None, binary: bool = False):
    """fetch() with slower pacing and retry on HTTP 429.

    hiqa.ie sends ``Retry-After: 0`` while still throttling, so the header is
    only trusted when it EXCEEDS our own backoff ladder.
    """
    for attempt, backoff in enumerate((*BACKOFFS_S, None)):
        time.sleep(EXTRA_DELAY_S)
        try:
            return fetch(url, params=params, binary=binary)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status != 429 or backoff is None:
                raise
            retry_after = (e.response.headers or {}).get("retry-after")
            hinted = int(retry_after) if retry_after and retry_after.isdigit() else 0
            wait = max(backoff, hinted)
            print(f"    429 on …{url[-55:]} — waiting {wait}s (retry {attempt + 1})")
            time.sleep(wait)
    raise RuntimeError("unreachable")

JOIN_CAVEAT = (
    "Provider names must inherit accommodation-providers public_display "
    "gating at join time"
)
TITLE_RE = re.compile(r"^\s*(\d+)\s*,\s*(.+)\s*,\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*$")


def _iso_from_long_date(raw: str) -> str | None:
    try:
        return datetime.strptime(raw.strip(), "%d %B %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _pdf_basename(pdf_url: str) -> str:
    """Local filename for a report PDF, derived from the ?file= query param."""
    q = parse_qs(urlparse(pdf_url).query)
    raw = (q.get("file", [None])[0] or urlparse(pdf_url).path).rsplit("/", 1)[-1]
    name = re.sub(r"[^A-Za-z0-9._-]", "_", raw)
    return name if name.lower().endswith(".pdf") else name + ".pdf"


def parse_listing_page(html: str, page: int, page_hash: str) -> list[dict]:
    s = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    for node in s.select("div.node--type-inspection-report"):
        h2 = node.select_one("h2")
        title = h2.get_text(" ", strip=True) if h2 else None
        centre_id = centre_name = inspection_raw = None
        if title:
            m = TITLE_RE.match(title)
            if m:
                centre_id, centre_name, inspection_raw = m.group(1), m.group(2).strip(), m.group(3)

        rtype = node.select_one(".report-icon__name")
        time_el = node.select_one(".field--name-field-ir-published-date time")
        pdf_a = node.select_one("a.icon-download[href]")
        centre_a = node.select_one("a.report-centre__link[href]")

        pdf_url = urljoin(BASE, pdf_a["href"]) if pdf_a else None
        pub_iso = (time_el.get("datetime") or "")[:10] if time_el else None
        out.append({
            "centre_id": centre_id,
            "centre_name": centre_name,
            "report_title": title,
            "report_type": rtype.get_text(" ", strip=True) if rtype else None,
            "inspection_date": _iso_from_long_date(inspection_raw) if inspection_raw else None,
            "inspection_date_raw": inspection_raw,
            "publication_date": pub_iso or None,
            "report_pdf_url": pdf_url,
            "centre_url": urljoin(BASE, centre_a["href"]) if centre_a else None,
            "listing_page": page,
            "listing_page_hash": page_hash,
        })
    return out


def parse_centre_county(html: str) -> str | None:
    s = BeautifulSoup(html, "html.parser")
    el = s.select_one("div.views-field-field-county .field-content")
    return el.get_text(" ", strip=True) or None if el else None


def enumerate_reports(max_pages: int = MAX_PAGES) -> list[dict]:
    """Walk the facet-filtered pager with a stop-on-no-new-rows guard."""
    rows: list[dict] = []
    seen: set[str] = set()
    for page in range(max_pages):
        cached = BRONZE / SOURCE / f"listing_p{page:02d}.html"
        try:
            html, meta = polite_fetch(BASE, params={"term_node_tid_depth": IPAS_FACET, "page": page})
            cache_raw(SOURCE, f"listing_p{page:02d}.html", html.encode("utf-8"))
            page_hash = meta["source_document_hash"]
        except requests.HTTPError as e:
            if not (cached.exists() and cached.stat().st_size):
                raise
            blob = cached.read_bytes()
            html, page_hash = blob.decode("utf-8"), sha256_bytes(blob)
            print(f"  page {page}: {e.response.status_code if e.response is not None else '?'} "
                  "— using bronze-cached listing from a prior run")
        batch = parse_listing_page(html, page, page_hash)
        if not batch:
            print(f"  page {page}: 0 tiles — stopping (end of list)")
            break
        fresh = 0
        for r in batch:
            key = r["report_pdf_url"] or r["report_title"] or ""
            if key and key not in seen:
                seen.add(key)
                rows.append(r)
                fresh += 1
        print(f"  page {page}: {len(batch)} tiles, +{fresh} new (running {len(rows)})")
        if fresh == 0:
            # guard against a no-op ?page= param that re-serves the same list
            print(f"  page {page}: no new rows — stopping (pager exhausted or no-op)")
            break
    return rows


def fetch_counties(rows: list[dict]) -> dict[str, str | None]:
    """One fetch per distinct centre page -> county. Failures degrade to None."""
    counties: dict[str, str | None] = {}
    urls = sorted({r["centre_url"] for r in rows if r["centre_url"]})
    print(f"\ncentre pages: {len(urls)} distinct")
    hits = 0
    for i, url in enumerate(urls, 1):
        slug = re.sub(r"[^A-Za-z0-9_-]", "_", urlparse(url).path.rsplit("/", 1)[-1])
        cached = BRONZE / SOURCE / f"centre_{slug}.html"
        if cached.exists() and cached.stat().st_size:
            counties[url] = parse_centre_county(cached.read_text(encoding="utf-8"))
            hits += 1
            continue
        try:
            html, _ = polite_fetch(url)
        except Exception as e:  # noqa: BLE001 — keep partials
            print(f"  centre {slug}: fetch error {type(e).__name__}: {e}")
            counties[url] = None
            continue
        cache_raw(SOURCE, f"centre_{slug}.html", html.encode("utf-8"))
        counties[url] = parse_centre_county(html)
        if i % 10 == 0 or i == len(urls):
            print(f"  centres done: {i}/{len(urls)} (bronze cache hits {hits})")
    return counties


def download_pdfs(rows: list[dict]) -> None:
    """Cache report PDFs to bronze/hiqa_ipas/pdf/ with SHA-256. Mutates rows."""
    pdf_dir = BRONZE / SOURCE / "pdf"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    todo = [r for r in rows if r["report_pdf_url"]]
    if len(todo) > PDF_CAP:
        sys.exit(
            f"ABORT before PDF stage: {len(todo)} PDFs exceeds cap {PDF_CAP} — "
            "corpus unexpectedly large, report back before downloading."
        )
    print(f"\nPDFs to cache: {len(todo)}")
    ok = hits = 0
    for i, r in enumerate(todo, 1):
        name = _pdf_basename(r["report_pdf_url"])
        path = pdf_dir / name
        meta_path = pdf_dir / (name + ".meta.json")
        if path.exists() and path.stat().st_size:  # resume from bronze
            side = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
            blob = path.read_bytes()
            r.update({
                "pdf_local_path": str(path),
                "pdf_sha256": side.get("sha256") or sha256_bytes(blob),
                "pdf_bytes": len(blob),
                "source_last_modified": side.get("source_last_modified"),
            })
            ok += 1
            hits += 1
            continue
        try:
            payload, meta = polite_fetch(r["report_pdf_url"], binary=True)
        except Exception as e:  # noqa: BLE001 — index row survives a dead PDF link
            print(f"  pdf {name}: fetch error {type(e).__name__}: {e}")
            continue
        path, digest = cache_raw(f"{SOURCE}/pdf", name, payload)
        meta_path.write_text(json.dumps({
            "sha256": digest,
            "source_last_modified": meta["source_last_modified"],
            "fetched_at": meta["fetched_at"],
            "source_url": meta["source_url"],
        }), encoding="utf-8")
        r.update({
            "pdf_local_path": str(path),
            "pdf_sha256": digest,
            "pdf_bytes": len(payload),
            "source_last_modified": meta["source_last_modified"],
        })
        ok += 1
        if i % 10 == 0 or i == len(todo):
            print(f"  pdfs done: {i}/{len(todo)} (bronze cache hits {hits})")
    print(f"  pdf ok: {ok}/{len(todo)} (fresh {ok - hits}, cached {hits})")


def run(max_pages: int = MAX_PAGES) -> None:
    print(f"HIQA IPAS inspections — facet term_node_tid_depth={IPAS_FACET}")
    rows = enumerate_reports(max_pages)
    if not rows:
        sys.exit("BLOCKED: 0 report tiles parsed — listing markup changed or facet gone.")

    counties = fetch_counties(rows)
    for r in rows:
        r["county"] = counties.get(r["centre_url"])
        r.setdefault("pdf_local_path", None)
        r.setdefault("pdf_sha256", None)
        r.setdefault("pdf_bytes", None)
        r.setdefault("source_last_modified", None)

    download_pdfs(rows)

    fetched = now_iso()
    for r in rows:
        r.update({
            "provider_name": None,  # NOT published by HIQA outside the PDF text
            "source_url": r["report_pdf_url"] or f"{BASE}?term_node_tid_depth={IPAS_FACET}",
            "source_document_hash": r["pdf_sha256"] or r["listing_page_hash"],
            "fetched_at": fetched,
            "source_published_date": r["publication_date"],
            "extraction_method": "html_scrape+pdf_cache",
            "confidence": "high",
            "privacy_tier": "public",  # facility-level only; no resident data
            "join_caveat": JOIN_CAVEAT,
        })

    df = pl.DataFrame(rows, schema_overrides={
        "provider_name": pl.Utf8, "pdf_local_path": pl.Utf8, "pdf_sha256": pl.Utf8,
        "pdf_bytes": pl.Int64, "source_last_modified": pl.Utf8, "county": pl.Utf8,
    }).unique(subset=["source_url"], keep="first").sort(
        "publication_date", "centre_id", descending=[True, False],
    )
    out = write_silver("hiqa_ipas_inspections", df)

    # ---- profile ----------------------------------------------------------
    n = df.height
    print(f"\nSILVER: {out}  rows={n}")
    print(f"  distinct centres: {df['centre_id'].n_unique()} "
          f"(names: {df['centre_name'].n_unique()})")
    print(f"  inspection dates: {df['inspection_date'].min()} … {df['inspection_date'].max()}")
    print(f"  published:        {df['publication_date'].min()} … {df['publication_date'].max()}")
    pdf_ok = df.filter(pl.col("pdf_sha256").is_not_null()).height
    print(f"  pdf coverage: {pdf_ok}/{n} ({100 * pdf_ok / n:.1f}%)")
    counties_df = (df.unique(subset=["centre_id"]).group_by("county").len()
                   .sort("len", descending=True))
    print(f"  county coverage (by centre): {counties_df.height} distinct values")
    for r in counties_df.head(30).to_dicts():
        print(f"    {r['len']:>3}  {r['county']}")
    print("  null rates:")
    for c in ("centre_id", "centre_name", "county", "inspection_date",
              "publication_date", "report_pdf_url", "report_type"):
        nulls = df[c].null_count()
        if nulls:
            print(f"    {c}: {nulls}/{n}")
    if not any(df[c].null_count() for c in df.columns if c != "provider_name"):
        print("    (none outside provider_name placeholder)")
    leak = df.filter(pl.col("report_type") != "International Protection Accommodation")
    if leak.height:
        print(f"  WARNING: {leak.height} rows with non-IPAS report_type — facet leak?")


if __name__ == "__main__":
    run(int(sys.argv[1]) if len(sys.argv) > 1 else MAX_PAGES)
