"""Crawl SIPO's per-candidate GE2024 Election Expense Statements and download them.

This is the granular *per-candidate* tier (e.g. `768ce-grealish-noel` → one PDF),
distinct from the 18 party-level National-Agent statements already in
`data/bronze/scan_pdf/`. The candidate corpus was documented as the "untouched
corpus" in `data/_meta/sipo_ge2024_expenses_sources.md`; this script sources it.

Structure (server-rendered HTML, no JS, no OCR to discover links):

    2e0c0-dail-general-election-2024            (root collection)
      └─ <hash>-<constituency>                  (43 constituency sub-collections)
           └─ <hash>-<surname-first>            (~15-17 candidate pages each)
                └─ assets.sipo.ie/media/<id>/<uuid>.pdf   (one PDF: the statement)

Output (bronze + manifest, no parsing/OCR here — that is a later pass):

    data/bronze/sipo_candidate_expenses/<constituency_slug>/<candidate_slug>.pdf
    data/bronze/sipo_candidate_expenses/_manifest.csv

Idempotent / resumable: an already-downloaded PDF (matching size) is skipped, so a
re-run only fetches what's missing. Scope is **GE2024 only** (see the scope note in
the sources md).

Usage:
    python -m extractors.sipo_candidate_expenses_crawl            # full run
    python -m extractors.sipo_candidate_expenses_crawl --index-only   # no PDF download
    python -m extractors.sipo_candidate_expenses_crawl --limit-constituencies 1  # smoke
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import html
import json
import logging
import re
import sys
import time

import requests

from config import BRONZE_DIR, DATA_DIR
from services.logging_setup import setup_standalone_logging

log = logging.getLogger("sipo_candidate_expenses_crawl")

BASE = "https://www.sipo.ie"
ROOT_SLUG = "2e0c0-dail-general-election-2024"
OUT_DIR = BRONZE_DIR / "sipo_candidate_expenses"
MANIFEST = OUT_DIR / "_manifest.csv"
CKPT_DIR = OUT_DIR / "_ckpt"
# Slim, git-tracked source list (data/_meta/*.csv is kept via a .gitignore negation
# rule). The bronze _manifest.csv is gitignored; this is the durable, retrievable copy.
SOURCES_META = DATA_DIR / "_meta" / "sipo_candidate_expenses_sources.csv"
SOURCE_FIELDS = [
    "constituency_slug",
    "constituency_name",
    "candidate_slug",
    "candidate_name",
    "candidate_page_url",
    "doc_type",
    "doc_label",
    "pdf_url",
    "media_id",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (dail-tracker civic-data crawler; +contact via repo)"}
SLEEP_S = 0.4  # politeness between requests
TIMEOUT = 30

# A SIPO collection slug: 4-6 hex chars, a dash, then a kebab name.
RE_COLLECTION = re.compile(r"/en/collection/([0-9a-f]{4,6}-[a-z0-9][a-z0-9-]*)/")
RE_TITLE = re.compile(r"<title>\s*(?:SIPO\s*-\s*)?(.*?)\s*</title>", re.IGNORECASE | re.DOTALL)

# Non-candidate things that can appear as collection links and must be ignored.
SKIP_SLUGS = {ROOT_SLUG}


def _get(session: requests.Session, url: str, attempts: int = 4) -> str:
    """GET with simple exponential backoff; returns response text (or raises)."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as exc:  # noqa: BLE001 - network is the whole job here
            last = exc
            wait = SLEEP_S * (2**i)
            log.warning("GET %s failed (%s); retry in %.1fs", url, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}") from last


def _collection_links(html: str) -> list[str]:
    """Distinct candidate/sub-collection slugs on a page, in first-seen order."""
    seen: dict[str, None] = {}
    for slug in RE_COLLECTION.findall(html):
        if slug not in SKIP_SLUGS:
            seen.setdefault(slug, None)
    return list(seen)


def _page_title(page_html: str) -> str:
    m = RE_TITLE.search(page_html)
    if not m:
        return ""
    return re.sub(r"\s+", " ", html.unescape(m.group(1))).strip()


def _slug_url(slug: str) -> str:
    return f"{BASE}/en/collection/{slug}/"


# Each document on a candidate page carries a title paragraph followed by its PDF:
#   <p reboot-markdown-document-title id="...">Walsh, Noel G. - GE 2024 Expense
#   Statement</p> ... <a href="https://assets.sipo.ie/media/<id>/<uuid>.pdf">
# Anchor on the title paragraph and take the next media link.
RE_DOC = re.compile(
    r"reboot-markdown-document-title[^>]*>(?P<label>.*?)</p>"
    r".*?(?P<url>https://assets\.sipo\.ie/media/(?P<mid>\d+)/[0-9a-f-]+\.pdf)",
    re.DOTALL,
)


def _classify(label: str) -> str:
    """expense / donation / other. SIPO labels vary: 'Expense Statement', the
    abbreviation 'EES' (Election Expense Statement), 'Donation Statement', 'EDS'."""
    low = label.lower()
    if "expense" in low or re.search(r"\bees\b", low):
        return "expense_statement"
    if "donation" in low or re.search(r"\beds\b", low):
        return "donation_statement"
    return "other"


def _candidate_documents(page_html: str) -> list[dict]:
    """All published documents on a candidate page, labelled and typed."""
    docs: list[dict] = []
    for m in RE_DOC.finditer(page_html):
        label = re.sub(r"<[^>]+>", " ", m.group("label"))
        label = re.sub(r"\s+", " ", html.unescape(label)).strip()
        docs.append(dict(doc_label=label, doc_type=_classify(label), pdf_url=m.group("url"), media_id=m.group("mid")))
    return docs


def _discover_constituency(session: requests.Session, c_slug: str) -> list[dict]:
    """Discover one constituency's candidate documents (rows). Network-bound."""
    c_html = _get(session, _slug_url(c_slug))
    time.sleep(SLEEP_S)
    c_name = _page_title(c_html)
    candidates = [s for s in _collection_links(c_html) if s != c_slug]
    log.info("  %s (%s): %d candidates", c_slug, c_name, len(candidates))

    rows: list[dict] = []
    for cand_slug in candidates:
        cand_html = _get(session, _slug_url(cand_slug))
        time.sleep(SLEEP_S)
        cand_name = _page_title(cand_html)
        docs = _candidate_documents(cand_html)
        base = dict(
            constituency_slug=c_slug,
            constituency_name=c_name,
            candidate_slug=cand_slug,
            candidate_name=cand_name,
            candidate_page_url=_slug_url(cand_slug),
        )
        if not docs:
            log.warning("    no documents on candidate page %s (%s)", cand_slug, cand_name)
            rows.append({**base, "doc_label": "", "doc_type": "", "pdf_url": "", "media_id": "", "status": "NO_PDF"})
            continue
        for d in docs:
            rows.append(
                {
                    **base,
                    "doc_label": d["doc_label"],
                    "doc_type": d["doc_type"],
                    "pdf_url": d["pdf_url"],
                    "media_id": d["media_id"],
                    "status": "FOUND",
                }
            )
    return rows


def discover(session: requests.Session, limit_constituencies: int | None) -> list[dict]:
    """Two-level crawl: root → constituencies → candidates, checkpointed per
    constituency to ``_ckpt/<c_slug>.json`` so a re-run resumes instantly (only
    not-yet-discovered constituencies hit the network)."""
    CKPT_DIR.mkdir(parents=True, exist_ok=True)
    root_html = _get(session, _slug_url(ROOT_SLUG))
    time.sleep(SLEEP_S)
    constituencies = _collection_links(root_html)
    log.info("root: %d sub-collections (constituencies)", len(constituencies))
    if limit_constituencies:
        constituencies = constituencies[:limit_constituencies]

    rows: list[dict] = []
    for ci, c_slug in enumerate(constituencies, 1):
        ckpt = CKPT_DIR / f"{c_slug}.json"
        if ckpt.exists():
            c_rows = json.loads(ckpt.read_text(encoding="utf-8"))
            log.info("[%d/%d] %s: %d rows (cached)", ci, len(constituencies), c_slug, len(c_rows))
        else:
            log.info("[%d/%d] discovering %s ...", ci, len(constituencies), c_slug)
            c_rows = _discover_constituency(session, c_slug)
            ckpt.write_text(json.dumps(c_rows, ensure_ascii=False, indent=1), encoding="utf-8")
        rows.extend(c_rows)
    return rows


def download(session: requests.Session, rows: list[dict], doc_types: set[str]) -> None:
    """Download FOUND pdfs whose ``doc_type`` is in ``doc_types``; fills metadata.

    Files are named ``<candidate_slug>__<media_id>.pdf`` so a candidate with two
    published media IDs (SIPO commonly re-uploads the same statement) never
    collides. After download, rows sharing an sha256 *within the same candidate*
    are flagged ``duplicate_of`` (the first-seen media_id) so the OCR pass can
    ingest one and skip byte-identical re-uploads.
    """
    to_get = [r for r in rows if r["status"] == "FOUND" and r["pdf_url"] and r["doc_type"] in doc_types]
    log.info("downloading %d PDFs (doc_types=%s)", len(to_get), sorted(doc_types))
    for i, r in enumerate(to_get, 1):
        dest_dir = OUT_DIR / r["constituency_slug"]
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{r['candidate_slug']}__{r['media_id']}.pdf"
        r["local_path"] = str(dest.relative_to(BRONZE_DIR.parent))
        if dest.exists() and dest.stat().st_size > 0:
            data = dest.read_bytes()
            r["bytes"] = len(data)
            r["sha256"] = hashlib.sha256(data).hexdigest()
            r["status"] = "CACHED"
            continue
        try:
            resp = session.get(r["pdf_url"], headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.content
            dest.write_bytes(data)
            r["bytes"] = len(data)
            r["sha256"] = hashlib.sha256(data).hexdigest()
            r["status"] = "DOWNLOADED"
            log.info("  [%d/%d] %s -> %s (%d bytes)", i, len(to_get), r["candidate_name"], dest.name, len(data))
        except Exception as exc:  # noqa: BLE001
            r["status"] = "DOWNLOAD_FAILED"
            log.error("  [%d/%d] FAILED %s: %s", i, len(to_get), r["pdf_url"], exc)
        time.sleep(SLEEP_S)

    # Flag byte-identical re-uploads within a candidate (first media_id = canonical).
    first_sha: dict[tuple[str, str], str] = {}
    for r in rows:
        sha = r.get("sha256")
        if not sha:
            continue
        key = (r["constituency_slug"], r["candidate_slug"])
        if first_sha.get(key) == sha:
            r["duplicate_of"] = next(
                o["media_id"]
                for o in rows
                if o["candidate_slug"] == r["candidate_slug"]
                and o.get("sha256") == sha
                and o["media_id"] != r["media_id"]
            )
        else:
            first_sha.setdefault(key, sha)
            r["duplicate_of"] = ""


FIELDS = [
    "constituency_slug",
    "constituency_name",
    "candidate_slug",
    "candidate_name",
    "candidate_page_url",
    "doc_type",
    "doc_label",
    "pdf_url",
    "media_id",
    "local_path",
    "bytes",
    "sha256",
    "duplicate_of",
    "status",
]


def write_manifest(rows: list[dict]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with MANIFEST.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    log.info("manifest -> %s (%d rows)", MANIFEST, len(rows))


def write_sources_meta(rows: list[dict]) -> None:
    """Write the slim, git-tracked source list (no run-specific size/sha/status)."""
    SOURCES_META.parent.mkdir(parents=True, exist_ok=True)
    src = sorted(
        (r for r in rows if r["pdf_url"]), key=lambda r: (r["constituency_slug"], r["candidate_slug"], r["doc_type"])
    )
    with SOURCES_META.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=SOURCE_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in src:
            w.writerow(r)
    log.info("source list -> %s (%d docs)", SOURCES_META, len(src))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--index-only", action="store_true", help="discover candidate→PDF URLs and write the manifest, no download"
    )
    ap.add_argument(
        "--limit-constituencies", type=int, default=None, help="only crawl the first N constituencies (smoke test)"
    )
    ap.add_argument(
        "--doc-types",
        default="expense_statement",
        help="comma-separated doc types to DOWNLOAD (manifest always lists "
        "all). Options: expense_statement, donation_statement, other, all",
    )
    args = ap.parse_args()
    if args.doc_types.strip().lower() == "all":
        doc_types = {"expense_statement", "donation_statement", "other"}
    else:
        doc_types = {t.strip() for t in args.doc_types.split(",") if t.strip()}

    setup_standalone_logging("sipo_candidate_expenses_crawl")
    session = requests.Session()

    rows = discover(session, args.limit_constituencies)
    found = sum(r["status"] == "FOUND" for r in rows)
    no_pdf = sum(r["status"] == "NO_PDF" for r in rows)
    by_type: dict[str, int] = {}
    for r in rows:
        if r["status"] == "FOUND":
            by_type[r["doc_type"]] = by_type.get(r["doc_type"], 0) + 1
    log.info("discovered %d rows: %d docs (%s), %d candidates with no docs", len(rows), found, by_type, no_pdf)

    if not args.index_only:
        download(session, rows, doc_types)

    write_manifest(rows)
    write_sources_meta(rows)

    by_status: dict[str, int] = {}
    for r in rows:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    log.info("done. status breakdown: %s", by_status)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
