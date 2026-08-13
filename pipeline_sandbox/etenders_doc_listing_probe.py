"""SANDBOX PROBE ONLY (no parquet, no gold writes) — Phase 0 of the eTenders
document-index plan: measure, on a stratified sample of tender IDs we already hold,
(a) whether `listContractDocuments.do?resourceId=<id>` resolves, (b) how many
documents each tender exposes, (c) the file-type mix. This is the measurement that
sizes Phase 1 (the full document-INDEX crawl, to run on the Hetzner box) and answers
the two unknowns the 2026-08-13 assessment left open: pre-2023 (old-platform) ID
attrition, and whether closed/awarded tenders keep their documents visible.

NOT A HARVEST: metadata listing pages only, never the documents themselves. Serial,
one request at a time, --delay spacing between EVERY request including pagination
pages, hard --max-requests cap. The anonymous listing/download path needs no login
and registers no supplier identity (verified 2026-08-13 with bare curl).

Parse contract (derived from a fetched live listing, resourceId 8809597):
  - docs table has id="T02"; one <tr> per document
  - per-row document id + filename come from
        <a href="#" onclick="downloadDocForAnonymous('<docId>')">filename.ext</a>
  - Title is the row's second <td>, Lang. the fifth
  - displaytag pagination links carry d-5419-p=<page>; a big tender's docs span
    pages, so the probe follows pagination until no new doc ids appear.

Outputs (gitignored runtime dir, append-per-row so a killed run keeps its rows):
  <out>/manifest.csv       one row per sampled tender
  <out>/documents.csv      one row per document seen
  <out>/summary.json       strata resolvability + docs-per-tender distribution
  <out>/diagnostics.txt    first ~300 chars of unclassifiable pages (few, capped)

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/etenders_doc_listing_probe.py
      ... --sample 200 --delay 1.5 --resume
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import html as html_mod
import json
import logging
import re
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import services.runtime_env  # noqa: E402,F401 — thread caps before Polars

import polars as pl  # noqa: E402
import requests  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from paths import runtime_path  # noqa: E402
from services.http_engine import fetch_text, polite_headers  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402

logger = logging.getLogger(__name__)

BASE = "https://www.etenders.gov.ie"
NOTICES_PARQUET = ROOT / "data/gold/parquet/procurement_notices.parquet"
# Browser UA clears WAFs that 403 bot UAs (same reasoning as the OGP CSV fetch);
# the From header keeps the operator contactable, which a bare browser UA is not.
HEADERS = polite_headers(browser=True, extra={"From": "p.glynn18@gmail.com", "Referer": BASE})

DOC_ANCHOR = re.compile(r"downloadDocForAnonymous\('(\d+)'\)\">([^<]+)</a>")
ROW = re.compile(r"<tr>(.*?)</tr>", re.S)
TD = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
TAGS = re.compile(r"<[^>]+>")
PAGE_LINK = re.compile(r"d-5419-p=(\d+)")

MANIFEST_FIELDS = [
    "tender_id", "year", "award_status", "http_status", "resolves", "page_kind",
    "n_docs", "n_pages", "doc_exts", "note",
]
DOC_FIELDS = ["tender_id", "document_id", "filename", "title", "lang"]


def _strip(cell: str) -> str:
    return re.sub(r"\s+", " ", TAGS.sub(" ", html_mod.unescape(cell))).strip()


def sample_tenders(parquet: Path, n: int, seed: int) -> pl.DataFrame:
    """Stratified sample over (year x award_status): every cell that exists gets
    at least 2 draws (else old sparse years vanish into proportional rounding),
    the rest proportional, deterministic under --seed."""
    df = (
        pl.scan_parquet(parquet)
        .select("tender_id", "published_date", "award_status")
        .with_columns(pl.col("published_date").dt.year().alias("year"))
        .filter(
            pl.col("tender_id").is_not_null()
            & (pl.col("tender_id").cast(pl.Utf8).str.strip_chars() != "")
            & (pl.col("tender_id").cast(pl.Utf8).str.strip_chars() != "NULL")
        )
        .unique(subset=["tender_id"])
        .collect()
    )
    total = len(df)
    cells = df.group_by("year", "award_status").len().sort("year", "award_status")
    picks: list[pl.DataFrame] = []
    for year, status, cell_n in cells.iter_rows():
        k = min(cell_n, max(2, round(n * cell_n / total)))
        # eq_missing, not ==: 1,398 rows have a null published_date, so null-year
        # cells exist and a plain == None filter silently drops every row.
        cell = df.filter(pl.col("year").eq_missing(year) & pl.col("award_status").eq_missing(status))
        picks.append(cell.sample(n=k, seed=seed))
    out = pl.concat(picks)
    if len(out) > n:  # proportional rounding overshoots; trim deterministically
        out = out.sample(n=n, seed=seed)
    logger.info("sampled %d of %d tenders across %d (year x status) cells", len(out), total, len(cells))
    return out.sort("year", "award_status", "tender_id")


def parse_listing(page_html: str) -> tuple[list[dict], set[int]]:
    """Return (doc rows, page numbers advertised by displaytag pagination)."""
    docs: list[dict] = []
    for tr in ROW.findall(page_html):
        anchor = DOC_ANCHOR.search(tr)
        if not anchor:
            continue
        tds = TD.findall(tr)
        docs.append({
            "document_id": anchor.group(1),
            "filename": _strip(anchor.group(2)),
            "title": _strip(tds[1]) if len(tds) > 1 else "",
            "lang": _strip(tds[4]) if len(tds) > 4 else "",
        })
    return docs, {int(p) for p in PAGE_LINK.findall(page_html)}


def classify(page_html: str) -> str:
    if 'id="T02"' in page_html:
        return "docs_table"
    low = page_html.lower()
    if 'name="password"' in low or "j_security_check" in low:
        return "login_wall"
    if "error" in low or "not available" in low or "invalid" in low:
        return "error_page"
    return "unclassified"


def probe_one(tender_id: str, delay: float, budget: list[int], diag: Path) -> tuple[dict, list[dict]]:
    """Fetch one tender's listing (+ pagination). budget is a mutable [remaining]
    request counter shared across the run — the hard cap lives there, not here."""
    # resolves is written 0/1, not True/False: CSV round-trips of Python bools are
    # dialect-dependent and the summary step must be able to sum the column.
    rec = {"tender_id": tender_id, "http_status": "", "resolves": 0,
           "page_kind": "", "n_docs": 0, "n_pages": 0, "doc_exts": "", "note": ""}
    all_docs: list[dict] = []
    seen_ids: set[str] = set()
    page_no, known_pages = 1, {1}
    full_page = 10  # displaytag page size floor: a page this full may have JS-button
    # pagination with no d-5419-p hrefs, so we speculatively try the next page and
    # let the no-fresh-rows check stop us on a repeat.
    try_next = False
    while (page_no in known_pages or try_next) and budget[0] > 0:
        url = (f"{BASE}/epps/cft/listContractDocuments.do?resourceId={tender_id}"
               + (f"&d-5419-p={page_no}" if page_no > 1 else ""))
        budget[0] -= 1
        time.sleep(delay)
        try:
            page_html, _ = fetch_text(url, headers=HEADERS, attempts=2)
            rec["http_status"] = "200"
        except requests.HTTPError as exc:
            rec["http_status"] = str(exc.response.status_code if exc.response is not None else "ERR")
            rec["note"] = f"http error p{page_no}"
            break
        except requests.RequestException as exc:
            rec["http_status"] = "EXC"
            rec["note"] = f"{type(exc).__name__} p{page_no}"
            break
        rec["n_pages"] = page_no
        kind = classify(page_html)
        if page_no == 1:
            rec["page_kind"] = kind
        if kind != "docs_table":
            if kind == "unclassified":
                with diag.open("a", encoding="utf-8") as fh:
                    fh.write(f"--- {tender_id} p{page_no} ---\n{_strip(page_html)[:300]}\n")
            break
        docs, pages = parse_listing(page_html)
        known_pages |= pages
        fresh = [d for d in docs if d["document_id"] not in seen_ids]
        if page_no > 1 and not fresh:  # displaytag served a repeat page: stop
            break
        seen_ids.update(d["document_id"] for d in fresh)
        for d in fresh:
            d["tender_id"] = tender_id
        all_docs.extend(fresh)
        try_next = len(fresh) >= full_page
        page_no += 1
    rec["resolves"] = int(rec["page_kind"] == "docs_table")
    rec["n_docs"] = len(all_docs)
    exts = Counter(Path(d["filename"]).suffix.lower().lstrip(".") or "none" for d in all_docs)
    rec["doc_exts"] = "|".join(f"{e}:{c}" for e, c in sorted(exts.items()))
    return rec, all_docs


def _append(path: Path, fields: list[str], rows: list[dict]) -> None:
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerows(rows)
        fh.flush()


def summarize(manifest: Path, out: Path) -> dict:
    if not manifest.exists():
        logger.warning("no manifest at %s (nothing probed); skipping summary", manifest)
        return {"sampled": 0, "resolved": 0, "resolved_pct": 0.0, "page_kind_tally": {}}
    df = pl.read_csv(manifest, schema_overrides={"tender_id": pl.Utf8, "http_status": pl.Utf8})
    resolved = df.filter(pl.col("resolves") == 1)
    docs_of_resolved = resolved.get_column("n_docs")
    by_year = (
        df.group_by("year")
        .agg(pl.len().alias("n"), pl.col("resolves").sum().alias("resolved"),
             pl.col("n_docs").filter(pl.col("resolves") == 1).mean().alias("mean_docs"))
        .sort("year")
    )
    by_status = (
        df.group_by("award_status")
        .agg(pl.len().alias("n"), pl.col("resolves").sum().alias("resolved"))
        .sort("award_status")
    )
    ext_tally: Counter = Counter()
    for spec in df.get_column("doc_exts"):
        for part in (spec or "").split("|"):
            if ":" in part:
                e, c = part.rsplit(":", 1)
                ext_tally[e] += int(c)
    summary = {
        "sampled": len(df),
        "resolved": len(resolved),
        "resolved_pct": round(100 * len(resolved) / len(df), 1) if len(df) else 0.0,
        "docs_per_resolved_tender": {
            "mean": round(float(docs_of_resolved.mean() or 0), 2),
            "p50": int(docs_of_resolved.median() or 0),
            "p90": int(docs_of_resolved.quantile(0.9) or 0),
            "max": int(docs_of_resolved.max() or 0),
            "zero_docs": int((docs_of_resolved == 0).sum()),
        } if len(resolved) else {},
        "by_year": by_year.to_dicts(),
        "by_award_status": by_status.to_dicts(),
        "file_ext_tally": dict(ext_tally.most_common()),
        "page_kind_tally": dict(Counter(df.get_column("page_kind").to_list())),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def run(args: argparse.Namespace) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest, documents = out / "manifest.csv", out / "documents.csv"
    diag = out / "diagnostics.txt"

    sample = sample_tenders(Path(args.parquet), args.sample, args.seed)
    done: set[str] = set()
    if args.resume and manifest.exists():
        with manifest.open(encoding="utf-8") as fh:
            done = {r["tender_id"] for r in csv.DictReader(fh)}
        logger.info("resume: %d of %d already probed", len(done), len(sample))

    budget = [args.max_requests]
    todo = [r for r in sample.iter_rows(named=True) if str(r["tender_id"]).strip() not in done]
    logger.info("probing %d tenders serially @ %.1fs spacing (request cap %d) -> %s",
                len(todo), args.delay, budget[0], out)
    for i, row in enumerate(todo, 1):
        tid = str(row["tender_id"]).strip()
        if budget[0] <= 0:
            logger.warning("request cap reached after %d tenders — rerun with --resume to finish", i - 1)
            break
        rec, docs = probe_one(tid, args.delay, budget, diag)
        rec["year"], rec["award_status"] = row["year"], row["award_status"]
        # documents BEFORE manifest: --resume keys on manifest presence, so the
        # manifest row must be the last thing to land — a kill between the two
        # writes must not mark a tender done with its doc rows lost.
        if docs:
            _append(documents, DOC_FIELDS, docs)
        _append(manifest, MANIFEST_FIELDS, [rec])
        if i % 20 == 0 or i == len(todo):
            logger.info("[%d/%d] last=%s resolves=%s docs=%d (requests left %d)",
                        i, len(todo), tid, rec["resolves"], rec["n_docs"], budget[0])

    summary = summarize(manifest, out)
    logger.info("SUMMARY: %d sampled, %d resolved (%.1f%%); docs/resolved-tender %s; kinds %s",
                summary["sampled"], summary["resolved"], summary["resolved_pct"],
                summary.get("docs_per_resolved_tender"), summary["page_kind_tally"])
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 0: stratified eTenders document-listing probe (sandbox).")
    ap.add_argument("--sample", type=int, default=200, help="tenders to sample (stratified year x award_status)")
    ap.add_argument("--delay", type=float, default=1.5, help="seconds between EVERY request; do not lower below 1.0")
    ap.add_argument("--seed", type=int, default=20260813)
    ap.add_argument("--max-requests", type=int, default=600, help="hard cap incl. pagination pages")
    ap.add_argument("--parquet", default=str(NOTICES_PARQUET))
    ap.add_argument("--out", default=str(runtime_path("etenders_doc_probe")))
    ap.add_argument("--resume", action="store_true", help="skip tender ids already in manifest.csv")
    args = ap.parse_args()
    if args.delay < 1.0:
        logger.error("--delay %.2f below the 1.0s politeness floor; refusing.", args.delay)
        return 2
    setup_standalone_logging("etenders_doc_listing_probe")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
