"""Overnight breadth harvest: pull minutes for ALL 31 councils from council_seeds.csv,
process anything new (fitz text layer, winocr for scans), and merge into the corpus.

EXPERIMENTAL sandbox. Fixes the v2 crawler's miss: its "filename must contain 'minute'"
filter was too strict for ~11 councils (Limerick, Meath, Mayo, Tipperary, Westmeath,
Wexford, Wicklow, Sligo, Longford, Fingal, Roscommon — FINDINGS.md group C). This pass:
  - fetches each council's seed page + one hop of archive/year subpages;
  - keeps links whose href OR anchor text mentions minutes (not just the filename);
  - dedupes against every URL already in meetings.jsonl / meetings_v2.jsonl /
    meetings_clean.jsonl / quarantine.jsonl;
  - processes new docs with the SAME doc_type/classify machinery as the consolidation
    (fitz if text layer, winocr if scanned — bounded pages), appends to
    meetings_clean.jsonl / quarantine.jsonl / corpus/, and regenerates
    QUALITY_ASSESSMENT.md.
CMIS portals (Dublin City, DLR) are skipped and logged — they need a bespoke scraper.

Bounds: MAX_NEW_PER_COUNCIL, MAX_SUBPAGES, MAX_OCR_PAGES_PER_DOC, SLEEP_S between
fetches, and a global deadline via env NIGHT_DEADLINE_TS (unix ts; stop cleanly after).

Usage: python night_harvest.py [--council "Limerick"] [--max-per-council 30]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import fitz
import requests
from bs4 import BeautifulSoup

import council_minutes_consolidate as base

HERE = Path(__file__).resolve().parent
SLEEP_S = 1.5
MAX_SUBPAGES = 10
MAX_OCR_PAGES_PER_DOC = 80
DPI = 200

_MINUTE_HINT = re.compile(r"minute|miontuairisc", re.I)
_ARCHIVE_HINT = re.compile(r"minute|meeting|agenda|20(1\d|2\d)|archive", re.I)
_SKIP_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg|css|js|ico|xml|zip|docx?|xlsx?)($|\?)", re.I)


def _winocr_run():
    from PIL import Image
    import winocr

    def run(png: bytes) -> list[str]:
        r = winocr.recognize_pil_sync(Image.open(io.BytesIO(png)))
        return [ln["text"] for ln in r.get("lines", [])]

    return run


_PW = None  # lazily-started Playwright browser (env NIGHT_PW=1); one per process


class _FakeResp:
    """requests.Response stand-in for Playwright-fetched content."""

    status_code = 200

    def __init__(self, content: bytes, ctype: str):
        self.content = content
        self.headers = {"content-type": ctype}

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


def _pw():
    global _PW  # noqa: PLW0603
    if _PW is None and os.environ.get("NIGHT_PW") == "1":
        from pw_fetch import PWFetch

        _PW = PWFetch()
    return _PW


def _get(url: str, timeout: int = 60) -> requests.Response | _FakeResp | None:
    try:
        r = requests.get(url, headers=base.HDRS, timeout=timeout)
        if r.status_code == 200:
            return r
    except requests.RequestException:
        pass
    if pw := _pw():  # WAF 403 / JS shell fallback — browser render or context-request bytes
        if url.lower().endswith(".pdf") or ".pdf?" in url.lower():
            b = pw.bytes(url)
            return _FakeResp(b, "application/pdf") if b else None
        h = pw.html(url)
        return _FakeResp(h.encode(), "text/html") if h else None
    return None


def _links(page_url: str, html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = urljoin(page_url, a["href"].strip())
        if _SKIP_EXT.search(href) or href.startswith(("mailto:", "javascript:")):
            continue
        out.append((href, a.get_text(" ", strip=True)[:120]))
    return out


def harvest_council(la: str, seed: str, portal: str, known: set[str], cap: int, log,
                    deep: bool = False) -> list[str]:
    """Return new candidate minutes-doc URLs for one council. deep = 2 hops, more
    subpages, and a relaxed hint (agenda/meeting PDFs too — classify() quarantines
    the non-minutes ones, so recall beats precision here)."""
    if portal == "cmis":
        log(f"  {la}: CMIS portal — skipped (moderngov_harvest.py covers Dublin City/Fingal)")
        return []
    max_pages = 25 if deep else MAX_SUBPAGES
    r = _get(seed)
    if not r:
        log(f"  {la}: seed fetch FAILED {seed}")
        return []
    pages = [(seed, r.text)]
    seen_pages = {seed}
    host = urlparse(seed).netloc
    frontier = [(seed, r.text)]
    for _hop in range(2 if deep else 1):
        nxt = []
        for page_url, html in frontier:
            for href, txt in _links(page_url, html):
                if len(pages) >= max_pages:
                    break
                if urlparse(href).netloc != host or href in seen_pages:
                    continue
                if href.lower().endswith(".pdf"):
                    continue
                if _ARCHIVE_HINT.search(href) or _ARCHIVE_HINT.search(txt):
                    time.sleep(SLEEP_S)
                    sub = _get(href)
                    seen_pages.add(href)
                    if sub and "text/html" in sub.headers.get("content-type", ""):
                        pages.append((href, sub.text))
                        nxt.append((href, sub.text))
            if len(pages) >= max_pages:
                break
        frontier = nxt
        if not frontier or len(pages) >= max_pages:
            break

    relaxed = re.compile(r"minute|miontuairisc|agenda|meeting", re.I)
    cands: list[str] = []
    for page_url, html in pages:
        for href, txt in _links(page_url, html):
            if href in known or href in cands:
                continue
            is_pdf = href.lower().endswith(".pdf") or ".pdf?" in href.lower()
            hint_rx = relaxed if deep else _MINUTE_HINT
            hinted = hint_rx.search(href) or hint_rx.search(txt)
            if is_pdf and hinted:
                cands.append(href)
            elif portal == "html" and hinted and urlparse(href).netloc == host and not is_pdf:
                # HTML-minutes councils (Clare/Wicklow/Sligo): meeting pages are html docs
                if re.search(r"20(1\d|2\d)", href + txt) and _MINUTE_HINT.search(href + txt):
                    cands.append(href)
            if len(cands) >= cap:
                break
        if len(cands) >= cap:
            break
    log(f"  {la}: {len(pages)} pages crawled -> {len(cands)} new candidates")
    return cands


def process_doc(url: str, la: str, ocr, log) -> tuple[dict, list[dict], str]:
    """Fetch + extract + classify one doc. Returns (record, votes, text)."""
    fname = url.split("/")[-1][:80] or url[-80:]
    rec = {"url": url, "local_authority": la, "meeting": fname}
    text, dvotes = "", []
    try:
        r = _get(url, timeout=90)
        if r is None:
            rec["status"] = "fetch_fail"
        elif "pdf" in r.headers.get("content-type", "") or url.lower().endswith(".pdf"):
            doc = fitz.open(stream=r.content, filetype="pdf")
            native = sum(len(p.get_text().strip()) for p in doc)
            rec["n_pages"] = len(doc)
            if native >= base.TEXT_MIN * max(1, len(doc)):
                text = "\n".join(p.get_text() for p in doc)
                rec["status"] = "text"
                dvotes = base.votes_pdf(doc, la, fname)
            else:
                n = min(len(doc), MAX_OCR_PAGES_PER_DOC)
                text = "\n".join(
                    "\n".join(ocr(doc[i].get_pixmap(dpi=DPI).tobytes("png"))) for i in range(n)
                )
                rec["status"] = "ocr_winocr"
                if n < len(doc):
                    rec["ocr_truncated_at"] = n
        else:
            soup = BeautifulSoup(r.text, "html.parser")
            for x in soup(["script", "style", "nav", "header", "footer"]):
                x.decompose()
            text = soup.get_text("\n", strip=True)
            rec["status"] = "html"
    except Exception as e:  # noqa: BLE001
        rec["status"] = f"err_{type(e).__name__}"

    dtype = base.doc_type(url, text)
    rec["doc_type"] = dtype
    rec["text_chars"] = len(text)
    rec.update(base.parse_struct(text))
    ok, reason = base.classify(rec, text, dtype)
    rec["clean"], rec["reason"] = ok, reason
    return rec, (dvotes if ok else []), text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", default=None, help="one name or comma-separated list")
    ap.add_argument("--max-per-council", type=int, default=30)
    ap.add_argument("--deep", action="store_true")
    args = ap.parse_args()
    deadline = float(os.environ.get("NIGHT_DEADLINE_TS", "0")) or None

    def log(msg: str) -> None:
        print(msg, flush=True)

    known: set[str] = set()
    for fn in ("meetings.jsonl", "meetings_v2.jsonl", "meetings_clean.jsonl",
               "quarantine/quarantine.jsonl"):
        p = HERE / fn
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    u = json.loads(line).get("url")
                    if u:
                        known.add(u)
    log(f"known URLs: {len(known)}")

    seeds = list(csv.DictReader(open(HERE / "council_seeds.csv", encoding="utf-8")))
    if args.council:
        want = {c.strip() for c in args.council.split(",")}
        seeds = [s for s in seeds if s["local_authority"] in want]

    ocr = _winocr_run()
    clean = [json.loads(l) for l in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    quar = [json.loads(l) for l in (HERE / "quarantine/quarantine.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    votes = [json.loads(l) for l in (HERE / "member_votes_all.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]

    n_new_clean = n_new_quar = 0
    stopped = False
    for s in seeds:
        if deadline and time.time() > deadline:
            log("DEADLINE reached — stopping harvest cleanly")
            stopped = True
            break
        la = s["local_authority"]
        cands = harvest_council(la, s["seed_url"], s["portal"], known, args.max_per_council, log,
                                deep=args.deep)
        for u in cands:
            if deadline and time.time() > deadline:
                stopped = True
                break
            time.sleep(SLEEP_S)
            rec, dvotes, text = process_doc(u, la, ocr, log)
            known.add(u)
            if rec["clean"]:
                cdir = base.CORPUS / base.slug(la)
                cdir.mkdir(exist_ok=True)
                stem = base.slug(rec["meeting"])[:80]
                (cdir / (stem + ".txt")).write_text(text, encoding="utf-8")
                rec["text_path"] = f"corpus/{base.slug(la)}/{stem}.txt"
                clean.append(rec)
                votes += dvotes
                n_new_clean += 1
            else:
                quar.append(rec)
                n_new_quar += 1
            log(f"    [{la}] {rec['meeting'][:55]} ({rec['status']}) -> "
                f"{'CLEAN' if rec['clean'] else rec['reason']} chars={rec['text_chars']}")
        if stopped:
            break

    votes = base.norm_members(votes)
    (HERE / "meetings_clean.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in clean), encoding="utf-8")
    (HERE / "quarantine/quarantine.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in quar), encoding="utf-8")
    (HERE / "member_votes_all.jsonl").write_text(
        "\n".join(json.dumps(v, ensure_ascii=False) for v in votes), encoding="utf-8")
    base.write_quality_report(clean, quar, votes)
    log(f"\nHARVEST DONE new_clean={n_new_clean} new_quarantined={n_new_quar} "
        f"total_clean={len(clean)} stopped_at_deadline={stopped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
