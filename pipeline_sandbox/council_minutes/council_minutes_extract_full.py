"""Classify + extract council meeting minutes IN FULL.

For each council (council_domains.csv):
  1. Discover the meetings page (expanded path list) and collect ALL minutes-PDF URLs.
  2. For each PDF: classify born-digital (fitz) vs scanned (image); extract text accordingly
     (fitz for text; rapidocr CPU for scanned, BOUNDED — full scanned bulk is staged for the
     off-box PaddleOCR-GPU run via council_minutes_pipeline.py).
  3. Parse a structured record: agenda items, motions (proposer/seconder), decisions, and
     NAMED roll-call votes (Carlow-style "For/Against/Abstain ... Result N For, M Against").
  4. Write meetings.jsonl (one row per PDF) + council_classification.csv (one row per council).

Bounded so it finishes unattended: MAX_PDF_PER_COUNCIL, and scanned docs OCR'd up to
MAX_SCANNED_DOCS / MAX_OCR_PAGES — anything beyond is recorded as 'staged_offbox'.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
HDRS = {"User-Agent": UA}

MAX_PDF_PER_COUNCIL = 40
MAX_SCANNED_DOCS = 8
MAX_OCR_PAGES = 12
TEXT_MIN = 80

CANDIDATE_PATHS = [
    "/council-meetings", "/en/council-meetings", "/meetings", "/minutes",
    "/your-council/council-meetings", "/your-council/meetings", "/council/meetings",
    "/council/council-meetings", "/agendas-and-minutes", "/council-meetings-management",
    "/en/governance-administration/your-county-council/monthly-council-meetings",
    "/services/your-council/your-council-services/council-meeting-archive",
    "/your-council/about-the-council/minutes-of-council-meetings",
    "/eng/your_council/council_meetings", "/coco/en/about_us/local_representatives",
    "/your-council/council-meetings-agendas-and-minutes", "/democratic-services",
    "/about-the-council/meetings", "/council/meetings-and-agendas",
]
LINK_RX = re.compile(r"meeting|minute|agenda|miontuair|democrat", re.I)
MIN_RX = re.compile(r"minute|mion|miontuair", re.I)

_ITEM = re.compile(r"ITEM\s*N[O0]\.?\s*\d+[^\n]{0,90}", re.I)
_MOTION = re.compile(r"[Pp]ropos(?:ed|al)\s+(?:by|of)[^\n]{0,120}?(?:[Ss]econd)[^\n]{0,80}")
_DEC = re.compile(r"\b(AGREED|N[O0]TED|CARRIED|LOST|ADOPTED|DEFERRED|APPROVED|RESOLVED|DEFEATED)\b")
_ROLLCALL = re.compile(r"roll[\s-]?call vote", re.I)
_RESULT = re.compile(r"Result[:\s]+(\d+)\s*For[,\s]+(?:(\d+)\s*Against)?[,\s]*(?:(\d+)\s*Abstain)?",
                     re.I)
_MOTION_OUTCOME = re.compile(r"Motion\s+(Carried|Defeated|Lost)", re.I)
_VOTE_MARK = re.compile(r"(in favou?r|\bagainst\b|abstain|division|voted for|show of hands|"
                        r"\bT[áa]\b|\bN[íi]l\b)", re.I)


def get(url, timeout=25):
    try:
        r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:  # noqa: BLE001
        return None


def discover(domain: str):
    base = f"https://www.{domain}"
    home = get(base) or get(f"https://{domain}")
    cands = []
    if home:
        soup = BeautifulSoup(home.text, "html.parser")
        for a in soup.find_all("a", href=True):
            if LINK_RX.search(a.get_text(" ", strip=True) + " " + a["href"]):
                cands.append(urljoin(home.url, a["href"]))
    cands += [base + p for p in CANDIDATE_PATHS]
    seen, pages = set(), []
    for c in cands:
        if c in seen or c.lower().endswith(".pdf"):
            continue
        seen.add(c)
        pages.append(c)
    best_page, all_pdfs = None, []
    for page in pages[:18]:
        r = get(page)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = [urljoin(r.url, a["href"]) for a in soup.find_all("a", href=True)
                if ".pdf" in a["href"].lower()]
        minpdfs = [u for u in pdfs if MIN_RX.search(u)]
        chosen = minpdfs or pdfs
        if len(chosen) > len(all_pdfs):
            best_page, all_pdfs = r.url, list(dict.fromkeys(chosen))
        if len(all_pdfs) >= MAX_PDF_PER_COUNCIL:
            break
    return best_page, all_pdfs[:MAX_PDF_PER_COUNCIL]


def parse_struct(t: str) -> dict:
    results = [{"for": int(m.group(1)), "against": int(m.group(2) or 0),
                "abstain": int(m.group(3) or 0)} for m in _RESULT.finditer(t)]
    return {
        "agenda_items": len(_ITEM.findall(t)),
        "motions": len(_MOTION.findall(t)),
        "decisions": len(_DEC.findall(t)),
        "rollcall_votes": len(_ROLLCALL.findall(t)),
        "named_vote_results": results,
        "motion_outcomes": [m.group(1) for m in _MOTION_OUTCOME.finditer(t)],
        "vote_markers": len(_VOTE_MARK.findall(t)),
    }


def extract_one(url, ocr, scanned_budget):
    import fitz  # noqa: PLC0415
    r = get(url, timeout=70)
    if not r:
        return {"url": url, "status": "fetch_fail"}, scanned_budget
    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception:  # noqa: BLE001
        return {"url": url, "status": "open_fail"}, scanned_budget
    native = sum(len(p.get_text().strip()) for p in doc)
    n = len(doc)
    scanned = native < TEXT_MIN * max(1, n)
    if not scanned:
        text = "\n".join(p.get_text() for p in doc)
        status = "text"
    elif scanned_budget > 0 and ocr is not None:
        lines = []
        for p in list(doc)[:MAX_OCR_PAGES]:
            lines += ocr(p.get_pixmap(dpi=200).tobytes("png"))
        text = "\n".join(lines)
        status = "ocr"
        scanned_budget -= 1
    else:
        return ({"url": url, "status": "staged_offbox_scanned", "n_pages": n}, scanned_budget)
    return ({"url": url, "status": status, "n_pages": n, "text_chars": len(text),
             **parse_struct(text)}, scanned_budget)


def classify_council(rows: list[dict]) -> dict:
    done = [r for r in rows if r.get("status") in ("text", "ocr")]
    scanned = any(r.get("status") in ("ocr", "staged_offbox_scanned") for r in rows)
    text = any(r.get("status") == "text" for r in rows)
    named = sum(len(r.get("named_vote_results", [])) for r in done)
    rollcalls = sum(r.get("rollcall_votes", 0) for r in done)
    motions = sum(r.get("motions", 0) for r in done)
    fmt = ("mixed" if (scanned and text) else "scanned" if scanned else "born_digital"
           if text else "unknown")
    vote_style = ("named_rollcall" if named or rollcalls else
                  "proposer_seconder" if motions else "unknown")
    return {"format": fmt, "vote_style": vote_style, "named_vote_results": named,
            "rollcall_votes": rollcalls, "total_motions": motions, "docs_extracted": len(done)}


def main() -> int:
    councils = list(csv.DictReader(open(HERE / "council_domains.csv", encoding="utf-8")))
    from rapidocr_onnxruntime import RapidOCR
    _r = RapidOCR()
    ocr = lambda png: [t for _, t, _ in (_r(png)[0] or [])]  # noqa: E731

    jsonl = open(HERE / "meetings.jsonl", "w", encoding="utf-8")
    cls_csv = open(HERE / "council_classification.csv", "w", newline="", encoding="utf-8")
    cw = csv.DictWriter(cls_csv, fieldnames=[
        "local_authority", "domain", "meetings_page", "n_pdf", "format", "vote_style",
        "named_vote_results", "rollcall_votes", "total_motions", "docs_extracted"])
    cw.writeheader()

    for i, c in enumerate(councils):
        la, dom = c["local_authority"], c["domain"]
        page, pdfs = discover(dom)
        rows, budget = [], MAX_SCANNED_DOCS
        for u in pdfs:
            rec, budget = extract_one(u, ocr, budget)
            rec["local_authority"] = la
            rows.append(rec)
            jsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")
        jsonl.flush()
        summ = classify_council(rows)
        cw.writerow({"local_authority": la, "domain": dom, "meetings_page": page or "",
                     "n_pdf": len(pdfs), **summ})
        cls_csv.flush()
        print(f"[{i+1}/{len(councils)}] {la:24} pdf={len(pdfs):3} fmt={summ['format']:11} "
              f"votes={summ['vote_style']:17} named={summ['named_vote_results']} "
              f"motions={summ['total_motions']}")
    jsonl.close()
    cls_csv.close()
    print("\nwrote meetings.jsonl + council_classification.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
