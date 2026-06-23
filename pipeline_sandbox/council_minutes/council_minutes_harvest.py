"""Discover + characterise council meeting-minutes sources across all 31 local authorities.

For each council (from council_domains.csv): find a meetings/minutes page, collect minutes PDF
links, then SAMPLE one recent PDF — classify born-digital (fitz) vs scanned (OCR), and on the
sample extract a light structure (agenda items / motions / decisions / vote markers).

Safe local engine = rapidocr (CPU). Bounded: OCR only the first N pages of one sample per council
(agenda + opening decisions live early). The full "OCR everything" pass is council_minutes_pipeline.py
with --engine paddle-gpu, off-box. Writes minutes_sources.csv incrementally (partial-safe).
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
SAMPLE_OCR_PAGES = 3
TEXT_MIN = 80

CANDIDATE_PATHS = [
    "/council-meetings", "/en/council-meetings", "/meetings", "/minutes",
    "/your-council/council-meetings", "/your-council/meetings",
    "/council/meetings", "/council/council-meetings",
    "/en/governance-administration/your-county-council/monthly-council-meetings",
    "/services/your-council/council-meeting-archive",
    "/your-council/council-meetings-agendas-and-minutes",
    "/council/meetings-agendas-and-minutes", "/agendas-and-minutes",
    "/your-council/meetings-agendas-minutes", "/democratic-services",
]
LINK_RX = re.compile(r"meeting|minute|agenda|miontuair|mion|democrat", re.I)
MIN_RX = re.compile(r"minute|mion|miontuair", re.I)


def get(url, timeout=20):
    try:
        r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:  # noqa: BLE001
        return None


def discover_meetings_page(domain: str) -> tuple[str | None, list[str]]:
    """Return (best_meetings_page, [pdf_urls]). Tries homepage links + candidate paths."""
    base = f"https://www.{domain}"
    home = get(base) or get(f"https://{domain}")
    candidates = []
    if home:
        soup = BeautifulSoup(home.text, "html.parser")
        for a in soup.find_all("a", href=True):
            if LINK_RX.search(a.get_text(" ", strip=True) + " " + a["href"]):
                candidates.append(urljoin(home.url, a["href"]))
    candidates += [base + p for p in CANDIDATE_PATHS]
    # dedup, keep non-pdf pages
    seen, pages = set(), []
    for c in candidates:
        if c in seen or c.lower().endswith(".pdf"):
            continue
        seen.add(c)
        pages.append(c)

    best, best_pdfs = None, []
    for page in pages[:14]:
        r = get(page)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = [urljoin(r.url, a["href"]) for a in soup.find_all("a", href=True)
                if ".pdf" in a["href"].lower()]
        min_pdfs = [u for u in pdfs if MIN_RX.search(u)]
        chosen = min_pdfs or pdfs
        if len(chosen) > len(best_pdfs):
            best, best_pdfs = r.url, list(dict.fromkeys(chosen))
        if len(best_pdfs) >= 5:
            break
    return best, best_pdfs


def classify_and_sample(pdf_url: str, ocr) -> dict:
    import fitz  # noqa: PLC0415
    r = get(pdf_url, timeout=60)
    if not r:
        return {"sample_url": pdf_url, "kind": "fetch_fail"}
    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception:  # noqa: BLE001
        return {"sample_url": pdf_url, "kind": "open_fail"}
    native = sum(len(p.get_text().strip()) for p in doc)
    n_pages = len(doc)
    scanned = native < TEXT_MIN * max(1, n_pages)
    # build sample text
    if not scanned:
        text = "\n".join(p.get_text() for p in doc)
        kind = "text"
    else:
        lines = []
        for p in list(doc)[:SAMPLE_OCR_PAGES]:
            if ocr:
                lines += ocr(p.get_pixmap(dpi=200).tobytes("png"))
        text = "\n".join(lines)
        kind = "scanned"
    struct = parse_structure(text)
    return {"sample_url": pdf_url, "kind": kind, "n_pages": n_pages,
            "native_chars": native, "sample_text_chars": len(text), **struct,
            "_text": text}


_ITEM = re.compile(r"ITEM\s*N[O0]\.?\s*\d+[^\n]{0,90}", re.I)
_MOTION = re.compile(r"PROPOS[AE]L? (?:of|by)[^\n]{0,120}?(?:SECOND|seconded)[^\n]{0,80}", re.I)
_DEC = re.compile(r"\b(AGREED|N[O0]TED|CARRIED|LOST|ADOPTED|DEFERRED|APPROVED|RESOLVED)\b")
_VOTE = re.compile(r"(roll[\s-]?call|in favou?r|\bagainst\b|abstain|division|voted for|"
                   r"show of hands|\bT[áa]\b|\bN[íi]l\b)", re.I)


def parse_structure(t: str) -> dict:
    return {"agenda_items": len(_ITEM.findall(t)), "motions": len(_MOTION.findall(t)),
            "decisions": len(_DEC.findall(t)), "vote_markers": len(_VOTE.findall(t)),
            "named_rollcall": bool(re.search(r"roll[\s-]?call", t, re.I))}


def main() -> int:
    rows = list(csv.DictReader(open(HERE / "council_domains.csv", encoding="utf-8")))
    from rapidocr_onnxruntime import RapidOCR
    _r = RapidOCR()
    ocr = lambda png: [t for _, t, _ in (_r(png)[0] or [])]  # noqa: E731

    out_csv = HERE / "minutes_sources.csv"
    samples_dir = HERE / "samples"
    samples_dir.mkdir(exist_ok=True)
    fields = ["local_authority", "domain", "meetings_page", "n_minutes_pdf", "sample_url",
              "kind", "n_pages", "agenda_items", "motions", "decisions", "vote_markers",
              "named_rollcall"]
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for i, row in enumerate(rows):
            la, dom = row["local_authority"], row["domain"]
            page, pdfs = discover_meetings_page(dom)
            rec = {"local_authority": la, "domain": dom, "meetings_page": page or "",
                   "n_minutes_pdf": len(pdfs), "sample_url": "", "kind": "no_pdf_found"}
            if pdfs:
                s = classify_and_sample(pdfs[0], ocr)
                txt = s.pop("_text", "")
                rec.update({k: s.get(k, "") for k in fields if k in s})
                if txt:
                    (samples_dir / f"{re.sub(r'[^a-z]+','_',la.lower())}.txt").write_text(
                        txt, encoding="utf-8")
            w.writerow({k: rec.get(k, "") for k in fields})
            fh.flush()
            print(f"[{i+1}/{len(rows)}] {la:26} pdfs={rec['n_minutes_pdf']:3} "
                  f"kind={rec.get('kind','')} items={rec.get('agenda_items','')} "
                  f"motions={rec.get('motions','')} votes={rec.get('vote_markers','')}")
    print(f"\nwrote {out_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
