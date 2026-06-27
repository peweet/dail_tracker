"""Comprehensive OCR pass over the SCANNED councils → parse agendas → meeting_history.jsonl.

Born-digital councils were already parsed by the fan-out. This OCRs the recent (2024+) SCANNED minutes
(Galway City/County, Louth, Wicklow) with rapidocr across a bounded process pool (onnxruntime here is
CPU-only; PaddleOCR-GPU is the off-box path per the crash rule). Parses agenda items and appends to
meeting_history.jsonl (dedupe). Sandbox only.
"""
from __future__ import annotations

import json
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
MAX_PAGES = 20
WORKERS = 1  # serial: one OCR process at a time (no fan-out), per the crash-safety rule
MINUTE_RX = re.compile(r"minute|mion|miontuair|agenda", re.I)
YEAR_RX = re.compile(r"202[4-9]")

# scanned councils + their meetings index pages
SOURCES = {
    "Galway City": ["https://www.galwaycity.ie/services/your-council/your-council-services/council-meeting-archive"],
    "Galway County": ["https://www.galway.ie/en/council-meetings"],
    # 2026-06: old /services/your-council/council-meetings/ paths 404'd (site moved). Minutes now
    # live under /louth_county_council/minutes_of_statutory_meetings/<YEAR>/ — harvest follows the
    # 2024-2026 year sub-pages (signed-minutes-county-council-<month>-<year>.pdf).
    "Louth": ["https://www.louthcoco.ie/en/louth_county_council/minutes_of_statutory_meetings/",
              "https://www.louthcoco.ie/en/louth_county_council/agenda-council-meetings/"],
    "Wicklow": ["https://www.wicklow.ie/Living/Your-Council/Council-Meetings/Minutes-Agendas"],
}


def get(u, t=30):
    try:
        r = requests.get(u, headers=H, timeout=t, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:  # noqa: BLE001
        return None


def harvest(pages):
    pdfs, seen = [], set()
    todo = list(pages)
    # index-based queue (NOT `for pg in todo[:10]` — that froze the slice, so year
    # sub-pages appended below were never crawled; councils that list PDFs only on
    # /<year>/ sub-pages, e.g. Louth, yielded 0). Process up to 18 pages, following
    # year sub-pages discovered along the way.
    i = 0
    while i < len(todo) and i < 18:
        pg = todo[i]
        i += 1
        r = get(pg)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        # follow year sub-pages once
        for a in soup.find_all("a", href=True):
            u = urljoin(r.url, a["href"])
            tx = a.get_text(" ", strip=True)
            if u.lower().endswith(".pdf") and MINUTE_RX.search(u) and YEAR_RX.search(u + tx):
                if u not in seen:
                    seen.add(u); pdfs.append(u)
            elif YEAR_RX.search(tx) and MINUTE_RX.search(tx + u) and ".pdf" not in u.lower() and u not in todo and len(todo) < 18:
                todo.append(u)
    return pdfs[:24]


def agenda_items(text: str) -> list[str]:
    out = []
    for m in re.finditer(r"(?:ITEM\s*N[O0]\.?\s*\d+|^\s*\d{1,2}[.\)])\s*([A-Z][^\n]{5,95})", text, re.M):
        s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", re.sub(r"\s+", " ", m.group(1)).strip(" .-"))
        s = re.sub(r"\s+", " ", s)[:90]
        if s and not s.lower().startswith(("page", "minutes of", "present", "apolog", "in attend")):
            out.append(s)
    seen, u = set(), []
    for x in out:
        if x not in seen:
            seen.add(x); u.append(x)
    return u[:15]


def mdate(fn: str) -> str:
    fn = unquote(fn).rsplit("/", 1)[-1]
    m = re.search(r"(\d{1,2})[.\-\s](\d{1,2})[.\-\s](20\d{2})", fn)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"
    m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})", fn, re.I)
    return f"{m.group(1)} {m.group(2)} {m.group(3)}" if m else fn[:24]


def ocr_one(args):
    """Worker: download + OCR (rapidocr) + parse agenda. Own RapidOCR per process."""
    la, url = args
    try:
        import fitz
        from rapidocr_onnxruntime import RapidOCR
        ocr = RapidOCR()
        pdf = requests.get(url, headers=H, timeout=70).content
        doc = fitz.open(stream=pdf, filetype="pdf")
        native = sum(len(p.get_text().strip()) for p in doc)
        if native >= 80 * max(1, len(doc)):
            text = "\n".join(p.get_text() for p in doc)  # born-digital after all
        else:
            text = "\n".join(l for p in list(doc)[:MAX_PAGES]
                             for l in [t for _, t, _ in (ocr(p.get_pixmap(dpi=200).tobytes("png"))[0] or [])])
        items = agenda_items(text)
        return {"council": la, "file": unquote(url.split("/")[-1]), "date": mdate(url),
                "agenda_items": items, "source_url": url, "n_items": len(items)}
    except Exception as e:  # noqa: BLE001
        return {"council": la, "file": url.split("/")[-1], "error": type(e).__name__}


def main():
    jobs = []
    for la, pages in SOURCES.items():
        urls = harvest(pages)
        print(f"{la}: {len(urls)} recent scanned/minutes docs to OCR")
        jobs += [(la, u) for u in urls]
    print(f"\nOCR'ing {len(jobs)} docs with {WORKERS} workers ...")
    rows = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(ocr_one, j): j for j in jobs}
        for i, f in enumerate(as_completed(futs)):
            r = f.result()
            rows.append(r)
            tag = f"{r.get('n_items','ERR')}" if "error" not in r else r["error"]
            print(f"  [{i+1}/{len(jobs)}] {r['council']:14} {r.get('date','')[:12]:12} -> {tag}")

    good = [r for r in rows if r.get("agenda_items")]
    # merge into meeting_history.jsonl (dedupe by council+date+file)
    mh = [json.loads(l) for l in (HERE / "meeting_history.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    seen = {(r["council"], r.get("date", ""), r.get("file", "")) for r in mh}
    added = 0
    for r in good:
        key = (r["council"], r["date"], r["file"])
        if key in seen:
            continue
        seen.add(key)
        mh.append({"council": r["council"], "file": r["file"], "date": r["date"],
                   "agenda_items": r["agenda_items"], "source_url": r["source_url"]})
        added += 1
    (HERE / "meeting_history.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in mh), encoding="utf-8")
    from collections import Counter
    print(f"\nOCR done. agendas extracted: {len(good)}/{len(jobs)}; added {added} new meetings.")
    print("meeting_history now:", len(mh), "meetings;", len({r['council'] for r in mh}), "councils")
    print("scanned-council meeting counts:", {k: v for k, v in Counter(r['council'] for r in mh).items() if k in SOURCES})


if __name__ == "__main__":
    main()
