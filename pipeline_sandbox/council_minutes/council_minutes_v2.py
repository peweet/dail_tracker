"""Seed-driven, recency-bounded council-minutes extractor (all 31 LAs).

Reads council_seeds.csv (per-council meetings page). For each council:
  - crawl the seed + 1 level of year/sub-meeting links;
  - collect minutes docs (PDF and HTML), keep only RECENT (year >= MIN_YEAR);
  - extract text: fitz for born-digital PDF, OCR for scanned PDF (guarded — degrades to
    'staged_offbox' if OCR unavailable), BeautifulSoup for HTML minutes pages;
  - parse structure (agenda items / motions / decisions / vote markers);
  - attribute per-member roll-call votes (PDF tables via fitz.find_tables; HTML <table>s).

Writes meetings_v2.jsonl, council_classification_v2.csv, member_votes_v2.jsonl.
Bounded for unattended run: MAX_DOCS_PER_COUNCIL, MAX_OCR_DOCS, MIN_YEAR.
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
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}
MIN_YEAR = 2024
MAX_DOCS_PER_COUNCIL = 20
MAX_OCR_DOCS = 6
MAX_OCR_PAGES = 12
TEXT_MIN = 80
YEAR_RX = re.compile(r"20(2[4-9]|3\d)")
MARKS = {"√", "✓", "✔", "x", "X", "✗", "•", "Y"}
VOTE_COLS = ("for", "against", "abstain", "absent")

_ITEM = re.compile(r"ITEM\s*N[O0]\.?\s*\d+[^\n]{0,90}", re.I)
_MOTION = re.compile(r"[Pp]ropos(?:ed|al)\s+(?:by|of)[^\n]{0,120}?(?:[Ss]econd)[^\n]{0,70}")
_DEC = re.compile(r"\b(AGREED|N[O0]TED|CARRIED|LOST|ADOPTED|DEFERRED|APPROVED|RESOLVED|DEFEATED)\b")
_ROLL = re.compile(r"roll[\s-]?call vote", re.I)
_RESULT = re.compile(r"Result[:\s]+(\d+)\s*For[,\s]+(?:(\d+)\s*Against)?[,\s]*(?:(\d+)\s*Abstain)?",
                     re.I)
_VOTEMARK = re.compile(r"(in favou?r|\bagainst\b|abstain|division|voted for|show of hands)", re.I)
_MOT_CTX = re.compile(r"(Resolution|Motion|Proposed by|That the|We the Members)[^\n]{0,200}", re.I)

_OCR = None
_OCR_TRIED = False


def get_ocr():
    global _OCR, _OCR_TRIED  # noqa: PLW0603
    if _OCR_TRIED:
        return _OCR
    _OCR_TRIED = True
    try:
        from rapidocr_onnxruntime import RapidOCR
        r = RapidOCR()
        _OCR = lambda png: [t for _, t, _ in (r(png)[0] or [])]  # noqa: E731
    except Exception:  # noqa: BLE001
        _OCR = None
    return _OCR


def get(url, timeout=30):
    try:
        r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:  # noqa: BLE001
        return None


def recent(url: str) -> bool:
    yrs = YEAR_RX.findall(url)
    return bool(yrs) or not re.search(r"20\d\d", url)  # recent, or undated (keep)


def collect_docs(seed: str) -> list[tuple[str, str]]:
    """Return [(url, kind)] kind in {pdf, html}. Crawl seed + 1 level of year/sub links."""
    docs, seen = [], set()
    pages = [seed]
    root = get(seed)
    if root:
        soup = BeautifulSoup(root.text, "html.parser")
        for a in soup.find_all("a", href=True):
            u = urljoin(root.url, a["href"])
            txt = a.get_text(" ", strip=True).lower()
            if re.search(r"minute|meeting|agenda|20(2[4-9])", txt + " " + u, re.I) \
                    and not u.lower().endswith(".pdf") and u not in pages and len(pages) < 12:
                if YEAR_RX.search(txt + u) or "minute" in (txt + u).lower():
                    pages.append(u)
    for page in pages[:12]:
        r = get(page)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            u = urljoin(r.url, a["href"])
            txt = a.get_text(" ", strip=True).lower()
            low = (txt + " " + u).lower()
            if u in seen:
                continue
            if u.lower().endswith(".pdf") and re.search(r"minute|mion", low) and recent(u):
                seen.add(u)
                docs.append((u, "pdf"))
            elif "minutes-of" in u.lower() and u.lower().endswith(".html") and recent(u):
                seen.add(u)
                docs.append((u, "html"))
        if len(docs) >= MAX_DOCS_PER_COUNCIL:
            break
    return docs[:MAX_DOCS_PER_COUNCIL]


def motion_ctx(text: str) -> str:
    hits = list(_MOT_CTX.finditer(text or ""))
    return re.sub(r"\s+", " ", hits[-1].group(0)).strip()[:220] if hits else ""


def parse_struct(t: str) -> dict:
    res = [{"for": int(m.group(1)), "against": int(m.group(2) or 0), "abstain": int(m.group(3) or 0)}
           for m in _RESULT.finditer(t)]
    return {"agenda_items": len(_ITEM.findall(t)), "motions": len(_MOTION.findall(t)),
            "decisions": len(_DEC.findall(t)), "rollcall_votes": len(_ROLL.findall(t)),
            "named_vote_results": res, "vote_markers": len(_VOTEMARK.findall(t))}


def hdr_map(row):
    low = [(c or "").strip().lower() for c in row]
    m = {i: v for i, c in enumerate(low) for v in VOTE_COLS if c == v or c.startswith(v)}
    return m if {"for", "against"} <= set(m.values()) else None


def votes_from_pdf(doc, la, fname):
    out, last = [], ""
    for page in doc:
        last = motion_ctx(page.get_text()) or last
        try:
            tbls = page.find_tables()
        except Exception:  # noqa: BLE001
            continue
        for tbl in tbls.tables:
            rows = tbl.extract()
            if not rows or not (hm := hdr_map(rows[0])):
                continue
            ncol = min(set(range(len(rows[0]))) - set(hm)) if set(hm) else 0
            for r in rows[1:]:
                nm = (r[ncol] or "").replace("\n", " ").strip()
                if not nm or nm.lower().startswith(("member", "total", "result")):
                    continue
                v = next((hm[i] for i in hm if i < len(r) and (r[i] or "").strip() in MARKS
                          or (i < len(r) and "√" in (r[i] or ""))), None)
                if v:
                    out.append({"local_authority": la, "meeting": fname, "motion": last[:200],
                                "member": nm, "vote": v})
    return out


def votes_from_html(html, la, fname):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tbl in soup.find_all("table"):
        head = [th.get_text(" ", strip=True).lower() for th in tbl.find_all(["th", "td"])[:6]]
        hm = {i: v for i, c in enumerate(head) for v in VOTE_COLS if c == v or c.startswith(v)}
        if not ({"for", "against"} <= set(hm.values())):
            continue
        for tr in tbl.find_all("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 2 or not cells[0] or cells[0].lower().startswith(("member", "total")):
                continue
            v = next((hm[i] for i in hm if i < len(cells) and cells[i].strip() in MARKS), None)
            if v:
                out.append({"local_authority": la, "meeting": fname, "motion": "",
                            "member": cells[0], "vote": v})
    return out


def extract_doc(url, kind, la, ocr_budget):
    import fitz  # noqa: PLC0415
    r = get(url, timeout=70)
    if not r:
        return {"url": url, "status": "fetch_fail", "local_authority": la}, ocr_budget, []
    fname = url.split("/")[-1][:70]
    if kind == "html":
        soup = BeautifulSoup(r.text, "html.parser")
        for x in soup(["script", "style", "nav", "header", "footer"]):
            x.decompose()
        text = soup.get_text("\n", strip=True)
        votes = votes_from_html(r.text, la, fname)
        return ({"url": url, "status": "html", "local_authority": la, "text_chars": len(text),
                 **parse_struct(text)}, ocr_budget, votes)
    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception:  # noqa: BLE001
        return {"url": url, "status": "open_fail", "local_authority": la}, ocr_budget, []
    native = sum(len(p.get_text().strip()) for p in doc)
    scanned = native < TEXT_MIN * max(1, len(doc))
    votes = []
    if not scanned:
        text = "\n".join(p.get_text() for p in doc)
        status = "text"
        votes = votes_from_pdf(doc, la, fname)
    elif ocr_budget > 0 and (ocr := get_ocr()):
        text = "\n".join(l for p in list(doc)[:MAX_OCR_PAGES] for l in ocr(p.get_pixmap(dpi=200).tobytes("png")))
        status = "ocr"
        ocr_budget -= 1
    else:
        return ({"url": url, "status": "staged_offbox_scanned", "local_authority": la,
                 "n_pages": len(doc)}, ocr_budget, [])
    return ({"url": url, "status": status, "local_authority": la, "n_pages": len(doc),
             "text_chars": len(text), **parse_struct(text)}, ocr_budget, votes)


def classify(rows, votes):
    done = [r for r in rows if r.get("status") in ("text", "ocr", "html")]
    fmts = {r["status"] for r in done}
    fmt = ("html" if fmts == {"html"} else "scanned" if "ocr" in fmts and "text" not in fmts
           else "mixed" if len(fmts) > 1 else "born_digital" if "text" in fmts
           else "scanned_staged" if any(r.get("status") == "staged_offbox_scanned" for r in rows)
           else "none")
    named = sum(len(r.get("named_vote_results", [])) for r in done) + len(votes)
    style = ("named_rollcall" if named or any(r.get("rollcall_votes") for r in done)
             else "proposer_seconder" if sum(r.get("motions", 0) for r in done)
             else "unknown")
    return {"format": fmt, "vote_style": style, "docs_extracted": len(done),
            "member_vote_rows": len(votes),
            "named_results": sum(len(r.get("named_vote_results", [])) for r in done)}


def main():
    seeds = list(csv.DictReader(open(HERE / "council_seeds.csv", encoding="utf-8")))
    mj = open(HERE / "meetings_v2.jsonl", "w", encoding="utf-8")
    vj = open(HERE / "member_votes_v2.jsonl", "w", encoding="utf-8")
    cc = csv.DictWriter(open(HERE / "council_classification_v2.csv", "w", newline="", encoding="utf-8"),
                        fieldnames=["local_authority", "seed_url", "n_docs", "format", "vote_style",
                                    "docs_extracted", "member_vote_rows", "named_results"])
    cc.writeheader()
    for i, s in enumerate(seeds):
        la, seed = s["local_authority"], s["seed_url"]
        try:
            docs = collect_docs(seed)
        except Exception:  # noqa: BLE001
            docs = []
        rows, allvotes, budget = [], [], MAX_OCR_DOCS
        for url, kind in docs:
            try:
                rec, budget, votes = extract_doc(url, kind, la, budget)
            except Exception as e:  # noqa: BLE001
                rec, votes = {"url": url, "status": f"err_{type(e).__name__}", "local_authority": la}, []
            rows.append(rec)
            allvotes += votes
            mj.write(json.dumps(rec, ensure_ascii=False) + "\n")
        for v in allvotes:
            vj.write(json.dumps(v, ensure_ascii=False) + "\n")
        mj.flush(); vj.flush()
        summ = classify(rows, allvotes)
        cc.writerow({"local_authority": la, "seed_url": seed, "n_docs": len(docs), **summ})
        print(f"[{i+1}/{len(seeds)}] {la:24} docs={len(docs):2} fmt={summ['format']:14} "
              f"style={summ['vote_style']:17} votes={summ['member_vote_rows']}")
    mj.close(); vj.close()
    print("\nwrote meetings_v2.jsonl + member_votes_v2.jsonl + council_classification_v2.csv")


if __name__ == "__main__":
    sys.exit(main())
