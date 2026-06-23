"""Consolidate + full-text-extract + classify + quarantine + quantify council minutes.

Merges v1 (meetings.jsonl) and v2 (meetings_v2.jsonl) doc lists (dedup by URL), then for every
unique doc:
  - re-fetch and extract the FULL text (fitz all pages for born-digital PDF; OCR for scanned,
    BOUNDED + guarded; BeautifulSoup for HTML);
  - save clean full text to corpus/<council>/<file>.txt;
  - label a doc_type (plenary_minutes / md_minutes / agenda / standing_orders / report / other);
  - decide CLEAN vs QUARANTINE with a reason code;
  - parse structure + attribute per-member roll-call votes.

Outputs:
  meetings_clean.jsonl         clean docs (metadata + text_path + structure)
  quarantine/quarantine.jsonl  rejected docs with reason (for later review)
  member_votes_all.jsonl       consolidated per-member votes (clean docs only)
  corpus/<council>/*.txt       extracted full text
  QUALITY_ASSESSMENT.md        quantified type/quality tables (auto-generated)

Bounded for unattended run: MAX_DOCS, MAX_OCR_DOCS, MAX_OCR_PAGES.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}
CORPUS = HERE / "corpus"
QDIR = HERE / "quarantine"
CLEAN_MIN_CHARS = 1200
MAX_DOCS = 320
MAX_OCR_DOCS = 30
MAX_OCR_PAGES = 25
TEXT_MIN = 80

MARKS = {"√", "✓", "✔", "x", "X", "✗", "•", "Y"}
VOTE_COLS = ("for", "against", "abstain", "absent")
_MOTION = re.compile(r"[Pp]ropos(?:ed|al)\s+(?:by|of)[^\n]{0,120}?(?:[Ss]econd)[^\n]{0,70}")
_DEC = re.compile(r"\b(AGREED|N[O0]TED|CARRIED|LOST|ADOPTED|DEFERRED|APPROVED|RESOLVED|DEFEATED)\b")
_ITEM = re.compile(r"ITEM\s*N[O0]\.?\s*\d+", re.I)
_ROLL = re.compile(r"roll[\s-]?call vote", re.I)
_RESULT = re.compile(r"Result[:\s]+(\d+)\s*For[,\s]+(?:(\d+)\s*Against)?[,\s]*(?:(\d+)\s*Abstain)?", re.I)
_MINMARK = re.compile(r"minutes of|confirmation of (?:the )?minutes|i l[áa]thair|members present|"
                      r"in attendance|proposed by", re.I)
_MOT_CTX = re.compile(r"(Resolution|Motion|Proposed by|That the|We the Members)[^\n]{0,200}", re.I)
YEAR_RX = re.compile(r"20(1\d|2\d)")

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


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def doc_type(url: str, text: str) -> str:
    u = url.lower()
    t = text[:3000].lower()
    is_md = "municipal district" in t or re.search(r"\bmd\b|municipal", u)
    if re.search(r"standing.?order", u + t):
        return "standing_orders"
    if re.search(r"agenda", u) and not re.search(r"minute", u) and "minutes of" not in t:
        return "agenda"
    if re.search(r"management report|chief executive.?s? (monthly )?report|annual report|"
                 r"financial statement|local economic", u + t[:1500]):
        return "report_or_plan"
    if _MINMARK.search(text) or "minute" in u:
        return "md_minutes" if is_md else "plenary_minutes"
    return "other"


def classify(rec: dict, text: str, dtype: str) -> tuple[bool, str]:
    st = rec.get("status")
    if st in ("fetch_fail", "open_fail") or (st or "").startswith("err_"):
        return False, f"extract_{st}"
    if st == "staged_offbox_scanned":
        return False, "scanned_not_ocr"
    if len(text) < CLEAN_MIN_CHARS:
        return False, "low_text"
    if dtype in ("agenda", "standing_orders", "report_or_plan"):
        return False, f"not_minutes_{dtype}"
    if dtype == "other":
        return False, "unrecognised_doctype"
    return True, "clean"


def parse_struct(t: str) -> dict:
    res = [{"for": int(m.group(1)), "against": int(m.group(2) or 0), "abstain": int(m.group(3) or 0)}
           for m in _RESULT.finditer(t)]
    return {"motions": len(_MOTION.findall(t)), "decisions": len(_DEC.findall(t)),
            "agenda_items": len(_ITEM.findall(t)), "rollcall_votes": len(_ROLL.findall(t)),
            "named_vote_results": res}


def hdr_map(row):
    low = [(c or "").strip().lower() for c in row]
    m = {i: v for i, c in enumerate(low) for v in VOTE_COLS if c == v or c.startswith(v)}
    return m if {"for", "against"} <= set(m.values()) else None


def votes_pdf(doc, la, fname):
    out, last = [], ""
    for page in doc:
        hits = list(_MOT_CTX.finditer(page.get_text()))
        if hits:
            last = re.sub(r"\s+", " ", hits[-1].group(0))[:200]
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
                v = next((hm[i] for i in hm if i < len(r) and
                          ((r[i] or "").strip() in MARKS or "√" in (r[i] or ""))), None)
                if v:
                    out.append({"local_authority": la, "meeting": fname, "motion": last,
                                "member": nm, "vote": v})
    return out


def norm_members(votes):
    import difflib
    by = defaultdict(Counter)
    for v in votes:
        by[v["local_authority"]][v["member"]] += 1
    rosters = {la: [n for n, c in cnt.items() if c >= 3 and len(n) > 6 and " " in n]
               for la, cnt in by.items()}
    out = []
    for v in votes:
        roster = rosters.get(v["local_authority"], [])
        if v["member"] in roster:
            out.append(v)
        elif (m := difflib.get_close_matches(v["member"], roster, n=1, cutoff=0.6)):
            out.append({**v, "member": m[0]})
    return out


def main():
    CORPUS.mkdir(exist_ok=True)
    QDIR.mkdir(exist_ok=True)
    # gather unique docs from v1 + v2
    docs = {}
    for fn in ("meetings.jsonl", "meetings_v2.jsonl"):
        p = HERE / fn
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            u = r.get("url")
            if u and u not in docs:
                docs[u] = r
    items = list(docs.items())[:MAX_DOCS]
    print(f"unique docs to consolidate: {len(items)}")

    import fitz
    clean, quarantined, votes = [], [], []
    ocr_used = 0
    for i, (url, prev) in enumerate(items):
        la = prev.get("local_authority", "?")
        fname = url.split("/")[-1][:80]
        kind = "html" if url.lower().endswith(".html") else "pdf"
        rec = {"url": url, "local_authority": la, "meeting": fname}
        text, dvotes = "", []
        try:
            r = requests.get(url, headers=HDRS, timeout=70)
            if r.status_code != 200:
                rec["status"] = "fetch_fail"
            elif kind == "html":
                soup = BeautifulSoup(r.text, "html.parser")
                for x in soup(["script", "style", "nav", "header", "footer"]):
                    x.decompose()
                text = soup.get_text("\n", strip=True)
                rec["status"] = "html"
            else:
                doc = fitz.open(stream=r.content, filetype="pdf")
                native = sum(len(p.get_text().strip()) for p in doc)
                rec["n_pages"] = len(doc)
                if native >= TEXT_MIN * max(1, len(doc)):
                    text = "\n".join(p.get_text() for p in doc)
                    rec["status"] = "text"
                    dvotes = votes_pdf(doc, la, fname)
                elif ocr_used < MAX_OCR_DOCS and (ocr := get_ocr()):
                    text = "\n".join(l for p in list(doc)[:MAX_OCR_PAGES]
                                     for l in ocr(p.get_pixmap(dpi=200).tobytes("png")))
                    rec["status"] = "ocr"
                    ocr_used += 1
                else:
                    rec["status"] = "staged_offbox_scanned"
        except Exception as e:  # noqa: BLE001
            rec["status"] = f"err_{type(e).__name__}"

        dtype = doc_type(url, text)
        rec["doc_type"] = dtype
        rec["text_chars"] = len(text)
        rec.update(parse_struct(text))
        ok, reason = classify(rec, text, dtype)
        rec["clean"] = ok
        rec["reason"] = reason
        if ok:
            cdir = CORPUS / slug(la)
            cdir.mkdir(exist_ok=True)
            (cdir / (slug(fname)[:80] + ".txt")).write_text(text, encoding="utf-8")
            rec["text_path"] = f"corpus/{slug(la)}/{slug(fname)[:80]}.txt"
            clean.append(rec)
            votes += dvotes
        else:
            quarantined.append(rec)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(items)} clean={len(clean)} quar={len(quarantined)} ocr={ocr_used}")

    votes = norm_members(votes)
    (HERE / "meetings_clean.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in clean), encoding="utf-8")
    (QDIR / "quarantine.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in quarantined), encoding="utf-8")
    (HERE / "member_votes_all.jsonl").write_text(
        "\n".join(json.dumps(v, ensure_ascii=False) for v in votes), encoding="utf-8")
    write_quality_report(clean, quarantined, votes)
    print(f"\nDONE clean={len(clean)} quarantined={len(quarantined)} votes={len(votes)} ocr_docs={ocr_used}")


def write_quality_report(clean, quar, votes):
    allrecs = clean + quar
    by_status = Counter(r["status"] for r in allrecs)
    by_type = Counter(r["doc_type"] for r in allrecs)
    by_reason = Counter(r["reason"] for r in quar)
    per_council = defaultdict(lambda: {"clean": 0, "quar": 0, "votes": 0})
    for r in clean:
        per_council[r["local_authority"]]["clean"] += 1
    for r in quar:
        per_council[r["local_authority"]]["quar"] += 1
    vc = Counter(v["local_authority"] for v in votes)
    for la, n in vc.items():
        per_council[la]["votes"] = n
    total = len(allrecs)
    pct = (100 * len(clean) / total) if total else 0

    L = []
    L.append("# Council minutes — extraction quality assessment\n")
    L.append(f"Auto-generated. Consolidated v1+v2. **{total} unique docs**, "
             f"**{len(clean)} clean ({pct:.0f}%)**, **{len(quar)} quarantined**, "
             f"**{len(votes)} attributed member-votes**.\n")
    L.append("## By extraction status")
    L.append("| status | docs |\n|---|---|")
    for s, n in by_status.most_common():
        L.append(f"| {s} | {n} |")
    L.append("\n## By document type")
    L.append("| doc_type | docs |\n|---|---|")
    for t, n in by_type.most_common():
        L.append(f"| {t} | {n} |")
    L.append("\n## Quarantine reasons (for later review)")
    L.append("| reason | docs |\n|---|---|")
    for r, n in by_reason.most_common():
        L.append(f"| {r} | {n} |")
    L.append("\n## Per-council coverage")
    L.append("| council | clean | quarantined | member_votes |\n|---|---|---|---|")
    for la in sorted(per_council):
        d = per_council[la]
        L.append(f"| {la} | {d['clean']} | {d['quar']} | {d['votes']} |")
    L.append("\n## Vote coverage")
    L.append(f"- councils with attributed votes: {sorted(vc)}")
    L.append(f"- total member-vote rows: {len(votes)}")
    by_vote = Counter(v["vote"] for v in votes)
    L.append(f"- by vote: {dict(by_vote)}")
    (HERE / "QUALITY_ASSESSMENT.md").write_text("\n".join(L), encoding="utf-8")


if __name__ == "__main__":
    main()
