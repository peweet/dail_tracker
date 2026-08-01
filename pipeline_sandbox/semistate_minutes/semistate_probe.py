"""Discover + sample BOARD MINUTES of Irish semi-state bodies, seeded from the lobbying register.

EXPERIMENTAL sandbox — the "minutes as enrichment" expansion beyond councils. Candidate
bodies come from data/bronze/lobbying_csv_data/Lobbying_ie_organisation_results.csv
(Name, Website, CompanyRegistrationNumber = the CRO tie-in) matched against a semi-state
name pattern list; bodies absent from the register are added from a static seed so the
big ones (ESB, RTÉ, Coillte…) can't be missed just because they registered under a
variant name.

Per body: fetch homepage + common governance paths (/board, /publications,
/corporate-governance, /freedom-of-information, /about), collect links whose href/text
mentions minutes, download up to MAX_DOCS_PER_BODY PDFs, extract text (fitz; winocr for
scans, bounded), and flag whether the text actually reads like meeting minutes.

Outputs (this dir): candidates.csv · minutes_docs.jsonl · corpus/<body>/*.txt · SEMISTATE.md

Usage: python semistate_probe.py [--limit-bodies N]
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

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
LOBBY_CSV = ROOT / "data" / "bronze" / "lobbying_csv_data" / "Lobbying_ie_organisation_results.csv"
CORPUS = HERE / "corpus"
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}
SLEEP_S = 1.5
MAX_DOCS_PER_BODY = 8
MAX_OCR_PAGES = 30
TEXT_MIN = 80
DPI = 200

# Commercial semi-states + major state agencies likely to publish board papers.
SEMISTATE_PATTERNS = [
    r"\bESB\b|Electricity Supply Board", r"EirGrid", r"Ervia|Gas Networks",
    r"Uisce [ÉE]ireann|Irish Water", r"Bord na M[óo]na", r"Coillte", r"\bRT[ÉE]\b",
    r"An Post\b", r"\bdaa\b|Dublin Airport", r"Shannon (Airport|Group)",
    r"Iarnr[óo]d [ÉE]ireann|Irish Rail", r"Bus [ÉE]ireann", r"Dublin Bus",
    r"C[óo]ras Iompair|\bCI[ÉE]\b", r"Port of Cork", r"Dublin Port",
    r"Shannon Foynes", r"IDA Ireland", r"Enterprise Ireland", r"F[áa]ilte Ireland",
    r"Tourism Ireland", r"Teagasc", r"Bord Bia", r"Bord Iascaigh Mhara",
    r"Horse Racing Ireland", r"National Oil Reserves", r"\bEPA\b|Environmental Protection Agency",
    r"Sustainable Energy Authority|SEAI", r"Land Development Agency",
    r"Housing Finance Agency", r"National Treasury Management|NTMA",
    r"Strategic Banking Corporation", r"VHI|Voluntary Health Insurance",
    r"An Coimisi[úu]n Plean[áa]la|An Bord Plean[áa]la",
]
# Static fallback (body, website) for majors that may not be in the register under these names.
STATIC_SEEDS = [
    ("ESB", "https://esb.ie"), ("EirGrid", "https://www.eirgrid.ie"),
    ("Coillte", "https://www.coillte.ie"), ("Bord na Móna", "https://www.bordnamona.ie"),
    ("RTÉ", "https://www.rte.ie/about"), ("An Post", "https://www.anpost.com"),
    ("daa", "https://www.daa.ie"), ("Uisce Éireann", "https://www.water.ie"),
    ("CIÉ", "https://www.cie.ie"), ("IDA Ireland", "https://www.idaireland.com"),
    ("Enterprise Ireland", "https://www.enterprise-ireland.com"),
    ("Teagasc", "https://www.teagasc.ie"), ("Fáilte Ireland", "https://www.failteireland.ie"),
    ("Land Development Agency", "https://lda.ie"),
    ("NTMA", "https://www.ntma.ie"), ("SEAI", "https://www.seai.ie"),
    ("EPA", "https://www.epa.ie"), ("Dublin Port", "https://www.dublinport.ie"),
]
GOV_PATHS = ["", "/board-minutes", "/publications", "/corporate-governance",
             "/about/corporate-governance", "/freedom-of-information", "/about/board",
             "/about-us/corporate-governance"]
_MINUTE_HINT = re.compile(r"minute", re.I)
_MINUTES_MARK = re.compile(
    r"minutes of (the )?(board|meeting)|board meeting|present:|in attendance|apologies", re.I)


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _winocr_run():
    from PIL import Image
    import winocr

    def run(png: bytes) -> list[str]:
        r = winocr.recognize_pil_sync(Image.open(io.BytesIO(png)))
        return [ln["text"] for ln in r.get("lines", [])]

    return run


_PW = None


class _FakeResp:
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
        import sys

        sys.path.insert(0, str(HERE.parent / "council_minutes"))
        from pw_fetch import PWFetch

        _PW = PWFetch()
    return _PW


def _get(url: str, timeout: int = 45) -> requests.Response | _FakeResp | None:
    try:
        r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r
    except requests.RequestException:
        pass
    if pw := _pw():  # JS-shell / WAF fallback: render HTML, context-request for PDFs
        if url.lower().endswith(".pdf") or ".pdf?" in url.lower():
            b = pw.bytes(url)
            return _FakeResp(b, "application/pdf") if b else None
        h = pw.html(url)
        return _FakeResp(h.encode(), "text/html") if h else None
    return None


def candidates_from_register() -> list[dict]:
    out = []
    if not LOBBY_CSV.exists():
        return out
    rx = re.compile("|".join(SEMISTATE_PATTERNS), re.I)
    with open(LOBBY_CSV, encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            name = (row.get("Name") or "").strip()
            if name and rx.search(name):
                out.append({
                    "body": name,
                    "website": (row.get("Website") or "").strip(),
                    "cro_number": (row.get("CompanyRegistrationNumber") or "").strip(),
                    "source": "lobbying_register",
                })
    return out


def merge_candidates() -> list[dict]:
    cands = candidates_from_register()
    have = {slug(c["body"]) for c in cands}
    for body, site in STATIC_SEEDS:
        if slug(body) not in have and not any(slug(body) in slug(c["body"]) for c in cands):
            cands.append({"body": body, "website": site, "cro_number": "", "source": "static_seed"})
    # normalise website
    for c in cands:
        w = c["website"]
        if w and not w.startswith("http"):
            c["website"] = "https://" + w
    return [c for c in cands if c["website"]]


def _collect_links(html: str, page_url: str, host: str, links: dict[str, str]) -> None:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = urljoin(page_url + "/", a["href"].strip())
        txt = a.get_text(" ", strip=True)[:120]
        if _MINUTE_HINT.search(href) or _MINUTE_HINT.search(txt):
            if urlparse(href).netloc.endswith(host.split(".", 1)[-1]) or href.lower().endswith(".pdf"):
                links.setdefault(href, txt)


def probe_body(cand: dict, ocr, log) -> list[dict]:
    site = cand["website"].rstrip("/")
    host = urlparse(site).netloc
    links: dict[str, str] = {}
    for path in GOV_PATHS:
        r = _get(site + path)
        if not r or "text/html" not in r.headers.get("content-type", ""):
            continue
        _collect_links(r.text, site + path, host, links)
        time.sleep(SLEEP_S)
    if not links and (pw := _pw()):
        # requests saw only a JS shell — re-crawl key paths with a rendered browser
        for path in GOV_PATHS[:5]:
            if h := pw.html(site + path):
                _collect_links(h, site + path, host, links)
            time.sleep(SLEEP_S)
    # one hop: minute-ish HTML pages (e.g. "Board minutes" listing) usually hold the PDFs
    for href in [h for h in list(links) if not (h.lower().endswith(".pdf") or ".pdf?" in h.lower())][:4]:
        r = _get(href)
        if r and "text/html" in r.headers.get("content-type", ""):
            _collect_links(r.text, href, host, links)
        time.sleep(SLEEP_S)
    log(f"  {cand['body']}: {len(links)} minute-ish links")

    docs = []
    # Try EVERY collected link and sniff %PDF magic — semi-state sites serve PDFs from
    # dynamic /download?id= URLs with no .pdf suffix (phase-1/2 zero-doc failure mode).
    for href, txt in list(links.items())[: MAX_DOCS_PER_BODY * 3]:
        if len([d for d in docs if d.get("status") in ("text", "ocr_winocr")]) >= MAX_DOCS_PER_BODY:
            break
        time.sleep(SLEEP_S)
        r = _get(href, timeout=90)
        rec = {"body": cand["body"], "cro_number": cand["cro_number"], "url": href,
               "link_text": txt, "source": cand["source"]}
        if not r:
            rec["status"] = "fetch_fail"
            docs.append(rec)
            continue
        if not r.content.lstrip()[:5].startswith(b"%PDF"):
            continue  # HTML listing page (already followed one hop) or other asset
        try:
            doc = fitz.open(stream=r.content, filetype="pdf")
            rec["n_pages"] = len(doc)
            native = sum(len(p.get_text().strip()) for p in doc)
            if native >= TEXT_MIN * max(1, len(doc)):
                text = "\n".join(p.get_text() for p in doc)
                rec["status"] = "text"
            else:
                n = min(len(doc), MAX_OCR_PAGES)
                text = "\n".join("\n".join(ocr(doc[i].get_pixmap(dpi=DPI).tobytes("png")))
                                 for i in range(n))
                rec["status"] = "ocr_winocr"
        except Exception as e:  # noqa: BLE001
            rec["status"] = f"err_{type(e).__name__}"
            docs.append(rec)
            continue
        rec["text_chars"] = len(text)
        rec["looks_like_minutes"] = bool(_MINUTES_MARK.search(text[:6000]))
        if rec["looks_like_minutes"] and len(text) > 800:
            bdir = CORPUS / slug(cand["body"])
            bdir.mkdir(parents=True, exist_ok=True)
            stem = slug(href.split("/")[-1])[:80]
            (bdir / (stem + ".txt")).write_text(text, encoding="utf-8")
            rec["text_path"] = f"corpus/{slug(cand['body'])}/{stem}.txt"
        docs.append(rec)
        log(f"    {href.split('/')[-1][:55]} ({rec['status']}) minutes={rec.get('looks_like_minutes')}")
    return docs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-bodies", type=int, default=None)
    args = ap.parse_args()
    deadline = float(os.environ.get("NIGHT_DEADLINE_TS", "0")) or None

    def log(msg: str) -> None:
        print(msg, flush=True)

    cands = merge_candidates()
    if args.limit_bodies:
        cands = cands[: args.limit_bodies]
    log(f"semi-state candidates with websites: {len(cands)}")
    with open(HERE / "candidates.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["body", "website", "cro_number", "source"])
        w.writeheader()
        w.writerows(cands)

    ocr = _winocr_run()
    all_docs = []
    for c in cands:
        if deadline and time.time() > deadline:
            log("DEADLINE reached — stopping probe cleanly")
            break
        try:
            all_docs += probe_body(c, ocr, log)
        except Exception as e:  # noqa: BLE001
            log(f"  {c['body']}: BODY ERR {type(e).__name__}")
        (HERE / "minutes_docs.jsonl").write_text(
            "\n".join(json.dumps(d, ensure_ascii=False) for d in all_docs), encoding="utf-8")

    real = [d for d in all_docs if d.get("looks_like_minutes")]
    bodies_with = sorted({d["body"] for d in real})
    L = ["# Semi-state board minutes — discovery probe\n"]
    L.append(f"Auto-generated by semistate_probe.py. {len(cands)} candidate bodies probed "
             f"(lobbying register + static seeds), **{len(all_docs)} PDFs sampled**, "
             f"**{len(real)} read as real minutes** across **{len(bodies_with)} bodies**.\n")
    L.append("## Bodies with harvestable minutes\n\n| body | minutes docs | CRO no. |\n|---|---|---|")
    from collections import Counter
    cnt = Counter(d["body"] for d in real)
    cro = {d["body"]: d.get("cro_number", "") for d in real}
    for b, n in cnt.most_common():
        L.append(f"| {b} | {n} | {cro.get(b, '')} |")
    L.append("\n## All candidates probed\n\n| body | source | website |\n|---|---|---|")
    for c in cands:
        L.append(f"| {c['body']} | {c['source']} | {c['website']} |")
    (HERE / "SEMISTATE.md").write_text("\n".join(L), encoding="utf-8")
    log(f"\nPROBE DONE docs={len(all_docs)} real_minutes={len(real)} bodies={len(bodies_with)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
