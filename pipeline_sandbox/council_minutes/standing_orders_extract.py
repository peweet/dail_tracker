"""Standing Orders extractor — how each council formulates its agenda + takes votes.

Harvests each council's Standing Orders PDF (born-digital → fitz, NO OCR) from its meetings page,
and parses VERBATIM governance clauses: Order of Business (the agenda template), Notice of Motion
(how councillors table items), Voting/Divisions (whether a recorded/named roll-call vote is taken —
the structural reason named voting records exist for some councils and not others), Quorum.

Output: standing_orders.jsonl — one row per council. Verbatim excerpts (display per the project's
no-inference rule). Sandbox only.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
SO_RX = re.compile(r"standing.?order", re.I)


def get(u, t=30):
    try:
        r = requests.get(u, headers=H, timeout=t, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:  # noqa: BLE001
        return None


def find_so_pdf(seed: str) -> str | None:
    """From a council's meetings page, find its Standing Orders PDF (crawl 1 level)."""
    r = get(seed)
    if not r:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    # direct SO pdf
    for a in soup.find_all("a", href=True):
        u = urljoin(r.url, a["href"])
        if u.lower().endswith(".pdf") and SO_RX.search(u + " " + a.get_text(" ", strip=True)):
            return u
    # SO sub-page → its pdf
    for a in soup.find_all("a", href=True):
        if SO_RX.search(a.get_text(" ", strip=True) + " " + a["href"]) and ".pdf" not in a["href"].lower():
            r2 = get(urljoin(r.url, a["href"]))
            if r2:
                s2 = BeautifulSoup(r2.text, "html.parser")
                for a2 in s2.find_all("a", href=True):
                    u2 = urljoin(r2.url, a2["href"])
                    if u2.lower().endswith(".pdf") and SO_RX.search(u2 + " " + a2.get_text(" ", strip=True)):
                        return u2
    return None


def clause(text: str, *keywords: str, window: int = 320) -> str:
    """Return the first verbatim clause around any keyword (cleaned)."""
    for kw in keywords:
        m = re.search(r"[^.]{0,40}\b" + kw + r"\b[^.]{0,%d}\." % window, text, re.I)
        if m:
            return re.sub(r"\s+", " ", m.group(0)).strip()[:window]
    return ""


def order_of_business(text: str) -> list[str]:
    m = re.search(r"Order of Business(.{20,1200})", text, re.I | re.S)
    if not m:
        return []
    blk = m.group(1)
    items = re.findall(r"(?:^|\n)\s*(?:\([a-z]\)|[a-z]\)|\d+\.|\([ivx]+\))\s*([A-Z][^\n]{4,80})", blk)
    out, seen = [], set()
    for it in items:
        s = re.sub(r"\s+", " ", it).strip(" .;")
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out[:10]


def parse_so(la: str, url: str) -> dict:
    import fitz  # noqa: PLC0415
    r = get(url, 50)
    if not r:
        return {"local_authority": la, "source_url": url, "status": "fetch_fail"}
    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception:  # noqa: BLE001
        return {"local_authority": la, "source_url": url, "status": "open_fail"}
    text = "\n".join(p.get_text() for p in doc)
    if len(text) < 800:
        return {"local_authority": la, "source_url": url, "status": "scanned_or_thin", "chars": len(text)}
    voting = clause(text, "roll call", "recorded vote", "show of hands", "by a division", "Divisions")
    records_named = bool(re.search(r"roll[\s-]?call|recorded vote|names?.{0,30}recorded|voting.{0,20}by name", text, re.I))
    return {
        "local_authority": la, "source_url": url, "status": "ok", "n_pages": len(doc),
        "order_of_business": order_of_business(text),
        "notice_of_motion": clause(text, "Notice of Motion", "notices of motion", window=360),
        "voting": voting,
        "quorum": clause(text, "quorum", window=240),
        "records_named_votes": records_named,
    }


# Curated SO PDF URLs (web-found 2026-06-26) — used in preference to the meetings-page crawl.
CURATED = {
    "Cork County": "https://www.corkcoco.ie/sites/default/files/2022-02/standing-orders-cork-county-council-july-2016-pdf.pdf",
    "Dublin City": "https://dublin.moderngov.co.uk/mgConvert2PDF.aspx?ID=48879",
    "Kildare": "https://kildarecoco.ie/YourCouncil/YourElectedCouncil/FullCouncil/StandingOrders/Standing%20Orders%20Final%20290620%20Signed.pdf",
    "Meath": "https://www.meath.ie/system/files/media/file-uploads/2021-08/Standing%20Orders(Adopted%20Nov2,%202020).pdf",
    "Limerick": "https://www.limerick.ie/sites/default/files/media/documents/2024-07/04-b-standing-orders-limerick-city-and-county-council.pdf",
    "Galway County": "https://www.galway.ie/sites/default/files/2026-06/Adopted%20Standing%20Orders%20for%20Meetings%20of%20Plenary%20Council%20%2023.03.2026.pdf",
}


def main():
    councils = list(csv.DictReader(open(HERE / "council_seeds.csv", encoding="utf-8")))
    rows = []
    for c in councils:
        la, seed = c["local_authority"], c["seed_url"]
        url = CURATED.get(la)
        if not url:
            try:
                url = find_so_pdf(seed)
            except Exception:  # noqa: BLE001
                url = None
        if not url:
            rows.append({"local_authority": la, "status": "not_found"})
            print(f"{la:24} -- not found")
            continue
        rec = parse_so(la, url)
        rows.append(rec)
        print(f"{la:24} {rec['status']:14} OoB={len(rec.get('order_of_business',[]))} "
              f"NoM={'Y' if rec.get('notice_of_motion') else '-'} vote={'Y' if rec.get('voting') else '-'} "
              f"named_votes={rec.get('records_named_votes','')}")
    (HERE / "standing_orders.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    ok = [r for r in rows if r.get("status") == "ok"]
    print(f"\nstanding_orders.jsonl: {len(ok)}/{len(rows)} councils parsed; "
          f"{sum(r.get('records_named_votes') for r in ok)} have recorded/named-vote standing orders")


if __name__ == "__main__":
    main()
