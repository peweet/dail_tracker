"""National councillor roster from Wikipedia (sandbox spine for the Your Councillors prototype).

For each of the 31 LAs, fetch the council's Wikipedia article, find the "Councillors by electoral area"
section, and parse its table(s) into rows: (local_authority, lea, name, party, status). Handles rowspan
(the LEA cell spans its group). Best-effort: councils that don't parse are recorded with 0 rows
(honest), not faked. Output councillors_roster.csv + coverage stats. NOT gold.
"""
from __future__ import annotations

import csv
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
API = "https://en.wikipedia.org/w/api.php"
H = {"User-Agent": "dail-tracker-scoping/1.0 (research; councillor roster; contact p.glynn18@gmail.com)"}


def api_json(params, tries=4):
    """GET the MediaWiki API as JSON with backoff (Wikipedia throttles rapid bursts -> non-JSON)."""
    for a in range(tries):
        try:
            r = requests.get(API, params=params, headers=H, timeout=40)
            return r.json()
        except Exception:  # noqa: BLE001
            time.sleep(2 * (a + 1))
    return {}

# Wikipedia article title per LA (en-dash where Wikipedia uses it).
ARTICLES = {
    "Carlow": "Carlow County Council", "Cavan": "Cavan County Council",
    "Clare": "Clare County Council", "Cork City": "Cork City Council",
    "Cork County": "Cork County Council", "Donegal": "Donegal County Council",
    "Dublin City": "Dublin City Council", "Dún Laoghaire-Rathdown": "Dún Laoghaire–Rathdown County Council",
    "Fingal": "Fingal County Council", "Galway City": "Galway City Council",
    "Galway County": "Galway County Council", "Kerry": "Kerry County Council",
    "Kildare": "Kildare County Council", "Kilkenny": "Kilkenny County Council",
    "Laois": "Laois County Council", "Leitrim": "Leitrim County Council",
    "Limerick": "Limerick City and County Council", "Longford": "Longford County Council",
    "Louth": "Louth County Council", "Mayo": "Mayo County Council",
    "Meath": "Meath County Council", "Monaghan": "Monaghan County Council",
    "Offaly": "Offaly County Council", "Roscommon": "Roscommon County Council",
    "Sligo": "Sligo County Council", "South Dublin": "South Dublin County Council",
    "Tipperary": "Tipperary County Council", "Waterford": "Waterford City and County Council",
    "Westmeath": "Westmeath County Council", "Wexford": "Wexford County Council",
    "Wicklow": "Wicklow County Council",
}

PARTY_HINTS = ("fianna", "fine gael", "sinn", "labour", "social democrat", "green",
               "people before profit", "solidarity", "independent", "aontú", "ind ")


def section_index(title: str) -> str | None:
    secs = api_json({"action": "parse", "page": title, "prop": "sections",
                     "format": "json", "redirects": 1}).get("parse", {}).get("sections", [])
    # prefer "Councillors by electoral area", else a "Councillors" section
    for want in ("councillors by electoral area", "members by", "councillors by local"):
        for s in secs:
            if want in s["line"].lower():
                return s["index"]
    for s in secs:
        if s["line"].lower().strip() in ("councillors", "members", "elected members"):
            return s["index"]
    return None


def expand_table(tbl) -> list[list[str]]:
    """Return a matrix of cell texts with rowspan/colspan expanded (so LEA cells fill down)."""
    rows = tbl.find_all("tr")
    grid: list[list[str]] = []
    pending: dict[int, tuple[str, int]] = {}  # col -> (text, remaining_rows)
    for tr in rows:
        cells = tr.find_all(["td", "th"])
        out, ci = [], 0
        # place carried-down rowspans first
        def fill_pending():
            nonlocal ci
            while ci in pending:
                text, rem = pending[ci]
                out.append(text)
                if rem - 1 <= 0:
                    del pending[ci]
                else:
                    pending[ci] = (text, rem - 1)
                ci += 1
        for c in cells:
            fill_pending()
            txt = c.get_text(" ", strip=True)
            cs = int(c.get("colspan", 1) or 1)
            rs = int(c.get("rowspan", 1) or 1)
            for k in range(cs):
                out.append(txt)
                if rs > 1:
                    pending[ci] = (txt, rs - 1)
                ci += 1
        fill_pending()
        if any(x.strip() for x in out):
            grid.append(out)
    return grid


def is_party(s: str) -> bool:
    return any(h in s.lower() for h in PARTY_HINTS)


def parse_council(la: str, title: str) -> list[dict]:
    idx = section_index(title)
    if not idx:
        return []
    j = api_json({"action": "parse", "page": title, "prop": "text", "section": idx, "format": "json"})
    html = j.get("parse", {}).get("text", {}).get("*")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tbl in soup.find_all("table"):
        grid = expand_table(tbl)
        if not grid:
            continue
        header = [c.lower() for c in grid[0]]
        # identify columns
        name_col = next((i for i, h in enumerate(header) if h in ("name", "councillor", "member")), None)
        party_col = next((i for i, h in enumerate(header) if "party" in h), None)
        lea_col = next((i for i, h in enumerate(header) if "electoral" in h or h in ("lea", "area", "ward")), 0)
        body = grid[1:] if name_col is not None else grid
        for row in body:
            if name_col is not None and party_col is not None:
                nm = row[name_col].strip() if name_col < len(row) else ""
                pty = row[party_col].strip() if party_col < len(row) else ""
                lea = row[lea_col].strip() if lea_col < len(row) else ""
            else:
                # heuristic: find a party-looking cell + a name-looking cell
                pty = next((c for c in row if is_party(c)), "")
                nm = next((c for c in row if " " in c and not is_party(c)
                           and not re.search(r"electoral|area|party|member|total", c, re.I)
                           and len(c) < 40 and re.match(r"[A-ZÁÉÍÓÚ]", c)), "")
                lea = row[0].strip()
            nm = re.sub(r"\[[a-z]\]", "", nm).strip()  # drop footnote markers
            if nm and " " in nm and not is_party(nm) and len(nm) < 40:
                out.append({"local_authority": la, "lea": lea, "name": nm,
                            "party": pty, "status": "sitting", "source": title})
    # dedupe by (name) keeping first
    seen, uniq = set(), []
    for r in out:
        if r["name"] in seen:
            continue
        seen.add(r["name"])
        uniq.append(r)
    return uniq


def main():
    rows = []
    for la, title in ARTICLES.items():
        try:
            recs = parse_council(la, title)
        except Exception as e:  # noqa: BLE001
            recs = []
            print(f"{la:24} ERR {type(e).__name__}")
        rows += recs
        print(f"{la:24} {len(recs):3} councillors  ({len({r['lea'] for r in recs})} LEAs)")
        time.sleep(0.8)  # be polite to the Wikipedia API
    with open(HERE / "councillors_roster.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["local_authority", "lea", "name", "party", "status", "source"])
        w.writeheader()
        w.writerows(rows)
    councils_with = len({r["local_authority"] for r in rows})
    print(f"\nTOTAL {len(rows)} councillors across {councils_with}/31 councils -> councillors_roster.csv")


if __name__ == "__main__":
    main()
