"""Roster v2 — content-based column detection (rowspan-aware). Fixes LEA-as-name bug.
Rebuilds councillors_roster.csv for all 31 councils. Sandbox only."""
from __future__ import annotations
import csv, re, time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
API = "https://en.wikipedia.org/w/api.php"
H = {"User-Agent": "dail-tracker/1.0 (research; p.glynn18@gmail.com)"}
PARTIES = ["fianna fáil", "fine gael", "sinn féin", "labour", "green party", "social democrats",
           "independent ireland", "aontú", "people before profit", "solidarity", "independent", "non-party"]
ARTICLES = {"Carlow": "Carlow County Council", "Cavan": "Cavan County Council", "Clare": "Clare County Council",
    "Cork City": "Cork City Council", "Cork County": "Cork County Council", "Donegal": "Donegal County Council",
    "Dublin City": "Dublin City Council", "Dún Laoghaire-Rathdown": "Dún Laoghaire–Rathdown County Council",
    "Fingal": "Fingal County Council", "Galway City": "Galway City Council", "Galway County": "Galway County Council",
    "Kerry": "Kerry County Council", "Kildare": "Kildare County Council", "Kilkenny": "Kilkenny County Council",
    "Laois": "Laois County Council", "Leitrim": "Leitrim County Council", "Limerick": "Limerick City and County Council",
    "Longford": "Longford County Council", "Louth": "Louth County Council", "Mayo": "Mayo County Council",
    "Meath": "Meath County Council", "Monaghan": "Monaghan County Council", "Offaly": "Offaly County Council",
    "Roscommon": "Roscommon County Council", "Sligo": "Sligo County Council", "South Dublin": "South Dublin County Council",
    "Tipperary": "Tipperary County Council", "Waterford": "Waterford City and County Council",
    "Westmeath": "Westmeath County Council", "Wexford": "Wexford County Council", "Wicklow": "Wicklow County Council"}


def isparty(s): return any(p in s.lower() for p in PARTIES)
def isname(s):
    s = s.strip()
    return bool(re.match(r"^[A-ZÁÉÍÓÚ][\wáéíóúÁÉÍÓÚ'’.\-]+(?:\s+[A-ZÁÉÍÓÚ'’][\wáéíóúÁÉÍÓÚ'’.\-]*){1,3}$", s)) and len(s) < 36 and not isparty(s)


def api(params):
    for a in range(4):
        try:
            return requests.get(API, params=params, headers=H, timeout=40).json()
        except Exception:
            time.sleep(2 * (a + 1))
    return {}


def grid(tbl):
    g, pend = [], {}
    for tr in tbl.find_all("tr"):
        out, ci = [], 0
        def fp():
            nonlocal ci
            while ci in pend:
                t, r = pend[ci]; out.append(t); pend[ci] = (t, r - 1)
                if pend[ci][1] <= 0: del pend[ci]
                ci += 1
        for c in tr.find_all(["td", "th"]):
            fp(); t = re.sub(r"\[[a-z0-9]\]", "", c.get_text(" ", strip=True)).strip()
            cs = int(c.get("colspan", 1) or 1); rs = int(c.get("rowspan", 1) or 1)
            for _ in range(cs):
                out.append(t)
                if rs > 1: pend[ci] = (t, rs - 1)
                ci += 1
        fp()
        if any(x.strip() for x in out): g.append(out)
    return g


def roster(title, la):
    secs = api({"action": "parse", "page": title, "prop": "sections", "format": "json", "redirects": 1}).get("parse", {}).get("sections", [])
    idx = next((s["index"] for s in secs if "councillors by electoral" in s["line"].lower()), None) or \
        next((s["index"] for s in secs if s["line"].lower().strip() in ("councillors", "members", "elected members")), None)
    if not idx: return []
    html = api({"action": "parse", "page": title, "prop": "text", "section": idx, "format": "json"}).get("parse", {}).get("text", {}).get("*")
    if not html: return []
    out = []
    for tbl in BeautifulSoup(html, "html.parser").find_all("table"):
        for row in grid(tbl):
            pcol = next((i for i, c in enumerate(row) if isparty(c)), None)
            if pcol is None: continue
            lea, name = row[0].strip(), ""
            for i, c in enumerate(row):
                if i == pcol or i == 0: continue
                if isname(c): name = c.strip(); break
            if not name and isname(row[0]) and pcol != 0: name, lea = row[0].strip(), ""
            if name: out.append({"local_authority": la, "lea": lea, "name": name, "party": row[pcol].strip(), "status": "sitting", "source": title})
    seen = set(); return [r for r in out if not (r["name"] in seen or seen.add(r["name"]))]


def main():
    allr = []
    for la, title in ARTICLES.items():
        r = roster(title, la); allr += r; print(f"{la:24} {len(r):3}"); time.sleep(0.7)
    with open(HERE / "councillors_roster.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["local_authority", "lea", "name", "party", "status", "source"])
        w.writeheader(); w.writerows(allr)
    print(f"\nTOTAL {len(allr)} across {len({r['local_authority'] for r in allr})}/31")


if __name__ == "__main__":
    main()
