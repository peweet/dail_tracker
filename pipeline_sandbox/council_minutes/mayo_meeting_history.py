"""Mayo County Council meeting-history (agenda items) extractor.

Source: eAgenda portal (ASP.NET WebForms) at https://eagenda.mayo.ie/
Flow per meeting:
  1. GET base page -> capture VIEWSTATE/EVENTVALIDATION.
  2. POST __EVENTTARGET=ddlSelectCouncilDate with chosen date -> reveals type dropdown.
  3. POST __EVENTTARGET=ddlSelectCouncilType=Monthly -> renders agenda HTML.
Parse table#gvCountyCouncilAgendaItems (Item No / Item Title) + #gvCountyCouncilNoticesOfMotion.

No PDF/OCR needed; the agenda is born-HTML.
"""
import json
import re
import sys

import requests
from bs4 import BeautifulSoup

BASE = "https://eagenda.mayo.ie/"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
     "Accept-Encoding": "gzip, deflate"}
DISPLAY_URL = "https://eagenda.mayo.ie/"  # accessible mirror apps.mayococo.ie is dead


def form_fields(soup):
    d = {}
    for i in soup.find_all("input"):
        n = i.get("name")
        if n and i.get("type") != "submit":
            d[n] = i.get("value", "") or ""
    return d


def clean(s):
    s = re.sub(r"\s+", " ", s or "").strip()
    return s


def get_county_council_dates(soup, year_min=2024):
    """Return list of (label, value) for County Council meeting dates >= year_min."""
    out = []
    sel = soup.find("select", {"name": "ddlSelectCouncilDate"})
    if not sel:
        return out
    for o in sel.find_all("option"):
        val = o.get("value", "")
        m = re.search(r"/(\d{4})\b", val)  # value like 12/9/2024 12:00:00 AM
        if not m:
            continue
        yr = int(m.group(1))
        if yr >= year_min:
            out.append((clean(o.get_text()), val))
    return out


def fetch_agenda(session, date_val):
    """Two-step postback for one date; return result soup (or None)."""
    r = session.get(BASE, timeout=45)
    so = BeautifulSoup(r.content, "html.parser")
    f = form_fields(so)
    f.update({"__EVENTTARGET": "ddlSelectCouncilDate", "__EVENTARGUMENT": "",
              "ddlSelectCouncilDate": date_val})
    r2 = session.post(BASE, data=f, timeout=60)
    so2 = BeautifulSoup(r2.content, "html.parser")
    f2 = form_fields(so2)
    f2["ddlSelectCouncilDate"] = date_val
    f2.update({"__EVENTTARGET": "ddlSelectCouncilType", "__EVENTARGUMENT": "",
               "ddlSelectCouncilType": "Monthly"})
    r3 = session.post(BASE, data=f2, timeout=60)
    return BeautifulSoup(r3.content, "html.parser")


def parse_items(soup):
    """Extract agenda item titles + notices of motion."""
    items = []
    t = soup.find("table", {"id": "gvCountyCouncilAgendaItems"})
    if t:
        for row in t.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            no = clean(cells[0].get_text())
            if not re.fullmatch(r"\d+", no):
                continue  # header / doc continuation row
            title = clean(cells[1].get_text(" "))
            if not title:
                continue
            # Item 1 is often the Irish minutes line; keep but trim
            items.append(f"{no}. {title}")
    # Notices of Motion
    nom = soup.find("table", {"id": "gvCountyCouncilNoticesOfMotion"})
    if nom:
        for row in nom.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            mtitle = clean(cells[0].get_text(" "))
            if not mtitle or mtitle.lower() == "motion title":
                continue
            cllr = clean(cells[1].get_text(" "))
            short = mtitle if len(mtitle) <= 160 else mtitle[:157] + "..."
            tag = f"Notice of Motion: {short}"
            if cllr:
                tag += f" ({cllr})"
            items.append(tag)
    return items


def main():
    n_max = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    s = requests.Session()
    s.headers.update(H)
    r = s.get(BASE, timeout=45)
    so = BeautifulSoup(r.content, "html.parser")
    dates = get_county_council_dates(so, 2024)
    print(f"[info] {len(dates)} County Council dates >=2024 (newest first)",
          file=sys.stderr)
    meetings = []
    for label, val in dates:
        if len(meetings) >= n_max:
            break
        try:
            asoup = fetch_agenda(s, val)
            items = parse_items(asoup)
        except Exception as e:
            print(f"[err] {label}: {e}", file=sys.stderr)
            continue
        if not items:
            # future / not-yet-published meeting: skip, do not pad
            print(f"[skip] {label}: 0 items (no published agenda)", file=sys.stderr)
            continue
        mm = re.match(r"(\d+)/(\d+)/(\d{4})", val)
        iso = (f"{mm.group(3)}-{int(mm.group(1)):02d}-{int(mm.group(2)):02d}"
               if mm else label)
        total = len(items)
        capped = items[:15]  # schema: <=15 agenda items per meeting
        print(f"[ok] {label}: {total} items (kept {len(capped)})", file=sys.stderr)
        meetings.append({"date": iso, "label": label,
                         "n_items_total": total, "n_items_kept": len(capped),
                         "agenda_items": capped, "source_url": DISPLAY_URL})
    out = json.dumps(meetings, ensure_ascii=False, indent=2)
    print(out)
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "mayo_meeting_history.json"), "w",
              encoding="utf-8") as fh:
        fh.write(out)


if __name__ == "__main__":
    main()
