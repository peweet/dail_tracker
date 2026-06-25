import os
import re
import sys
import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import fitz

BASE = "https://meetings.roscommoncoco.ie/"
OUT = os.path.join(os.path.dirname(__file__), "roscommon_pdfs")
os.makedirs(OUT, exist_ok=True)
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# 6 most recent PAST full-council (Monthly Meeting) meetings as of 2026-06-25
MEETINGS = [
    ("22 Jun 2026", 3371),
    ("25 May 2026", 3370),
    ("27 Apr 2026", 3349),
    ("23 Mar 2026", 3341),
    ("23 Feb 2026", 3328),
    ("26 Jan 2026", 3324),
]


def get_agenda_pdf_url(mid):
    url = f"{BASE}ieListDocuments.aspx?CId=140&MId={mid}&Ver=4"
    r = requests.get(url, headers=H, timeout=40)
    soup = BeautifulSoup(r.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    front = None
    pack = None
    for a in soup.find_all("a", href=True):
        t = a.get_text(" ", strip=True).lower()
        href = a["href"]
        if "agenda frontsheet" in t:
            front = href
        elif "agenda reports pack" in t or "public reports pack" in t:
            pack = href
    return title, front, pack, url


def fetch_pdf(href, mid, kind):
    full = urljoin(BASE, quote(href, safe=":/?=&"))
    r = requests.get(full, headers=H, timeout=60)
    path = os.path.join(OUT, f"m{mid}_{kind}.pdf")
    with open(path, "wb") as f:
        f.write(r.content)
    return path, full, len(r.content)


def extract_agenda_items(pdf_path):
    doc = fitz.open(pdf_path)
    full_text = "\n".join(p.get_text() for p in doc)
    doc.close()
    return full_text


for label, mid in MEETINGS:
    print("=" * 70)
    print(label, "MId", mid)
    title, front, pack, src = get_agenda_pdf_url(mid)
    print("  page title:", title)
    print("  frontsheet:", front)
    target = front or pack
    if not target:
        print("  NO PDF FOUND")
        continue
    kind = "front" if front else "pack"
    path, full, n = fetch_pdf(target, mid, kind)
    print("  downloaded", n, "bytes ->", path)
    txt = extract_agenda_items(path)
    print("  fitz text length:", len(txt))
    print("---- RAW TEXT (first 3500 chars) ----")
    print(txt[:3500])
    print("---- END ----")
