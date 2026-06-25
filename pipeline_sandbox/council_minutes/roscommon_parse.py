import os
import re
import sys
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import fitz

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = "https://meetings.roscommoncoco.ie/"
OUT = os.path.join(os.path.dirname(__file__), "roscommon_pdfs")
os.makedirs(OUT, exist_ok=True)
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# 6 most recent PAST full-council (Monthly Meeting) meetings as of 2026-06-25
MEETINGS = [
    ("2026-06-22", 3371),
    ("2026-05-25", 3370),
    ("2026-04-27", 3349),
    ("2026-03-23", 3341),
    ("2026-02-23", 3328),
    ("2026-01-26", 3324),
]


def get_pdf_link(mid):
    url = f"{BASE}ieListDocuments.aspx?CId=140&MId={mid}&Ver=4"
    r = requests.get(url, headers=H, timeout=40)
    soup = BeautifulSoup(r.text, "html.parser")
    front = None
    for a in soup.find_all("a", href=True):
        t = a.get_text(" ", strip=True).lower()
        if "agenda frontsheet" in t:
            front = a["href"]
            break
    return front, url


def fetch_pdf(href, mid):
    full = urljoin(BASE, quote(href, safe=":/?=&"))
    r = requests.get(full, headers=H, timeout=60)
    path = os.path.join(OUT, f"m{mid}_front.pdf")
    with open(path, "wb") as f:
        f.write(r.content)
    return path


def get_text(pdf_path):
    doc = fitz.open(pdf_path)
    txt = "\n".join(p.get_text() for p in doc)
    doc.close()
    return txt


def clean(s):
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_items(txt):
    # restrict to AGENDA section onward
    m = re.search(r"A\s?G\s?E\s?N\s?D\s?A", txt)
    if m:
        txt = txt[m.end():]
    lines = [ln.rstrip() for ln in txt.split("\n")]
    items = {}
    current_num = None
    title_parts = []
    capturing = False
    # top-level item: a line starting with "N.   Title" where N is integer (not N.N sub-items)
    top_re = re.compile(r"^(\d{1,2})\.\s{2,}(.+)$")

    def flush():
        if current_num is not None:
            items[current_num] = clean(" ".join(title_parts))

    for ln in lines:
        stripped = ln.strip()
        m = top_re.match(stripped)
        # ensure not a sub-item like 10.1 (those have a dot-digit)
        if m and not re.match(r"^\d{1,2}\.\d", stripped):
            flush()
            current_num = int(m.group(1))
            first = m.group(1) + ". " + m.group(2)  # noqa: F841 (kept for clarity)
            title_parts = [m.group(2)]
            # heading title may wrap if it ends with an unterminated clause
            capturing = not _looks_complete(m.group(2))
        elif capturing and stripped:
            # continuation line of a wrapped heading title
            title_parts.append(stripped)
            capturing = not _looks_complete(stripped)
        else:
            capturing = False
    flush()
    ordered = [items[k] for k in sorted(items.keys())]
    return ordered


def _looks_complete(s):
    s = s.strip()
    if not s:
        return True
    # a heading wraps if it ends with a connector / open paren / "and" / "the"
    if s.endswith(("and", "the", "of", "for", "to", "-", "(", ",", "Repair", "Major")):
        return False
    # unbalanced parens => continues
    if s.count("(") > s.count(")"):
        return False
    return True


results = []
for date, mid in MEETINGS:
    front, src = get_pdf_link(mid)
    entry = {"date": date, "source_url": src, "agenda_items": []}
    if not front:
        entry["note"] = "no agenda frontsheet PDF"
        results.append(entry)
        continue
    path = fetch_pdf(front, mid)
    txt = get_text(path)
    entry["fitz_len"] = len(txt)
    entry["agenda_items"] = parse_items(txt)
    results.append(entry)

print(json.dumps(results, indent=2, ensure_ascii=False))
