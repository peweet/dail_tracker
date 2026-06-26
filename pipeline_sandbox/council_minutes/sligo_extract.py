"""Sligo County Council - meeting-history (agenda items) extractor.

Sligo publishes each full-council meeting as a born-digital HTML page
(MeetingMainBody,<id>,en.html). Two layouts seen:

  * 2026 pages: hierarchical agenda. Top-level numbers are STANDING headings
    (1 Declaration of Interests, 2 Confirmation of Minutes, 3 Headed Items,
    4 Notices of Motion, 5 Votes of Sympathy ...). The SUBSTANTIVE tabled
    items are the sub-numbered entries under "Headed Items" (3.x) and
    "Notices of Motion" (4.x). We surface those.
  * older pages (e.g. Dec 2025): flat list of numbered UPPERCASE items
    ("1. MINUTES OF ...", "11. TAKING IN CHARGE OF ..."). We surface those.

Source: https://www.sligococo.ie/YourCouncil/CountyCouncil/Minutes/
"""
import json
import re
import sys

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings()

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"}
BASE = "https://www.sligococo.ie"

# 6 most recent full-council ORDINARY meetings (2024+), newest first.
MEETINGS = [
    ("2026-05-11", "/YourCouncil/CountyCouncil/Minutes/Minutes2026/MeetingMainBody,71053,en.html"),
    ("2026-04-13", "/YourCouncil/CountyCouncil/Minutes/Minutes2026/MeetingMainBody,70796,en.html"),
    ("2026-03-02", "/YourCouncil/CountyCouncil/Minutes/Minutes2026/MeetingMainBody,70560,en.html"),
    ("2026-02-09", "/YourCouncil/CountyCouncil/Minutes/Minutes2026/MeetingMainBody,70275,en.html"),
    ("2026-01-12", "/YourCouncil/CountyCouncil/Minutes/Minutes2026/MeetingMainBody,70124,en.html"),
    ("2025-12-01", "/YourCouncil/CountyCouncil/Minutes/Minutes2025/MeetingMainBody,69965,en.html"),
]

MAX_ITEMS = 15


def fetch(url):
    r = requests.get(url, headers=H, timeout=40, verify=False)
    r.raise_for_status()
    return r.content.decode("utf-8", errors="replace")


def clean(s):
    s = s.replace("\xa0", " ").replace("‑", "-").replace("�", " ")
    s = s.replace("‘", "'").replace("’", "'")
    s = s.replace("“", '"').replace("”", '"')
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip(":.").strip()
    return s


def page_lines(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "nav", "header", "footer"]):
        t.decompose()
    txt = soup.get_text("\n", strip=True)
    return [l for l in txt.split("\n") if l.strip()]


def extract_items(lines):
    """Prefer substantive sub-items (Headed Items / Notices of Motion).

    Fall back to flat top-level numbered items for the older verbose layout.
    """
    sub = []  # (sort_key, title) for x.y items under Headed Items / NoM
    flat = {}  # num -> title for top-level items

    # detect the standing-heading section numbers for "Headed Items" and
    # "Notices of Motion" so we only pull their children.
    section_nums = set()
    for l in lines:
        m = re.match(r"^(\d{1,2})[.\s\xa0]+(HEADED ITEMS|NOTICES OF MOTION)\b", l, re.I)
        if m:
            section_nums.add(m.group(1))

    for l in lines:
        # sub-item like "3.5   TITLE" or "4.21  TITLE"
        m = re.match(r"^(\d{1,2})\.(\d{1,2})[\s\xa0.]+([A-Z].+)$", l)
        if m and m.group(1) in section_nums:
            title = clean(m.group(3))
            if len(title) >= 4:
                key = (int(m.group(1)), int(m.group(2)))
                sub.append((key, title))
            continue
        # flat top-level "11. TITLE"
        m2 = re.match(r"^(\d{1,2})\.[\s\xa0]*([A-Z].+)$", l)
        if m2:
            num = int(m2.group(1))
            title = clean(m2.group(2))
            if len(title) >= 4 and num not in flat:
                flat[num] = title

    if sub:
        sub.sort(key=lambda x: x[0])
        # dedupe titles preserving order
        seen, items = set(), []
        for _, t in sub:
            if t not in seen:
                seen.add(t)
                items.append(t)
        return items[:MAX_ITEMS]

    ordered = [flat[k] for k in sorted(flat)]
    return ordered[:MAX_ITEMS]


def main():
    out = []
    for date, path in MEETINGS:
        url = BASE + path
        try:
            html = fetch(url)
            lines = page_lines(html)
            items = extract_items(lines)
            sys.stderr.write(f"{date}: {len(items)} items\n")
            out.append({"date": date, "source_url": url, "agenda_items": items})
        except Exception as e:  # noqa
            sys.stderr.write(f"{date}: FAIL {e}\n")
            out.append({"date": date, "source_url": url, "agenda_items": []})
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
