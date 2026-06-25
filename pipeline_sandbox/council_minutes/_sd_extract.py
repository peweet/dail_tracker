"""Extract agenda items for the 6 most recent SDCC full County Council meetings.

Born-digital HTML portal (meetings.southdublin.ie) - no OCR / PDF parsing needed.
Each agenda item is a div.row.border-dark whose item number is in a
div.bg-sdccroyalblue and whose title is the first bold <span>/<p> in the
content block. Headed Items get their CAPS title; Questions / Motions get
their question/motion text.
"""
import json
import re
import unicodedata
import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
BASE = "https://meetings.southdublin.ie"

MEETINGS = [
    ("08/06/2026", 2753),
    ("11/05/2026", 2735),
    ("13/04/2026", 2720),
    ("09/03/2026", 2707),
    ("09/02/2026", 2687),
    ("12/01/2026", 2680),
]


def clean(text):
    # The portal encoding mangles some chars (U+FFFD etc.). Normalise + tidy.
    text = text.replace("�", " ").replace("\xa0", " ")
    text = unicodedata.normalize("NFKC", text)
    # Fold unicode dashes/quotes to ASCII for clean readable titles.
    for ch in "‐‑‒–—―":
        text = text.replace(ch, "-")
    text = text.replace("‘", "'").replace("’", "'")
    text = text.replace("“", '"').replace("”", '"')
    text = re.sub(r"\s+", " ", text).strip()
    return text


def iso_date(ddmmyyyy):
    d, m, y = ddmmyyyy.split("/")
    return f"{y}-{m}-{d}"


def _block_title(block):
    num_el = block.select_one("div.bg-sdccroyalblue")
    num = clean(num_el.get_text(" ", strip=True)) if num_el else ""
    content = block.select_one("div.border-top")
    if content is None:
        return num, None
    title = None
    bold = content.find("span", style=re.compile("bold"))
    if bold:
        title = clean(bold.get_text(" ", strip=True))
    if not title:
        p = content.find("p")
        if p:
            title = clean(p.get_text(" ", strip=True))
    if not title:
        title = clean(content.get_text(" ", strip=True))[:160]
    return num, title


def extract_items(html):
    """Return clean agenda items.

    Headed Items (H-I) are kept verbatim - that is the substantive tabled
    business. Questions (Qu), Motions (Mot) and Correspondence (Cor) are
    procedural standing sections with one entry per councillor submission;
    we collapse each of those into a single counted section line so the
    agenda stays readable and <=15 items, without dropping any section.
    """
    soup = BeautifulSoup(html, "html.parser")
    headed = []
    seen = set()
    counts = {"Qu": 0, "Mot": 0, "Cor": 0}
    for block in soup.select("div.row.border.border-dark"):
        num, title = _block_title(block)
        if not title:
            continue
        prefix = num.split("(")[0].strip().split()[0] if num else ""
        if prefix in counts:
            counts[prefix] += 1
            continue
        label = clean(f"{num} {title}".strip()) if num else title
        if label and label.lower() not in seen:
            seen.add(label.lower())
            headed.append(label)

    items = list(headed)
    if counts["Qu"]:
        items.append(f"Questions to the Chief Executive ({counts['Qu']} submitted)")
    if counts["Cor"]:
        items.append(f"Replies, Acknowledgements & Correspondence ({counts['Cor']})")
    if counts["Mot"]:
        items.append(f"Motions ({counts['Mot']} submitted)")
    # Bounded to <=15 readable items.
    return items[:15]


def main():
    results = []
    for ddmmyyyy, mid in MEETINGS:
        url = f"{BASE}/Home/Agenda/{mid}"
        r = requests.get(url, headers=H, timeout=40)
        items = extract_items(r.text)
        results.append({
            "date": iso_date(ddmmyyyy),
            "source_url": url,
            "n_items": len(items),
            "agenda_items": items,
        })
        safe = lambda s: s.encode("ascii", "replace").decode("ascii")
        print(f"=== {iso_date(ddmmyyyy)} ({url}) -> {len(items)} items ===")
        for it in items:
            print("   ", safe(it))
        print()

    with open("pipeline_sandbox/council_minutes/_sd_agenda_out.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
