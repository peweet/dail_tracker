"""Parse Louth County Council agenda-only PDFs into agenda item lists.

Born-digital PDFs (fitz text); structure is:
    <header>
    Cl\xfffdr Oibre / Agenda
    <section heading in Irish/English>
    N.0
    <blank>
    <agenda item title  (often bilingual: Irish / English)>
    <optional descriptive sub-lines>
    M.0
    ...

We extract the title line(s) directly after each "N.0" item-number marker, keep
the English portion where a bilingual " / English" split exists, and stop the
title at the next blank/descriptive run.
"""
import fitz
import os
import re

OUT = os.path.join(os.path.dirname(__file__), "louth_pdfs")

# Map filename -> ISO date (from the page header / filename)
DOCS = [
    ("2026-04-20", "county-council-monthly-meeting-20-04-2026-agenda-only.pdf"),
    ("2026-03-23", "county-council-monthly-meeting-23-03-2026-agenda-only.pdf"),
    ("2026-02-16", "county-council-monthly-meeting-16-02-2026-agenda-only.pdf"),
    ("2026-01-19", "county-council-monthly-meeting-19-01-2026-agenda-only.pdf"),
    ("2025-12-15", "county-council-monthly-meeting-15-12-2025-agenda-only.pdf"),
    ("2025-11-17", "county-council-monthly-meeting-17-11-2025-agenda-only.pdf"),
]

ITEM_RE = re.compile(r"^\d+\.0$")
# Section-divider lines that precede / follow real items but are not items.
SECTION_NOISE = re.compile(
    r"(Powered by TCPDF|www\.tcpdf|Cl.r Oibre|/ Agenda)", re.I
)
# Lines that are pure section dividers, not agenda items (when captured alone).
DIVIDER_ONLY = {
    "notices of motion",
    "notices of question",
    "statutory business",
    "correspondence",
    "for discussion",
}
# Management-report bullet block that follows the CE-report item with no blank line.
MGMT_MARKER = "Consideration of Reports and"


def deaccent_clean(s: str) -> str:
    # the replacement char marks Irish-language glyphs lost in the font; drop them
    s = s.replace("�", "")
    # collapse a stray Irish prefix glued to English by a slash w/o space
    s = re.sub(r"\s+", " ", s).strip(" .-")
    return s.strip()


def english_part(title: str) -> str:
    """For bilingual headings 'Irish text / English text', keep the English half."""
    if "/" in title:
        left, right = title.rsplit("/", 1)
        right = right.strip()
        # keep English half only if it reads as English (mostly ASCII words)
        if re.search(r"[A-Za-z]{3,}", right) and len(right) > 6:
            return right
    return title.strip()


def shorten_title(title: str) -> str:
    """Reduce an over-captured heading+body to a clean agenda item title."""
    t = title
    # Notice of motion/question: keep "Notice of X - <councillor name(s)>" only.
    m = re.match(
        r"(Notice of (?:Motion|Question)\s*-\s*(?:Councillor|Cll?rs?\.?)\s+"
        r"[A-Z][A-Za-z'\-]+(?:[ ,&A-Za-z'\.\-]*?[A-Z][A-Za-z'\-]+)?)",
        t,
    )
    if m and ("Notice of Motion" in t or "Notice of Question" in t):
        cand = m.group(1).strip(" .-,")
        # guard: don't let it run into the motion body
        cand = re.split(r"\s+(?:That|I |Will|Calling|Asking|Can |Could |Please|Disability|Supports|Recognising|\")", cand)[0]
        return cand.strip(" .-,")
    # otherwise cut at the first sentence-ish boundary so the heading stays short
    # break at "  " double-space markers that separate heading from body, or at
    # the first full stop that ends a clause longer than a few words.
    # Prefer cutting before known descriptive openers.
    for marker in [
        " Elected Members appointed",
        " Where a member attends",
        " To consider the Draft",
        " Consideration of the Audit",
        " Minutes of Monthly Meeting",
        " Minutes of Budget Meeting",
        " Consideration of the Audit",
        " Clare County Council",
        " Laois County Council",
        " Galway City Council",
        " Offaly County Council",
    ]:
        if marker in t:
            t = t.split(marker)[0]
            break
    # the CE management-report item over-captures the bullet list -> normalise
    if MGMT_MARKER in t or "Recommendations from Chief Executive" in t:
        t = "Consideration of Reports and Recommendations from Chief Executive (Monthly Management Report)"
    t = re.sub(r"\s+", " ", t).strip(" .-")
    # drop a leading mojibake Irish run that precedes a slash within one line
    if "/" in t and not t.startswith("S."):
        t = english_part(t)
    # cap absolute length
    if len(t) > 160:
        t = t[:157].rstrip() + "..."
    return t


def parse(path: str):
    doc = fitz.open(path)
    lines = []
    for p in doc:
        lines.extend(p.get_text().split("\n"))
    doc.close()

    items = []
    i = 0
    n = len(lines)
    while i < n:
        if ITEM_RE.match(lines[i].strip()):
            j = i + 1
            title_lines = []
            while j < n:
                raw = lines[j].strip()
                if raw == "":
                    if title_lines:
                        break
                    j += 1
                    continue
                if ITEM_RE.match(raw):
                    break
                title_lines.append(raw)
                j += 1
            title = deaccent_clean(" ".join(title_lines))
            title = english_part(title)
            title = SECTION_NOISE.sub("", title).strip(" .-")
            title = shorten_title(title)
            low = title.lower().strip(" .-")
            if (
                title
                and not SECTION_NOISE.search(title)
                and low not in DIVIDER_ONLY
                and not low.startswith("correspondence ")
            ):
                items.append(title)
            i = j
        else:
            i += 1
    return items


if __name__ == "__main__":
    import json

    result = []
    for date, fn in DOCS:
        path = os.path.join(OUT, fn)
        items = parse(path)[:15]
        result.append({"date": date, "file": fn, "n_items": len(items), "items": items})
        print("=" * 60)
        print(date, fn, "->", len(items), "items")
        for k, it in enumerate(items, 1):
            print(f"  {k:2d}. {it}")
    with open(os.path.join(os.path.dirname(__file__), "louth_agendas.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
