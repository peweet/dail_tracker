"""Build per-council MEETING HISTORY = the agenda (what was tabled) for each meeting.

Per the owner steer (2026-06-23): show the AGENDA, not "what was agreed"; pair with "who sets the
agenda". Two inputs:
  - OCR recent Galway City+County minutes (scanned) via rapidocr (galway_urls.json).
  - Extract agenda items from every clean corpus text (born-digital councils, no OCR).
Output: meeting_history.jsonl — {council, file, date, agenda_items[]}.  Sandbox only.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import unquote

import requests

HERE = Path(__file__).resolve().parent
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
MAX_OCR_PAGES = 20

_ITEM = re.compile(r"ITEM\s*N[O0]\.?\s*(\d+)\s*\.?\s*(.{4,95}?)(?=\s*\d{3,4}\s|ITEM\s*N[O0]|$)", re.I | re.S)


def despace(s: str) -> str:
    """Light cleanup of OCR run-together caps text for display."""
    s = re.sub(r"\s+", " ", s).strip(" .-")
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)          # camel boundary
    s = re.sub(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])", " ", s)
    return re.sub(r"\s+", " ", s).strip()[:90]


def agenda_items(text: str) -> list[str]:
    items = []
    for m in _ITEM.finditer(text):
        title = despace(m.group(2))
        if title and not title.lower().startswith(("page ", "minutes of")):
            items.append(f"{m.group(1)}. {title}")
    # dedupe preserve order
    seen, out = set(), []
    for it in items:
        if it not in seen:
            seen.add(it); out.append(it)
    return out[:25]


def mdate(fn: str) -> str:
    fn = unquote(fn).rsplit("/", 1)[-1]
    m = re.search(r"(\d{1,2})[.\-\s](\d{1,2})[.\-\s](\d{2,4})", fn)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)[-4:]}"
    m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})", fn, re.I)
    return f"{m.group(1)} {m.group(2)}" if m else fn[:22]


def ocr_galway(rows: list[dict]) -> None:
    try:
        from rapidocr_onnxruntime import RapidOCR
        import fitz
    except Exception as e:  # noqa: BLE001
        print("OCR deps missing:", e); return
    ocr = RapidOCR()
    urls = json.loads((HERE / "galway_urls.json").read_text())
    for la, url in urls:
        try:
            pdf = requests.get(url, headers=H, timeout=70).content
            doc = fitz.open(stream=pdf, filetype="pdf")
            txt = "\n".join(l for p in list(doc)[:MAX_OCR_PAGES]
                            for l in [t for _, t, _ in (ocr(p.get_pixmap(dpi=200).tobytes("png"))[0] or [])])
            (HERE / "corpus" / "galway").mkdir(parents=True, exist_ok=True)
            (HERE / "corpus" / "galway" / (re.sub(r'\W+', '_', url.split('/')[-1])[:60] + ".txt")).write_text(txt, encoding="utf-8")
            items = agenda_items(txt)
            rows.append({"council": la, "file": unquote(url.split("/")[-1]), "date": mdate(url),
                         "agenda_items": items})
            print(f"  OCR {la:14} {mdate(url):10} -> {len(items)} agenda items")
        except Exception as e:  # noqa: BLE001
            print(f"  OCR ERR {la} {url.split('/')[-1][:40]}: {type(e).__name__}")


def from_corpus(rows: list[dict]) -> None:
    # map slug dir -> council name from coverage
    import csv
    cov = list(csv.DictReader(open(HERE / "council_coverage.csv", encoding="utf-8")))
    slug2la = {re.sub(r"[^a-z0-9]+", "_", c["local_authority"].lower()).strip("_"): c["local_authority"] for c in cov}
    cdir = HERE / "corpus"
    if not cdir.exists():
        return
    for sub in cdir.iterdir():
        if not sub.is_dir() or sub.name == "galway":
            continue
        la = slug2la.get(sub.name, sub.name)
        for txt in sub.glob("*.txt"):
            items = agenda_items(txt.read_text(encoding="utf-8", errors="ignore"))
            if items:
                rows.append({"council": la, "file": txt.name, "date": mdate(txt.name),
                             "agenda_items": items})


def main():
    rows: list[dict] = []
    print("OCR Galway minutes ...")
    ocr_galway(rows)
    print("Agendas from born-digital corpus ...")
    from_corpus(rows)
    (HERE / "meeting_history.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    from collections import Counter
    print(f"\nmeeting_history.jsonl: {len(rows)} meetings across {len({r['council'] for r in rows})} councils")
    print("by council:", dict(Counter(r["council"] for r in rows)))


if __name__ == "__main__":
    main()
