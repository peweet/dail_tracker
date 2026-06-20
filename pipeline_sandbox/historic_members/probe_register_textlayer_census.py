"""
probe_register_textlayer_census.py  (SANDBOX, read-only census)
---------------------------------------------------------------
Answers two questions before we commit to any OCR or roster work:
  1. How far back do the annual Register-of-Interests PDFs go on oireachtas.ie?
  2. For each year, is it BORN-DIGITAL (extractable text -> free to ingest, just
     parser tuning) or SCANNED (no text layer -> needs off-box OCR)?

It does NOT OCR anything and never touches INTERESTS_PDF_DIR. Pure HTTP +
PyMuPDF text extraction, safe to run locally. Output: a per-year/per-house table
+ JSON so we can size the OCR queue and the front-end "data back to YYYY" line.

Run:  python -m pipeline_sandbox.historic_members.probe_register_textlayer_census
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

UA = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
INDEX = "https://www.oireachtas.ie/en/publications/?topic%5B%5D=register-of-members-interests&resultsPerPage=200"
PDF_DIR = Path(__file__).parent / "_pdfs_census"


def discover_registers() -> list[dict]:
    """All ANNUAL registers (excludes supplements). -> [{house, interest_year, url, fname}]."""
    r = requests.get(INDEX, headers={"User-Agent": UA}, timeout=30)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    out: list[dict] = []
    seen: set[tuple[str, int]] = set()
    for card in soup.select("div.c-publications-list__item"):
        a = card.select_one("p.c-publications-list__view a[href]")
        if not a:
            continue
        href = a["href"]
        fname = href.rsplit("/", 1)[-1]
        if "register-of-member" not in href or "supplement" in href:
            continue
        house = "seanad" if "seanad" in href else ("dail" if "dail" in href else "?")
        m = re.match(r"(\d{4})-\d{2}-\d{2}_", fname)
        if not m:
            continue
        # Convention: register published Feb/Mar of year Y reports interests for Y-1.
        interest_year = int(m.group(1)) - 1
        key = (house, interest_year)
        if key in seen:
            continue
        seen.add(key)
        out.append({"house": house, "interest_year": interest_year, "url": href, "fname": fname})
    return out


def download(url: str) -> Path | None:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    dest = PDF_DIR / url.rsplit("/", 1)[-1]
    if dest.exists() and dest.stat().st_size > 5_000:
        return dest
    try:
        with requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=90) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
        if dest.stat().st_size < 5_000 or open(dest, "rb").read(5) != b"%PDF-":
            dest.unlink(missing_ok=True)
            return None
        return dest
    except Exception as exc:  # noqa: BLE001
        print(f"  download failed {url}: {exc}")
        return None


def classify(pdf: Path) -> dict:
    """Born-digital vs scanned, by extractable-text density."""
    doc = fitz.open(pdf)
    pages = doc.page_count
    total_chars = 0
    sample = min(pages, 12)
    for i in range(sample):
        total_chars += len(doc[i].get_text("text").strip())
    doc.close()
    chars_per_page = total_chars / max(sample, 1)
    # A born-digital register runs ~hundreds-to-thousands of chars/page; a scanned
    # image yields ~0. Use a conservative floor.
    if chars_per_page < 50:
        verdict = "SCANNED (needs OCR)"
    elif chars_per_page < 400:
        verdict = "SPARSE (verify)"
    else:
        verdict = "born-digital"
    return {"pages": pages, "chars_per_page": round(chars_per_page), "verdict": verdict}


def main() -> None:
    regs = discover_registers()
    print(f"discovered {len(regs)} annual registers on the index\n")
    rows = []
    for reg in sorted(regs, key=lambda r: (r["house"], r["interest_year"])):
        pdf = download(reg["url"])
        if not pdf:
            rec = {**reg, "verdict": "download failed"}
        else:
            rec = {**reg, "size_kb": round(pdf.stat().st_size / 1024), **classify(pdf)}
        rows.append(rec)
        print(
            f"  {rec['house']:<6} {rec['interest_year']}  "
            f"{rec.get('pages','?'):>4}pp  {rec.get('chars_per_page','?'):>6} c/pg  "
            f"{rec.get('size_kb','?'):>6}kb  {rec.get('verdict','?')}"
        )
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    (PDF_DIR / "textlayer_census.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")

    print("\n=== SUMMARY ===")
    for house in ("dail", "seanad"):
        hr = [r for r in rows if r["house"] == house and "interest_year" in r]
        if not hr:
            continue
        yrs = sorted(r["interest_year"] for r in hr)
        scanned = sorted(r["interest_year"] for r in hr if r.get("verdict", "").startswith("SCANNED"))
        digital = sorted(r["interest_year"] for r in hr if r.get("verdict") == "born-digital")
        print(f"{house}: registers span {min(yrs)}-{max(yrs)} ({len(yrs)} years)")
        print(f"   born-digital (free): {digital}")
        print(f"   SCANNED (OCR queue): {scanned}")


if __name__ == "__main__":
    main()
