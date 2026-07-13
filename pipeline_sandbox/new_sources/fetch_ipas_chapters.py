"""Fetch the two C&AG asylum-accommodation chapters + extract text (sandbox).

Born-digital PDFs — fitz text extraction, no OCR expected. Text goes to
c:/tmp/dail_new_sources/bronze/cag_reports/text/ for downstream parsing.
"""
import sys
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

from _common import SILVER, BRONZE, fetch, cache_raw

ch = pl.read_parquet(SILVER / "cag_chapters.parquet")
targets = ch.filter(
    pl.col("title_display").str.contains("international protection accommodation")
    | pl.col("title_display").str.contains("direct provi")
)
print(targets.select("report_year", "title_display", "pdf_url"))

TEXT_DIR = BRONZE / "cag_reports" / "text"
TEXT_DIR.mkdir(parents=True, exist_ok=True)

for r in targets.iter_rows(named=True):
    url = r["pdf_url"]
    name = url.rsplit("/", 1)[-1]
    pdf_path = BRONZE / "cag_reports" / "pdf" / name
    if not pdf_path.exists():
        payload, meta = fetch(url, binary=True)
        pdf_path, sha = cache_raw("cag_reports/pdf", name, payload)
        print(f"fetched {name}: {meta['bytes']/1024:.0f} KB sha={sha[:12]}")
    doc = fitz.open(pdf_path)
    pages = []
    for i, page in enumerate(doc):
        pages.append(f"\n=== PAGE {i+1} ===\n" + page.get_text("text"))
    txt = "".join(pages)
    out = TEXT_DIR / (name[:-4] + ".txt")
    out.write_text(txt, encoding="utf-8")
    n_chars = len(txt.replace(" ", "").replace("\n", ""))
    print(f"{name}: {doc.page_count} pages, {n_chars} non-space chars "
          f"({'TEXT OK' if n_chars > 500 * doc.page_count else 'LOW TEXT - may need OCR'})")
    print(f"  text -> {out}")
