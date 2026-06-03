"""Read-only census of the SIPO scanned PDFs in data/bronze/scan_pdf/.

Characterises each document so we can categorise the corpus in full: page count,
whether a text layer exists (chars/page), page geometry, and a coarse content
sniff (does it look like a National-Agent expenses return vs the donations
register?). Nothing is written; this is an assessment aid only.
"""

import sys
from pathlib import Path

import fitz  # PyMuPDF

sys.stdout.reconfigure(encoding="utf-8")

SCAN_DIR = Path("data/bronze/scan_pdf")

# include the OCR'd FF artifact + the pristine FF source under target/
EXTRA = [
    SCAN_DIR / "target" / "ff_sipo_ge_2024_expenses.pdf",
    SCAN_DIR / "output" / "ff_sipo_ge_2024_expenses-ocr.pdf",
]

KEYWORDS = (
    "national agent",
    "election expenses",
    "statement of",
    "donation",
    "advertising",
    "publicity",
    "constituency",
)


def sniff(doc: fitz.Document, n_pages: int) -> dict:
    """Sample text from first few + a middle page for keyword hits."""
    sample_idx = sorted({0, 1, 2, min(3, n_pages - 1), n_pages // 2, n_pages - 1})
    text = []
    total_chars = 0
    for i in range(n_pages):
        t = doc[i].get_text("text")
        total_chars += len(t.strip())
        if i in sample_idx:
            text.append(t.lower())
    blob = "\n".join(text)
    hits = {k: blob.count(k) for k in KEYWORDS if k in blob}
    return {"total_chars": total_chars, "hits": hits}


def main() -> None:
    pdfs = sorted(SCAN_DIR.glob("*.pdf")) + [p for p in EXTRA if p.exists()]
    print(f"{'file':<46}{'pp':>4}{'MB':>7}{'chars':>9}{'c/pg':>7}  layer  geometry")
    print("-" * 110)
    for p in pdfs:
        doc = fitz.open(p)
        n = doc.page_count
        mb = p.stat().st_size / 1e6
        info = sniff(doc, n)
        cpg = info["total_chars"] / max(n, 1)
        layer = "TEXT" if cpg > 100 else ("thin" if cpg > 5 else "NONE")
        pg0 = doc[0].rect
        geom = f"{pg0.width:.0f}x{pg0.height:.0f}"
        rel = str(p).replace(str(SCAN_DIR) + "\\", "").replace(str(SCAN_DIR) + "/", "")
        print(
            f"{rel:<46}{n:>4}{mb:>7.1f}{info['total_chars']:>9}{cpg:>7.0f}  {layer:<5}  {geom}"
        )
        if info["hits"]:
            print(f"      hits: {info['hits']}")
        doc.close()


if __name__ == "__main__":
    main()
