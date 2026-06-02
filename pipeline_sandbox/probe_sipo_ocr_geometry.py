"""PROBE v2 (throwaway): the text layer is coherent (probe_sipo_ocr_text.py
proved 0% char-fragmentation) but the line-stream de-aligns rows — candidate
name, constituency and € amount each land on SEPARATE lines instead of one row.

This probe tests the geometry approach: read page.get_text("words") (each word
carries an x0,y0,x1,y1 bbox), cluster words into ROWS by y-coordinate, then look
at the x-spread to see if columns (name | constituency | amount) align well
enough to reconstruct rows. This is the right tool for a scanned TABLE — far
better than a vertical line stream.

It also measures the clean-amount rate: of the € tokens, how many parse to a
plausible expense figure (vs OCR garble like "€ 85651" missing its decimal, or
"€249207").

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_geometry.py
Reads only; writes nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import fitz  # PyMuPDF

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

OCR_PDF = ROOT / "data/bronze/scan_pdf/output/ff_sipo_ge_2024_expenses-ocr.pdf"

# a token that looks like a euro amount sitting in the amount column
EURO_TOKEN = re.compile(r"€")
# parse a "clean" money value: optional €, thousands groups, 2-dp cents
CLEAN_MONEY = re.compile(r"^\(?€?\s?\d{1,3}(?:,\d{3})*\.\d{2}\)?,?$")
# any digit-bearing token
HAS_DIGIT = re.compile(r"\d")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def cluster_rows(words: list[tuple], y_tol: float = 4.0) -> list[list[tuple]]:
    """Group words (x0,y0,x1,y1,text,...) into rows by y0 proximity."""
    rows: list[list[tuple]] = []
    for w in sorted(words, key=lambda w: (round(w[1] / y_tol), w[0])):
        if rows and abs(rows[-1][0][1] - w[1]) <= y_tol:
            rows[-1].append(w)
        else:
            rows.append([w])
    # sort each row left-to-right
    for r in rows:
        r.sort(key=lambda w: w[0])
    return rows


def row_text(row: list[tuple]) -> str:
    return " ".join(w[4] for w in row)


def main() -> None:
    if not OCR_PDF.exists():
        print("OCR pdf not found:", OCR_PDF)
        return
    doc = fitz.open(OCR_PDF)
    hr("DOCUMENT")
    print(f"file : {OCR_PDF.name}")
    print(f"pages: {doc.page_count}")

    # --- reconstruct rows across all pages ---
    all_rows: list[tuple[int, list[tuple]]] = []
    for pno, page in enumerate(doc, start=1):
        words = page.get_text("words")  # (x0,y0,x1,y1,word,block,line,word_no)
        for row in cluster_rows(words):
            all_rows.append((pno, row))

    hr("ROW RECONSTRUCTION (geometry, clustered by y-coordinate)")
    print(f"total reconstructed rows : {len(all_rows):,}")

    # rows that carry a euro amount = candidate-expense candidate rows
    euro_rows = [(p, r) for (p, r) in all_rows if any(EURO_TOKEN.search(w[4]) for w in r)]
    print(f"rows containing a € token: {len(euro_rows):,}")

    # --- how many euro tokens are CLEAN (parseable) vs garbled? ---
    clean, garbled = 0, 0
    garble_samples: list[str] = []
    for _p, r in euro_rows:
        for w in r:
            if EURO_TOKEN.search(w[4]):
                # also try joining with the immediate next token (€ often split)
                joined = w[4]
                if CLEAN_MONEY.match(w[4].replace(" ", "")):
                    clean += 1
                else:
                    garbled += 1
                    if len(garble_samples) < 20:
                        garble_samples.append(joined)
    tot = clean + garbled
    hr("AMOUNT QUALITY (the OCR ceiling on the € column)")
    print(f"€ tokens total           : {tot:,}")
    print(f"clean (€1,234.56 shape)  : {clean:,}  ({clean / max(1, tot):.0%})")
    print(f"garbled / needs repair   : {garbled:,}  ({garbled / max(1, tot):.0%})")
    print("garble samples:", garble_samples)

    hr("SAMPLE: 30 reconstructed rows that carry a € amount (judge by eye)")
    for p, r in euro_rows[:30]:
        print(f"  p{p:>2} | {row_text(r)}")

    hr("VERDICT")
    print(f"euro-bearing rows reconstructed : {len(euro_rows):,}")
    print(f"clean-amount rate               : {clean / max(1, tot):.0%}")
    print("=> if rows read like 'Name Constituency €Amount' left-to-right, a")
    print("   column-split extractor is viable; if name/constituency/amount are")
    print("   still on different y-rows, the form layout fights row clustering.")


if __name__ == "__main__":
    main()
