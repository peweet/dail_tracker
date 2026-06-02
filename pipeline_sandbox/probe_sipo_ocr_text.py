"""PROBE (throwaway): can we recover usable SIPO election-expense data from the
ALREADY-OCR'd PDF by reading its text layer directly, instead of the
find_tables(strategy="text") approach that shattered text into per-character
cells (see data/bronze/scan_pdf/output/*-tables*.csv)?

The OCR layer already exists (ocrmypdf was run previously). ocrmypdf itself is
NOT installed in this venv and we do not need it here — we only read the text
layer with PyMuPDF.

Goal: judge whether the signal (candidate name, constituency, € amount) is
coherent enough in the text layer to be worth a real extractor, or whether the
OCR quality is the hard ceiling.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_text.py
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

# money: €1,234.56 or 1,234.56 or 34723
MONEY_RE = re.compile(r"€?\s?\d{1,3}(?:[,. ]\d{3})*(?:\.\d{2})?")
EURO_RE = re.compile(r"€\s?\d")
# crude "fragmented into single chars" detector: many 1-char tokens in a line
FRAG_RE = re.compile(r"(?:\b\w \w \w \w \w)")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main() -> None:
    if not OCR_PDF.exists():
        print("OCR pdf not found:", OCR_PDF)
        return
    doc = fitz.open(OCR_PDF)
    hr("DOCUMENT")
    print(f"file : {OCR_PDF.name}")
    print(f"pages: {doc.page_count}")

    all_lines: list[str] = []
    frag_lines = 0
    for page in doc:
        txt = page.get_text("text")
        for ln in txt.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            all_lines.append(ln)
            if FRAG_RE.search(ln):
                frag_lines += 1

    hr("TEXT-LAYER COHERENCE (vs the fragmented table CSVs)")
    print(f"non-empty lines           : {len(all_lines):,}")
    print(f"char-fragmented lines      : {frag_lines:,}  ({frag_lines / max(1, len(all_lines)):.1%})")
    print("  (low % => text layer is coherent and find_tables was the problem)")

    euro_lines = [ln for ln in all_lines if EURO_RE.search(ln)]
    money_lines = [ln for ln in all_lines if MONEY_RE.search(ln) and len(ln) > 6]
    hr("MONETARY SIGNAL")
    print(f"lines containing € amount  : {len(euro_lines):,}")
    print(f"lines containing any money : {len(money_lines):,}")

    hr("SAMPLE: first 25 € lines (the candidate-expense signal)")
    for ln in euro_lines[:25]:
        print("  ", ln)

    hr("SAMPLE: raw first-40 non-empty lines (judge OCR quality by eye)")
    for ln in all_lines[:40]:
        print("  ", ln)

    hr("VERDICT")
    coherent = frag_lines / max(1, len(all_lines)) < 0.15
    print(f"text layer coherent enough for regex extraction: {coherent}")
    print(f"€-amount lines available for parsing            : {len(euro_lines):,}")
    if not coherent:
        print("=> OCR/text-layer quality is the ceiling; find_tables was not the only issue.")
    else:
        print("=> worth a text-layer extractor (name/constituency/€) instead of find_tables.")


if __name__ == "__main__":
    main()
