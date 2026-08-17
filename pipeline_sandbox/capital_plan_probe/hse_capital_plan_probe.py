"""Probe: can the HSE Capital Plan 2026 be parsed into (project, stage, euro) triples?

EXPERIMENTAL sandbox — feasibility check for a PublicSignal early-signal source, per
doc/PUBLISHED_MINUTES_AND_CAPITAL_PIPELINE_REGISTER.md's #2 recommendation. Fetches the
public PDF, extracts text with fitz, and measures how much of the document carries a
project name + explicit stage keyword + euro value close together — the shape PublicSignal
would need for a "pre-tender lead" row. Writes nothing to data/, no gold, no promotion.

Source: https://about.hse.ie/publications/hse-capital-plan-2026/
PDF   : https://www.drugsandalcohol.ie/45231/1/HSE_Capital_Plan_2026.pdf (stable mirror)

Usage: python hse_capital_plan_probe.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import fitz
import requests

HERE = Path(__file__).resolve().parent
CORPUS = HERE / "corpus"
PDF_URL = "https://www.drugsandalcohol.ie/45231/1/HSE_Capital_Plan_2026.pdf"
PDF_PATH = CORPUS / "HSE_Capital_Plan_2026.pdf"
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}

EURO_RE = re.compile(r"€\s?[\d,]+(?:\.\d+)?\s?(?:m|million|bn|billion|k)?", re.I)
STAGE_KEYWORDS = [
    "tender phase", "tender", "construction phase", "construction",
    "enabling works", "design phase", "design stage", "planning phase",
    "business case", "procurement", "completed", "completion",
    "on site", "handover", "commissioning",
]
STAGE_RE = re.compile("|".join(re.escape(k) for k in STAGE_KEYWORDS), re.I)


def fetch_pdf() -> bytes:
    if PDF_PATH.exists():
        return PDF_PATH.read_bytes()
    CORPUS.mkdir(parents=True, exist_ok=True)
    r = requests.get(PDF_URL, headers=HDRS, timeout=60)
    r.raise_for_status()
    PDF_PATH.write_bytes(r.content)
    return r.content


def main() -> int:
    raw = fetch_pdf()
    doc = fitz.open(stream=raw, filetype="pdf")
    n_pages = len(doc)
    lines: list[str] = []
    for page in doc:
        text = page.get_text()
        lines.extend(ln.strip() for ln in text.split("\n") if ln.strip())
    doc.close()

    euro_lines = [ln for ln in lines if EURO_RE.search(ln)]
    stage_lines = [ln for ln in lines if STAGE_RE.search(ln)]
    both_lines = [ln for ln in lines if EURO_RE.search(ln) and STAGE_RE.search(ln)]

    # Windowed pairing: does a euro-bearing line have a stage keyword within +-2 lines?
    windowed_hits = []
    for i, ln in enumerate(lines):
        if not EURO_RE.search(ln):
            continue
        window = lines[max(0, i - 2): i + 3]
        if any(STAGE_RE.search(w) for w in window):
            windowed_hits.append({"line": ln, "window": window})

    report = {
        "pdf_bytes": len(raw),
        "n_pages": n_pages,
        "n_lines": len(lines),
        "n_euro_lines": len(euro_lines),
        "n_stage_lines": len(stage_lines),
        "n_same_line_euro_and_stage": len(both_lines),
        "n_euro_lines_with_stage_within_2": len(windowed_hits),
        "same_line_rate_of_euro_lines": round(len(both_lines) / max(1, len(euro_lines)), 3),
        "windowed_rate_of_euro_lines": round(len(windowed_hits) / max(1, len(euro_lines)), 3),
        "sample_same_line": both_lines[:15],
        "sample_windowed": windowed_hits[:15],
        "sample_euro_lines_no_stage_nearby": [
            ln for ln in euro_lines
            if ln not in {h["line"] for h in windowed_hits}
        ][:15],
    }
    (HERE / "hse_probe_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"pages={n_pages} lines={len(lines)} euro_lines={len(euro_lines)} "
          f"stage_lines={len(stage_lines)}")
    print(f"same-line euro+stage: {len(both_lines)}/{len(euro_lines)} "
          f"({report['same_line_rate_of_euro_lines']:.1%})")
    print(f"windowed (+-2 lines) euro+stage: {len(windowed_hits)}/{len(euro_lines)} "
          f"({report['windowed_rate_of_euro_lines']:.1%})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
