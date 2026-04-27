"""Static logic-firewall checker for Dáil Tracker Streamlit pages.

Usage:
    python tools/check_streamlit_logic_firewall.py utility/pages_code/interests.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

FORBIDDEN_PATTERNS = {
    "pandas/polars merge": r"\.merge\s*\(",
    "dataframe join": r"\.join\s*\(",
    "groupby/group_by": r"\.(groupby|group_by)\s*\(",
    "pivot": r"\.pivot(_table)?\s*\(",
    "raw parquet scan": r"(read_parquet|parquet_scan)\s*\(",
    "PDF parsing": r"\b(fitz|PyMuPDF|pdfplumber|camelot)\b",
    "API call": r"\b(requests|httpx)\b",
    "hardcoded Windows path": r"[A-Za-z]:\\\\Users\\\\",
    "SQL JOIN": r"\bJOIN\b",
    "SQL GROUP BY": r"\bGROUP\s+BY\b",
    "SQL CREATE": r"\bCREATE\s+(VIEW|TABLE)\b",
    "SQL DELETE/UPDATE/INSERT": r"\b(DELETE|UPDATE|INSERT)\b",
}


def check_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8", errors="ignore")
    failures = []
    for label, pattern in FORBIDDEN_PATTERNS.items():
        for match in re.finditer(pattern, text, re.IGNORECASE):
            line_no = text.count("\n", 0, match.start()) + 1
            line = text.splitlines()[line_no - 1].strip()
            failures.append((label, line_no, line))

    if not failures:
        print(f"PASS: {path}")
        return 0

    print(f"FAIL: {path}")
    for label, line_no, line in failures:
        print(f"  - {label} at line {line_no}: {line}")
    return 1


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python tools/check_streamlit_logic_firewall.py <file.py> [<file.py> ...]")
        return 2

    status = 0
    for raw in argv[1:]:
        status |= check_file(Path(raw))
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
