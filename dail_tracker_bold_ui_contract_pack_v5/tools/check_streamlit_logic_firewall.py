from __future__ import annotations

import re
import sys
from pathlib import Path

FORBIDDEN_PATTERNS = {
    "read_parquet": r"\bread_parquet\s*\(",
    "parquet_scan": r"\bparquet_scan\s*\(",
    "create_view": r"\bCREATE\s+VIEW\b",
    "create_table": r"\bCREATE\s+TABLE\b",
    "hardcoded_windows_path": r"[A-Za-z]:\\Users\\",
    "pandas_merge": r"\.merge\s*\(",
    "pandas_groupby": r"\.groupby\s*\(",
    "pandas_pivot": r"\.pivot",
    "polars_join": r"\.join\s*\(",
    "polars_group_by": r"\.group_by\s*\(",
    "pdf_parsing": r"\bfitz\.|import\s+fitz|PyMuPDF",
    "api_call": r"\brequests\.",
}

SQL_FORBIDDEN = {
    "sql_join": r"\bJOIN\b",
    "sql_group_by": r"\bGROUP\s+BY\b",
    "sql_having": r"\bHAVING\b",
    "sql_window_over": r"\bOVER\s*\(",
}


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python tools/check_streamlit_logic_firewall.py <file_or_directory>")
        return 2

    target = Path(sys.argv[1])
    files = [target] if target.is_file() else list(target.rglob("*.py"))

    failed = False
    for file in files:
        text = file.read_text(encoding="utf-8", errors="ignore")
        checks = dict(FORBIDDEN_PATTERNS)
        checks.update(SQL_FORBIDDEN)
        for name, pattern in checks.items():
            if re.search(pattern, text, flags=re.IGNORECASE):
                print(f"FAIL {file}: {name} matched `{pattern}`")
                failed = True

    if failed:
        return 1

    print("PASS: no Streamlit logic firewall violations found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
