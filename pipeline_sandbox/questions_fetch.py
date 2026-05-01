"""
pipeline_sandbox/questions_fetch.py — ISOLATED SANDBOX SCRIPT

Fetches all parliamentary questions for the 34th Dáil from the Oireachtas API,
flattens the JSON into a tidy Polars DataFrame, and writes it to:

  data/bronze/questions/questions_raw.json  <- full raw API pages
  data/gold/parquet/questions.parquet       <- flat, keyed by unique_member_code
  data/gold/csv/questions_preview.csv       <- first 500 rows for QA

Each row is one question. Key columns:
  unique_member_code  — matches flattened_members.csv for joins
  td_name             — TD who asked the question
  question_date       — date of the sitting
  question_type       — 'oral' or 'written'
  ministry            — Minister/Department being questioned
  topic               — debateSection topic label
  question_text       — full question text (showAs)
  question_number     — sequential number on that day's paper
  question_ref        — e.g. [31202/26] reference number

DO NOT import or call this from any existing pipeline file.
Run independently: python pipeline_sandbox/questions_fetch.py
Validate the preview CSV before treating the output as authoritative.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
import requests

_ROOT        = Path(__file__).resolve().parents[1]
_BRONZE_OUT  = _ROOT / "data" / "bronze" / "questions"
_GOLD_PARQ   = _ROOT / "data" / "gold" / "parquet" / "questions.parquet"
_PREVIEW_CSV = _ROOT / "data" / "gold" / "csv" / "questions_preview.csv"

_BRONZE_OUT.mkdir(parents=True, exist_ok=True)
(_ROOT / "data" / "gold" / "parquet").mkdir(parents=True, exist_ok=True)
(_ROOT / "data" / "gold" / "csv").mkdir(parents=True, exist_ok=True)

API_BASE    = "https://api.oireachtas.ie/v1"
DATE_START  = "2024-11-01"   # 34th Dáil convened Nov 2024
DATE_END    = "2099-01-01"
LIMIT       = 200
PAUSE_S     = 0.3            # polite delay between pages


def _fetch_page(skip: int) -> dict:
    url = (
        f"{API_BASE}/questions"
        f"?date_start={DATE_START}"
        f"&date_end={DATE_END}"
        f"&chamber=dail"
        f"&limit={LIMIT}"
        f"&skip={skip}"
    )
    resp = requests.get(url, timeout=(10, 60))
    resp.raise_for_status()
    return resp.json()


def fetch_all_pages() -> list[dict]:
    """Paginate through all questions, return list of raw result items."""
    all_items: list[dict] = []
    skip = 0
    page = 0

    while True:
        page += 1
        print(f"  page {page} (skip={skip}) …", end=" ", flush=True)
        data = _fetch_page(skip)

        head   = data.get("head", {})
        counts = head.get("counts", {})
        total  = int(counts.get("resultCount", 0))
        items  = data.get("results", [])

        if not items:
            print("empty — done.")
            break

        all_items.extend(items)
        print(f"got {len(items)} | cumulative {len(all_items)} / {total}")

        if len(all_items) >= total:
            break

        skip += LIMIT
        time.sleep(PAUSE_S)

    return all_items


def _safe_str(val) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _extract_row(item: dict) -> dict:
    """Flatten one API result item into a dict of scalar values."""
    q = item.get("question", item)  # some API versions nest under 'question'

    by      = q.get("by", {}) or {}
    to      = q.get("to", {}) or {}
    section = q.get("debateSection", {}) or {}
    house   = q.get("house", {}) or {}
    show_as = _safe_str(q.get("showAs", ""))

    # Extract reference number like [31202/26] from showAs text
    ref = ""
    if "[" in show_as and "]" in show_as:
        start = show_as.rfind("[")
        end   = show_as.rfind("]")
        if start < end:
            ref = show_as[start:end + 1]

    return {
        "unique_member_code": _safe_str(by.get("memberCode", "")),
        "td_name":            _safe_str(by.get("showAs", "")),
        "question_date":      _safe_str(q.get("date", "")),
        "question_type":      _safe_str(q.get("questionType", "")),
        "ministry":           _safe_str(to.get("showAs", "")),
        "topic":              _safe_str(section.get("showAs", "")),
        "question_text":      show_as,
        "question_number":    _safe_str(q.get("questionNumber", "")),
        "question_ref":       ref,
        "debate_section_id":  _safe_str(q.get("debateSectionId", "")),
        "uri":                _safe_str(q.get("uri", "")),
        "house":              _safe_str(house.get("showAs", "")),
    }


def _probe_schema(items: list[dict]) -> None:
    """Print a sample item so we can verify the schema matches expectations."""
    if not items:
        return
    sample = items[0]
    print("\n--- Schema probe (first item keys) ---")
    print(json.dumps(sample, indent=2, ensure_ascii=False)[:1200])
    print("--------------------------------------\n")


def run() -> None:
    print(f"Fetching questions from {DATE_START} onwards …")
    items = fetch_all_pages()

    if not items:
        print("No results returned — check the API or date range.")
        return

    # Save raw JSON for reproducibility
    raw_path = _BRONZE_OUT / "questions_raw.json"
    raw_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nRaw JSON saved → {raw_path} ({len(items)} items)")

    _probe_schema(items)

    rows = [_extract_row(item) for item in items]
    df = pl.DataFrame(rows)

    # Basic QA
    total      = len(df)
    matched    = df.filter(pl.col("unique_member_code") != "").height
    unmatched  = total - matched
    oral       = df.filter(pl.col("question_type") == "oral").height
    written    = df.filter(pl.col("question_type") == "written").height

    print(f"Rows: {total:,}")
    print(f"  member code matched : {matched:,}")
    print(f"  member code missing : {unmatched:,}")
    print(f"  oral questions      : {oral:,}")
    print(f"  written questions   : {written:,}")

    if unmatched > 0:
        print("\nSample unmatched:")
        print(df.filter(pl.col("unique_member_code") == "").head(5).select(["td_name", "question_date"]))

    df.write_parquet(_GOLD_PARQ)
    print(f"\nParquet → {_GOLD_PARQ}")

    df.head(500).write_csv(_PREVIEW_CSV)
    print(f"Preview CSV → {_PREVIEW_CSV}")
    print("\nDone. Validate preview CSV before treating output as authoritative.")


if __name__ == "__main__":
    run()
