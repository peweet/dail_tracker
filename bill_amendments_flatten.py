"""Flatten bill amendmentLists into a tabular silver parquet.

Reads the unscoped bronze legislation JSON (same source as legislation.py)
and emits data/silver/parquet/bill_amendments.parquet — one row per
amendment list (numberedList / creamList) with its PDF URL and stage
metadata.

Output schema:
    bill_year             str   e.g. "2017"
    bill_no               str   e.g. "23"
    bill_id               str   "{bill_year}_{bill_no}"
    amendment_type        str   "numberedList" | "creamList"
    chamber               str   "dail" | "seanad"
    stage_no              str   API stage number
    stage_show_as         str   "Committee Stage", "Report Stage", ...
    show_as               str   list label, e.g. "Numbered List [Dáil]"
    amendment_date        date  date the list was published
    pdf_url               str   PDF URI on data.oireachtas.ie
    bill_short_title_en   str   carried from bill metadata for joins
    bill_type             str
    bill_status           str

Usage:
    python bill_amendments_flatten.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from config import LEGISLATION_DIR, SILVER_PARQUET_DIR

BILL_META = [
    ["bill", "billNo"],
    ["bill", "billYear"],
    ["bill", "billType"],
    ["bill", "shortTitleEn"],
    ["bill", "status"],
]


def _chamber_slug(uri: str | None) -> str | None:
    if not uri:
        return None
    return uri.rstrip("/").rsplit("/", 1)[-1].lower() or None


def _amend_type_slug(uri: str | None) -> str | None:
    if not uri:
        return None
    return uri.rstrip("/").rsplit("/", 1)[-1] or None


def main() -> int:
    src = LEGISLATION_DIR / "legislation_results_unscoped.json"
    if not src.exists():
        print(f"ERROR: source not found: {src}", file=sys.stderr)
        return 1

    raw = json.loads(src.read_text(encoding="utf-8"))
    pages = raw if isinstance(raw, list) else [raw]
    bills: list[dict] = []
    for page in pages:
        bills.extend(page.get("results", []))

    df = pd.json_normalize(
        bills,
        record_path=["bill", "amendmentLists"],
        meta=BILL_META,
        errors="ignore",
    )

    if df.empty:
        print("No amendmentLists found in source — nothing to write.")
        return 0

    df = df.rename(columns={
        "amendmentList.date": "amendment_date",
        "amendmentList.showAs": "show_as",
        "amendmentList.stageNo": "stage_no",
        "amendmentList.stage.showAs": "stage_show_as",
        "amendmentList.formats.pdf.uri": "pdf_url",
        "amendmentList.amendmentTypeUri.uri": "_amend_type_uri",
        "amendmentList.chamber.uri": "_chamber_uri",
        "bill.billNo": "bill_no",
        "bill.billYear": "bill_year",
        "bill.billType": "bill_type",
        "bill.shortTitleEn": "bill_short_title_en",
        "bill.status": "bill_status",
    })

    df["amendment_type"] = df["_amend_type_uri"].map(_amend_type_slug)
    df["chamber"] = df["_chamber_uri"].map(_chamber_slug)
    df["bill_id"] = df["bill_year"].astype(str) + "_" + df["bill_no"].astype(str)
    df["amendment_date"] = pd.to_datetime(df["amendment_date"], errors="coerce").dt.date

    out = df[[
        "bill_year", "bill_no", "bill_id",
        "amendment_type", "chamber", "stage_no", "stage_show_as", "show_as",
        "amendment_date", "pdf_url",
        "bill_short_title_en", "bill_type", "bill_status",
    ]].copy()

    out = out.dropna(subset=["pdf_url"]).reset_index(drop=True)

    SILVER_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    target = SILVER_PARQUET_DIR / "bill_amendments.parquet"
    out.to_parquet(target, index=False, compression="zstd", compression_level=3)

    by_type = out["amendment_type"].value_counts().to_dict()
    by_chamber = out["chamber"].value_counts().to_dict()
    print(f"Wrote {target}  rows={len(out)}  bills={out['bill_id'].nunique()}")
    print(f"  by amendment_type: {by_type}")
    print(f"  by chamber:        {by_chamber}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
