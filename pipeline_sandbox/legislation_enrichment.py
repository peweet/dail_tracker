"""
pipeline_sandbox/legislation_enrichment.py

Produces parquet files that the main legislation.py pipeline run has not yet written:
  - data/silver/parquet/related_docs.parquet   (Explanatory Memoranda + related docs)
  - data/silver/parquet/versions.parquet       (bill text versions)

Also enriches existing parquet files with derived columns:
  - debates.parquet  → adds debate_url_web (human-readable oireachtas.ie URL)

Uses the same json_normalize(record_path, meta) approach as legislation.py.
These files are picked up automatically by analytics_loading.py on next Streamlit start.

Run standalone:
  python pipeline_sandbox/legislation_enrichment.py

Do not modify legislation.py or pipeline.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import polars as pl

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from config import LEGISLATION_DIR, SILVER_PARQUET_DIR  # noqa: E402

_JSON = LEGISLATION_DIR / "legislation_results.json"

BILL_META = [
    ["billSort", "billShortTitleEnSort"],
    ["billSort", "billYearSort"],
    ["bill", "billNo"],
    ["bill", "billYear"],
    ["bill", "billType"],
    ["bill", "shortTitleEn"],
    ["bill", "longTitleEn"],
    ["bill", "lastUpdated"],
    ["bill", "status"],
    ["bill", "source"],
    ["bill", "method"],
    ["bill", "mostRecentStage", "event", "showAs"],
    ["bill", "mostRecentStage", "event", "progressStage"],
    ["bill", "mostRecentStage", "event", "stageCompleted"],
    ["bill", "mostRecentStage", "event", "house", "showAs"],
    "contextDate",
]


def _load_bills() -> list:
    data = pd.read_json(_JSON, encoding="utf-8")
    bills = []
    for page in data["results"]:
        bills.extend(page)
    return bills


def _normalize(bills: list, record_path: list, label: str) -> pd.DataFrame:
    df = pd.json_normalize(bills, record_path=record_path, meta=BILL_META, errors="ignore")
    print(f"  {label}: {len(df)} rows, {len(df.columns)} columns")
    return df


def run() -> None:
    if not _JSON.exists():
        print(f"[skip] {_JSON} not found — run the legislation fetch pipeline first")
        return

    print(f"Loading bills from {_JSON.name} …")
    bills = _load_bills()
    print(f"  {len(bills)} bill records loaded")

    # ── Related docs (Explanatory Memoranda, etc.) ─────────────────────────────
    related_docs_df = _normalize(bills, ["bill", "relatedDocs"], "related_docs")
    out = SILVER_PARQUET_DIR / "related_docs.parquet"
    related_docs_df.to_parquet(out, index=False)
    print(f"  → {out}")

    # ── Bill text versions ─────────────────────────────────────────────────────
    versions_df = _normalize(bills, ["bill", "versions"], "versions")
    out = SILVER_PARQUET_DIR / "versions.parquet"
    versions_df.to_parquet(out, index=False)
    print(f"  → {out}")

    # ── Enrich debates with human-readable URL ─────────────────────────────────
    _enrich_debates(SILVER_PARQUET_DIR / "debates.parquet")

    print("Done.")


def _enrich_debates(path: Path) -> None:
    """Add debate_url_web to debates.parquet.

    URL pattern: https://www.oireachtas.ie/en/debates/debate/{chamber}/{date}/{section}/
    - chamber : last path segment of chamber.uri  (e.g. 'dail' or 'seanad')
    - date    : debate date as YYYY-MM-DD string
    - section : debateSectionId with 'dbsect_' stripped  (e.g. '23')
    """
    if not path.exists():
        print(f"  [skip] {path.name} not found")
        return

    df = pl.read_parquet(path)
    df = df.with_columns(
        (
            pl.lit("https://www.oireachtas.ie/en/debates/debate/")
            + pl.col("chamber.uri").str.split("/").list.last()
            + pl.lit("/")
            + pl.col("date").cast(pl.String)
            + pl.lit("/")
            + pl.col("debateSectionId").str.replace("dbsect_", "", literal=True)
            + pl.lit("/")
        ).alias("debate_url_web")
    )
    df.write_parquet(path)
    print(f"  enriched debates.parquet → debate_url_web added ({len(df)} rows)")


if __name__ == "__main__":
    run()
