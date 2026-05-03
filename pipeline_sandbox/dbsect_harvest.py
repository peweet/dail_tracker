"""
pipeline_sandbox/dbsect_harvest.py

Local-only harvester. Walks the bronze JSON we already have for
legislation, questions, and votes, and emits a deduplicated index of
every distinct dbsect_* identifier we have seen, with its provenance.

No API calls. No modification of pipeline.py / enrich.py / legislation.py.

Output:
  data/silver/parquet/dbsect_index.parquet

Schema (one row per (debate_section_id, source, source_key)):

  debate_section_id : str   e.g. 'dbsect_12'
  source            : str   'bill' | 'question' | 'vote'
  source_key        : str   bill_id | question_uri | vote_id
  date              : str   ISO date as string ('YYYY-MM-DD'), nullable
  chamber           : str   'dail' | 'seanad' | '', derived from chamber.uri
  debate_uri        : str   raw debate.uri from the source row, nullable
  debate_title      : str   showAs text from the source row, nullable

The index is the worklist for two follow-on tasks:

  1. Lane A — call /v1/debates?debate_id=<dbsect> per row to backfill the
     full debate payload for every debate already linked from a
     bill / question / vote.
  2. Lane B — call /v1/debates?member_id=<TD> per sitting member; the
     dbsect ids returned join back through this index to bills /
     questions / votes the member contributed to.

Run standalone:
  python pipeline_sandbox/dbsect_harvest.py

This script is intentionally read-only over bronze. It does not mutate
the JSON. The only write is the silver parquet.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from config import BRONZE_DIR, LEGISLATION_DIR, SILVER_PARQUET_DIR, VOTES_DIR  # noqa: E402

_LEG_JSON = LEGISLATION_DIR / "legislation_results.json"
_QUE_JSON = BRONZE_DIR / "questions" / "questions_results.json"
_VOT_JSON = VOTES_DIR / "votes_results.json"

_OUT = SILVER_PARQUET_DIR / "dbsect_index.parquet"


def _load(path: Path) -> list:
    if not path.exists():
        print(f"[skip] {path} not found")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _chamber_short(uri: str | None) -> str:
    """Return 'dail' / 'seanad' / '' from a chamber-or-house URI.

    Two URI shapes occur in bronze:
      def/house/dail              -> last segment is the chamber name
      house/dail/34               -> chamber name is the segment after 'house'
    Plus committee debates (def/committee) which have no chamber name.
    """
    if not uri:
        return ""
    parts = str(uri).rstrip("/").split("/")
    if "house" in parts:
        idx = parts.index("house")
        if idx + 1 < len(parts):
            tok = parts[idx + 1]
            if tok in {"dail", "seanad"}:
                return tok
    last = parts[-1] if parts else ""
    return last if last in {"dail", "seanad"} else ""


def _norm_dbsect(value: str | None) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s if s.startswith("dbsect_") else f"dbsect_{s}"


def harvest_bills(pages: list) -> list[dict]:
    rows: list[dict] = []
    for page in pages:
        for r in page.get("results", []) or []:
            bill = r.get("bill") or {}
            bill_year = bill.get("billYear")
            bill_no = bill.get("billNo")
            if bill_year is None or bill_no is None:
                continue
            bill_id = f"{bill_year}_{bill_no}"
            for d in bill.get("debates") or []:
                dbsect = _norm_dbsect(d.get("debateSectionId"))
                if not dbsect:
                    continue
                rows.append(
                    {
                        "debate_section_id": dbsect,
                        "source": "bill",
                        "source_key": bill_id,
                        "date": d.get("date"),
                        "chamber": _chamber_short((d.get("chamber") or {}).get("uri")),
                        "debate_uri": d.get("uri"),
                        "debate_title": d.get("showAs"),
                    }
                )
    return rows


def harvest_questions(pages: list) -> list[dict]:
    rows: list[dict] = []
    for page in pages:
        for r in page.get("results", []) or []:
            q = r.get("question") or {}
            ds = q.get("debateSection") or {}
            dbsect = _norm_dbsect(ds.get("debateSectionId"))
            if not dbsect:
                continue
            rows.append(
                {
                    "debate_section_id": dbsect,
                    "source": "question",
                    "source_key": q.get("uri") or "",
                    "date": q.get("date"),
                    "chamber": _chamber_short((q.get("house") or {}).get("uri")),
                    "debate_uri": ds.get("uri"),
                    "debate_title": ds.get("showAs"),
                }
            )
    return rows


def harvest_votes(pages: list) -> list[dict]:
    rows: list[dict] = []
    for page in pages:
        for r in page.get("results", []) or []:
            div = r.get("division") or {}
            debate = div.get("debate") or {}
            dbsect = _norm_dbsect(debate.get("debateSection"))
            if not dbsect:
                continue
            rows.append(
                {
                    "debate_section_id": dbsect,
                    "source": "vote",
                    "source_key": str(div.get("voteId") or ""),
                    "date": div.get("date"),
                    "chamber": _chamber_short((div.get("chamber") or {}).get("uri")),
                    "debate_uri": debate.get("uri"),
                    "debate_title": debate.get("showAs"),
                }
            )
    return rows


def _summary(df: pl.DataFrame) -> None:
    if df.is_empty():
        print("  (empty)")
        return
    by_source = (
        df.group_by("source")
        .agg(
            pl.len().alias("rows"),
            pl.col("debate_section_id").n_unique().alias("distinct_dbsect"),
        )
        .sort("source")
    )
    print("  rows + distinct dbsect per source:")
    for row in by_source.iter_rows(named=True):
        print(
            f"    {row['source']:<8}  rows={row['rows']:<7}  "
            f"distinct_dbsect={row['distinct_dbsect']}"
        )

    distinct_total = df.get_column("debate_section_id").n_unique()
    print(f"  distinct dbsect across all sources: {distinct_total}")

    overlap = (
        df.select("debate_section_id", "source")
        .unique()
        .group_by("debate_section_id")
        .agg(pl.col("source").n_unique().alias("source_count"))
        .group_by("source_count")
        .agg(pl.len().alias("dbsect_count"))
        .sort("source_count")
    )
    print("  source overlap (how many sources cite the same dbsect):")
    for row in overlap.iter_rows(named=True):
        print(
            f"    cited_by={row['source_count']} source(s)  "
            f"dbsect_count={row['dbsect_count']}"
        )


def run() -> None:
    print("Harvesting dbsect identifiers from bronze ...")
    print(f"  legislation: {_LEG_JSON}")
    print(f"  questions  : {_QUE_JSON}")
    print(f"  votes      : {_VOT_JSON}")

    rows: list[dict] = []
    rows.extend(harvest_bills(_load(_LEG_JSON)))
    rows.extend(harvest_questions(_load(_QUE_JSON)))
    rows.extend(harvest_votes(_load(_VOT_JSON)))

    if not rows:
        print("No dbsect rows harvested. Did you run the bronze fetches?")
        return

    df = pl.DataFrame(rows).unique(
        subset=["debate_section_id", "source", "source_key"]
    )

    _summary(df)

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(_OUT)
    print(f"  -> {_OUT}  ({len(df)} rows)")


if __name__ == "__main__":
    run()
