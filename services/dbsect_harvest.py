"""
services/dbsect_harvest.py

Local-only harvester. Flattens the bronze JSON already fetched for
legislation, questions, and votes into a deduplicated index of every
distinct dbsect_* identifier seen, with its provenance.

No API calls. Reads bronze JSON, writes one silver parquet:
  data/silver/parquet/dbsect_index.parquet

Schema (one row per (debate_section_id, source, source_key)):
  debate_section_id : str   e.g. 'dbsect_12'
  source            : str   'bill' | 'question' | 'vote'
  source_key        : str   bill_id | question_uri | vote_id
  date              : str   ISO date string, nullable
  chamber           : str   'dail' | 'seanad' | ''
  debate_uri        : str   raw debate.uri, nullable
  debate_title      : str   showAs text, nullable

Flattening uses pandas.json_normalize (record_path + meta), mirroring
legislation.py's silver flattener - the project idiom for turning nested
Oireachtas bronze JSON into tabular silver. dbsect ids are per-day, not
global: composite identity is (date, chamber, debate_section_id);
downstream joins must respect that.

Graduated from pipeline_sandbox/dbsect_harvest.py. Called by
services/oireachtas_api_main.main() (STEP 4.5).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from services.dail_config import (
    LEGISLATION_DIR,
    QUESTIONS_DIR,
    SILVER_PARQUET_DIR,
    VOTES_DIR,
)

logger = logging.getLogger(__name__)

_LEG_JSON = LEGISLATION_DIR / "legislation_results.json"
_QUE_JSON = QUESTIONS_DIR / "questions_results.json"
_VOT_JSON = VOTES_DIR / "votes_results.json"
_OUT = SILVER_PARQUET_DIR / "dbsect_index.parquet"

_SCHEMA = [
    "debate_section_id",
    "source",
    "source_key",
    "date",
    "chamber",
    "debate_uri",
    "debate_title",
]


def _records(path: Path) -> list[dict]:
    """Load a bronze results JSON and concatenate every page's `results`
    into one flat list of records. The page-concat is the only loop -
    it mirrors legislation.py; the flattening itself is json_normalize."""
    if not path.exists():
        logger.warning("dbsect_harvest: %s not found - skipping", path)
        return []
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return [r for page in raw for r in (page.get("results") or [])]


def _chamber_from_uri(uri: pd.Series) -> pd.Series:
    """Vectorised 'dail'/'seanad'/'' extraction from a house/chamber URI.

    Both observed URI shapes - `.../house/dail` and `.../house/dail/34`
    - carry the chamber as the segment right after `house/`. Committee
    URIs have no `house/<chamber>` segment and resolve to ''."""
    return uri.astype("string").str.extract(r"house/(dail|seanad)(?:/|$)", expand=False).fillna("")


def _norm_dbsect(col: pd.Series) -> pd.Series:
    """Vectorised: ensure every id carries the `dbsect_` prefix; blanks -> NA."""
    s = col.astype("string").str.strip()
    s = s.mask(s == "", pd.NA)
    return s.where(s.isna() | s.str.startswith("dbsect_"), "dbsect_" + s)


def harvest_bills(records: list[dict]) -> pd.DataFrame:
    """One row per bill->debate edge. record_path/meta mirror legislation.py."""
    if not records:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.json_normalize(
        records,
        record_path=["bill", "debates"],
        meta=[["bill", "billYear"], ["bill", "billNo"]],
        errors="ignore",
    )
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA)
    df = df.dropna(subset=["bill.billYear", "bill.billNo"])
    return pd.DataFrame(
        {
            "debate_section_id": _norm_dbsect(df["debateSectionId"]),
            "source": "bill",
            "source_key": (
                df["bill.billYear"].astype("Int64").astype("string")
                + "_"
                + df["bill.billNo"].astype("Int64").astype("string")
            ),
            "date": df["date"].astype("string"),
            "chamber": _chamber_from_uri(df["chamber.uri"]),
            "debate_uri": df["uri"].astype("string"),
            "debate_title": df["showAs"].astype("string"),
        }
    )


def harvest_questions(records: list[dict]) -> pd.DataFrame:
    """One row per question. debateSection is a single object, not an
    array - a flat json_normalize (no record_path) is the right call."""
    if not records:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.json_normalize(records, errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA)
    return pd.DataFrame(
        {
            "debate_section_id": _norm_dbsect(df["question.debateSection.debateSectionId"]),
            "source": "question",
            "source_key": df["question.uri"].astype("string"),
            "date": df["question.date"].astype("string"),
            "chamber": _chamber_from_uri(df["question.house.uri"]),
            "debate_uri": df["question.debateSection.uri"].astype("string"),
            "debate_title": df["question.debateSection.showAs"].astype("string"),
        }
    )


def harvest_votes(records: list[dict]) -> pd.DataFrame:
    """One row per division. division.debate.debateSection is the bare
    dbsect id string."""
    if not records:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.json_normalize(records, errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA)
    return pd.DataFrame(
        {
            "debate_section_id": _norm_dbsect(df["division.debate.debateSection"]),
            "source": "vote",
            "source_key": df["division.voteId"].astype("string"),
            "date": df["division.date"].astype("string"),
            "chamber": _chamber_from_uri(df["division.chamber.uri"]),
            "debate_uri": df["division.debate.uri"].astype("string"),
            "debate_title": df["division.debate.showAs"].astype("string"),
        }
    )


def harvest_dbsect_index() -> int:
    """Harvest dbsect identifiers from bronze JSON into dbsect_index.parquet.

    Returns the number of rows written (0 if no bronze JSON was found).
    """
    logger.info("dbsect_harvest: legislation=%s", _LEG_JSON)
    logger.info("dbsect_harvest: questions  =%s", _QUE_JSON)
    logger.info("dbsect_harvest: votes      =%s", _VOT_JSON)

    df = pd.concat(
        [
            harvest_bills(_records(_LEG_JSON)),
            harvest_questions(_records(_QUE_JSON)),
            harvest_votes(_records(_VOT_JSON)),
        ],
        ignore_index=True,
    )
    df = df.dropna(subset=["debate_section_id"])
    df = df.drop_duplicates(subset=["debate_section_id", "source", "source_key"])

    if df.empty:
        logger.warning("dbsect_harvest: no rows harvested - bronze fetches may not have run")
        return 0

    counts = df.groupby("source")["debate_section_id"].agg(rows="size", distinct_dbsect="nunique")
    logger.info("dbsect_harvest: counts by source\n%s", counts)
    logger.info("dbsect_harvest: distinct dbsect total=%d", df["debate_section_id"].nunique())

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_OUT, index=False, compression="zstd", compression_level=3)
    logger.info("dbsect_harvest: wrote %s (%d rows)", _OUT, len(df))
    return len(df)


if __name__ == "__main__":
    from services.logging_setup import setup_logging

    setup_logging()
    harvest_dbsect_index()
