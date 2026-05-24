"""
dbsect_listings_flatten.py

Flattens the bronze day-window debate-listing JSON into one silver
parquet row per (date, chamber, debate_section_id) - Stage 1 of the
debates integration (see pipeline_sandbox/dbsect_integration_plan.md s3).
Structural only: no AKN fetch, no speech parsing, no member resolution.

Input  (read-only):
  data/bronze/debates/listings/debates_listings_results.json
Output:
  data/silver/parquet/debate_listings.parquet

Flattening uses pandas.json_normalize (record_path + meta), the project
idiom shared with legislation.py and services/dbsect_harvest.py.
Composite identity is (date, chamber, debate_section_id); dbsect_2
recurs every sitting day, so never join on debate_section_id alone.

Run standalone (after the debates_listings scenario has produced bronze):
  python dbsect_listings_flatten.py
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from config import BRONZE_DIR, SILVER_PARQUET_DIR

logger = logging.getLogger(__name__)

_BRONZE = BRONZE_DIR / "debates" / "listings" / "debates_listings_results.json"
_OUT = SILVER_PARQUET_DIR / "debate_listings.parquet"

_AKN_BASE = "https://data.oireachtas.ie/akn/ie/debateRecord"
_WEB_BASE = "https://www.oireachtas.ie/en/debates/debate"


def _debate_records(path: Path) -> list[dict]:
    """Load bronze and return a flat list of debateRecord objects across
    all day-window pages. The page-concat is the only loop."""
    if not path.exists():
        logger.warning("dbsect_listings_flatten: %s not found", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [
        r["debateRecord"]
        for page in raw
        for r in (page.get("results") or [])
        if isinstance(r.get("debateRecord"), dict)
    ]


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Return df[name] as a string Series, or an all-NA string Series if
    the column is absent - json_normalize omits columns whose key never
    appears in any record."""
    if name in df.columns:
        return df[name].astype("string")
    return pd.Series(pd.NA, index=df.index, dtype="string")


def flatten_listings(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(
        records,
        record_path=["debateSections"],
        meta=["date", ["house", "houseCode"], ["house", "chamberType"],
              ["chamber", "uri"]],
        errors="ignore",
    )
    if df.empty:
        return pd.DataFrame()

    # debateSections elements may be wrapped ({"debateSection": {...}}) or
    # flat ({...}); strip the prefix so both shapes yield the same columns.
    df.columns = df.columns.str.replace(r"^debateSection\.", "", regex=True)

    # chamber: prefer house.houseCode, fall back to the last chamber.uri
    # segment; committee records resolve to '' and are dropped below.
    house_code = _col(df, "house.houseCode")
    uri_tail = (_col(df, "chamber.uri")
                .str.rstrip("/").str.rsplit("/", n=1).str[-1])
    chamber = house_code.where(house_code.isin(["dail", "seanad"]), uri_tail)
    chamber = chamber.where(chamber.isin(["dail", "seanad"]), "")
    chamber = chamber.mask(_col(df, "house.chamberType") == "committee", "")

    dbsect = _col(df, "debateSectionId")
    date = _col(df, "date")

    # bill_ref '<year>_<no>' from bill.uri, falling back to bill.event.uri.
    m_uri = _col(df, "bill.uri").str.extract(r"/bill/(\d+)/(\d+)")
    m_evt = _col(df, "bill.event.uri").str.extract(r"/bill/(\d+)/(\d+)")
    bill_ref = (m_uri[0].fillna(m_evt[0]) + "_" + m_uri[1].fillna(m_evt[1]))

    constructed_akn = (_AKN_BASE + "/" + chamber + "/" + date
                       + "/debate/mul@/" + dbsect + ".xml")
    akn = _col(df, "formats.xml.uri").fillna(constructed_akn)

    out = pd.DataFrame({
        "debate_section_id": dbsect,
        "date": date,
        "chamber": chamber,
        "parent_section_id": _col(df, "parentDebateSection.debateSectionId"),
        "parent_section_title": _col(df, "parentDebateSection.showAs"),
        "bill_ref": bill_ref,
        "debate_type": _col(df, "debateType"),
        "speaker_count": pd.to_numeric(
            _col(df, "counts.speakerCount"), errors="coerce").fillna(0).astype(int),
        "speech_count": pd.to_numeric(
            _col(df, "counts.speechCount"), errors="coerce").fillna(0).astype(int),
        "akn_xml_url": akn,
        # The public website path segment is the bare section number
        # (.../2026-04-23/63/), not the API's 'dbsect_63' id — strip the
        # prefix. Mirrors legislation.py's debate_url_web. (akn_xml_url
        # above keeps the full dbsect_ id — the AKN format does use it.)
        "debate_url_web": _WEB_BASE + "/" + chamber + "/" + date + "/"
                          + dbsect.str.replace("dbsect_", "", regex=False) + "/",
        "show_as": _col(df, "showAs"),
    })
    out = out[(out["chamber"] != "") & out["debate_section_id"].notna()]
    return out.drop_duplicates(subset=["date", "chamber", "debate_section_id"])


def run() -> int:
    df = flatten_listings(_debate_records(_BRONZE))
    if df.empty:
        logger.warning("dbsect_listings_flatten: no rows - run the "
                        "debates_listings scenario first")
        return 0
    logger.info("dbsect_listings_flatten: rows=%d distinct_dbsect=%d distinct_dates=%d",
                len(df), df["debate_section_id"].nunique(), df["date"].nunique())
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_OUT, index=False, compression="zstd", compression_level=3)
    logger.info("dbsect_listings_flatten: wrote %s", _OUT)
    return len(df)


if __name__ == "__main__":
    run()
