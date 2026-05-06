"""
pipeline_sandbox/dbsect_listings_flatten.py

Flattens the bronze day-window debate listings JSON into one silver
parquet row per (date, chamber, debate_section_id). Stage 1 of the
debates integration plan — see pipeline_sandbox/dbsect_integration_plan.md
§3 for the schema and §10 for why this is "structural only" (no AKN
fetch, no speech parsing, no member resolution).

Inputs (read-only):
  data/bronze/debates/listings/debates_listings_results.json
    Each element is one /v1/debates day-window response. The combined
    JSON is what services/run_member_scenario writes for this scenario,
    matching the legislation_results.json / questions_results.json
    pattern used elsewhere in the pipeline.

Output:
  data/silver/parquet/debate_listings.parquet

Schema (per plan §3, one row per (date, chamber, debate_section_id)):
  debate_section_id, date, chamber,
  parent_section_id, parent_section_title,
  bill_ref, debate_type,
  speaker_count, speech_count,
  akn_xml_url, debate_url_web

Identity rule: composite (date, chamber, debate_section_id). dbsect_2
recurs every sitting day, so never join on debate_section_id alone.

Run standalone (after services/oireachtas_api_main has produced the
bronze JSON via the debates_listings scenario):

  python pipeline_sandbox/dbsect_listings_flatten.py

This script lives in sandbox per the project pipeline_sandbox rule. It
will graduate alongside dbsect_harvest.py once Stage 1 has been stable
in production for a refresh cycle or two — see plan §10.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from config import BRONZE_DIR, SILVER_PARQUET_DIR  # noqa: E402

_BRONZE = BRONZE_DIR / "debates" / "listings" / "debates_listings_results.json"
_OUT = SILVER_PARQUET_DIR / "debate_listings.parquet"

_BILL_URI_RE = re.compile(r"/bill/(\d+)/(\d+)(?:/|$)")


def _load_bronze() -> list[dict]:
    if not _BRONZE.exists():
        print(f"[skip] {_BRONZE} not found — run the debates_listings scenario first")
        return []
    # API responses can carry non-ASCII (Irish-language titles) — explicit utf-8.
    with open(_BRONZE, "r", encoding="utf-8") as f:
        return json.load(f)


def _bill_ref(bill: dict | None) -> str | None:
    """Extract '<year>_<no>' from a bill object, or None if absent.

    The day-window response gives `bill.uri` like
    .../bill/2026/20 (with optional trailing /<chamber>/<stage>). The
    matching key in dbsect_index.source_key for source='bill' is
    f'{billYear}_{billNo}', so this normaliser produces the same shape.
    """
    if not bill:
        return None
    uri = bill.get("uri") or ""
    m = _BILL_URI_RE.search(uri)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    event = bill.get("event") or {}
    event_uri = event.get("uri") or ""
    m = _BILL_URI_RE.search(event_uri)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return None


def _akn_xml_url(section: dict, date: str, chamber: str, dbsect: str) -> str | None:
    """Pick the canonical AKN XML URL — section.formats.xml.uri, else
    construct the standard pattern. Plan §3 stores this for Stage 2 to use
    later; nothing on Stage 1 fetches it."""
    fmt_xml = ((section.get("formats") or {}).get("xml") or {}) if section else {}
    inline = fmt_xml.get("uri")
    if inline:
        return inline
    if date and chamber and dbsect:
        return (
            f"https://data.oireachtas.ie/akn/ie/debateRecord/"
            f"{chamber}/{date}/debate/mul@/{dbsect}.xml"
        )
    return None


def _debate_url_web(date: str, chamber: str, dbsect: str) -> str | None:
    """Public oireachtas.ie deep link, mirrors legislation_enrichment's pattern.

    Example:
      https://www.oireachtas.ie/en/debates/debate/dail/2026-03-26/dbsect_30/
    """
    if not (date and chamber and dbsect):
        return None
    return (
        f"https://www.oireachtas.ie/en/debates/debate/{chamber}/{date}/{dbsect}/"
    )


def _chamber_short(record: dict) -> str:
    """'dail' / 'seanad' / '' for a debateRecord. The /v1/debates chamber
    filter is loose — committee debates leak into chamber=dail responses
    (see probe findings) — so we re-derive the chamber from the record's
    own house metadata and drop committee-only records.
    """
    house = record.get("house") or {}
    if house.get("chamberType") == "committee":
        return ""  # caller filters out empty-chamber rows
    house_code = house.get("houseCode")
    if house_code in {"dail", "seanad"}:
        return house_code
    chamber = record.get("chamber") or {}
    uri = (chamber.get("uri") or "").rstrip("/")
    parts = uri.split("/")
    last = parts[-1] if parts else ""
    return last if last in {"dail", "seanad"} else ""


def flatten_listings(payloads: list[dict]) -> list[dict]:
    """Walk the bronze JSON and emit one row per debate section.

    Skips committee records — Stage 1 cards live on TD pages, and
    committee-section attribution requires committee↔TD membership joins
    that are out of scope for this slice.
    """
    rows: list[dict] = []
    for payload in payloads or []:
        for r in payload.get("results") or []:
            rec = (r or {}).get("debateRecord") or {}
            chamber = _chamber_short(rec)
            if not chamber:
                continue  # committee or unknown — skip per plan §11 scope
            date = rec.get("date")
            if not date:
                continue
            for s in rec.get("debateSections") or []:
                ds = (s or {}).get("debateSection") or s
                if not isinstance(ds, dict):
                    continue
                dbsect = ds.get("debateSectionId")
                if not dbsect:
                    continue
                parent = ds.get("parentDebateSection") or {}
                counts = ds.get("counts") or {}
                bill_ref = _bill_ref(ds.get("bill"))
                rows.append(
                    {
                        "debate_section_id": str(dbsect),
                        "date": str(date),
                        "chamber": chamber,
                        "parent_section_id": parent.get("debateSectionId"),
                        "parent_section_title": parent.get("showAs"),
                        "bill_ref": bill_ref,
                        "debate_type": ds.get("debateType"),
                        "speaker_count": int(counts.get("speakerCount") or 0),
                        "speech_count": int(counts.get("speechCount") or 0),
                        "akn_xml_url": _akn_xml_url(ds, str(date), chamber, str(dbsect)),
                        "debate_url_web": _debate_url_web(str(date), chamber, str(dbsect)),
                        "show_as": ds.get("showAs"),
                    }
                )
    return rows


def _summary(df: pl.DataFrame) -> None:
    """Emit drift-detection counts for the PR description."""
    if df.is_empty():
        print("  (empty)")
        return
    print(f"  rows                 : {df.height}")
    print(f"  distinct dbsects     : {df.get_column('debate_section_id').n_unique()}")
    print(f"  distinct dates       : {df.get_column('date').n_unique()}")
    by_chamber = (
        df.group_by("chamber")
        .agg(
            pl.len().alias("rows"),
            pl.col("debate_section_id").n_unique().alias("distinct_dbsect"),
        )
        .sort("chamber")
    )
    print("  per-chamber split    :")
    for row in by_chamber.iter_rows(named=True):
        print(
            f"    {row['chamber']:<8}  rows={row['rows']:<6}  "
            f"distinct_dbsect={row['distinct_dbsect']}"
        )
    with_bill = df.filter(pl.col("bill_ref").is_not_null()).height
    with_parent = df.filter(pl.col("parent_section_id").is_not_null()).height
    print(f"  rows w/ bill_ref     : {with_bill}")
    print(f"  rows w/ parent       : {with_parent}")


def run() -> None:
    print(f"Flattening debate listings from {_BRONZE} ...")
    payloads = _load_bronze()
    if not payloads:
        print("No bronze listings found. Run pipeline.py (debates_listings step) first.")
        return

    rows = flatten_listings(payloads)
    if not rows:
        print("No debate sections found in bronze listings.")
        return

    df = pl.DataFrame(rows).unique(
        subset=["date", "chamber", "debate_section_id"]
    )

    _summary(df)

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(_OUT)
    print(f"  -> {_OUT}  ({df.height} rows)")


if __name__ == "__main__":
    run()
