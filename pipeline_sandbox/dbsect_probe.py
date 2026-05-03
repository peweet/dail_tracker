"""
pipeline_sandbox/dbsect_probe.py

Lane A probe. Picks a small, stratified cross-section of dbsect ids
from data/silver/parquet/dbsect_index.parquet (produced by
dbsect_harvest.py) and calls the Oireachtas /v1/debates endpoint for
each. Confirms what the API actually returns before any production
fetch is wired into services/.

The probe is deliberately small (default ~12 ids) and politely rate
limited via services.http_engine's shared session.

Sampling strategy (best-effort, capped at MAX_PER_BUCKET each):
  - dbsects cited by all 3 sources (bill + question + vote)
  - dbsects cited by exactly 2 sources
  - dbsects cited by exactly 1 source, one each from bill / question / vote
  - mix of dail and seanad if both are present

Two URL shapes are tried per dbsect, and both raw responses are saved:
  1. Day window: /v1/debates?date_start=DATE&date_end=DATE&chamber=CH
     Returns every debate section that sat that day in that chamber.
     We then check whether one of the returned records carries our
     debate_section_id.
  2. Direct AKN record: data.oireachtas.ie/akn/.../debate/main
     The debate_uri already stored in the index. Returns the structured
     debate record (XML over HTTP — saved as bytes, not parsed here).

Outputs:
  data/bronze/debates/probe/<dbsect>__day.json   (api.oireachtas.ie)
  data/bronze/debates/probe/<dbsect>__akn.xml    (data.oireachtas.ie)
  pipeline_sandbox/dbsect_probe_findings.md      (printed + appended)

Run:
  python pipeline_sandbox/dbsect_probe.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import polars as pl

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from config import BRONZE_DIR, SILVER_PARQUET_DIR  # noqa: E402
from services.http_engine import session  # noqa: E402

API_BASE = "https://api.oireachtas.ie/v1"

# api.oireachtas.ie is happy with any UA; data.oireachtas.ie/akn 403s
# unless we look browser-ish AND set a Referer pointing at the public
# oireachtas.ie site. Both are needed — UA alone is still 403.
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; dail-tracker/0.1; +contact: pglyn)",
    "Accept": "application/json, application/xml;q=0.9, */*;q=0.1",
    "Referer": "https://www.oireachtas.ie/",
})

_INDEX = SILVER_PARQUET_DIR / "dbsect_index.parquet"
_OUT_DIR = BRONZE_DIR / "debates" / "probe"
_FINDINGS = Path(__file__).with_name("dbsect_probe_findings.md")

MAX_PER_BUCKET = 4


def _load_index() -> pl.DataFrame:
    if not _INDEX.exists():
        raise SystemExit(
            f"{_INDEX} not found. Run pipeline_sandbox/dbsect_harvest.py first."
        )
    return pl.read_parquet(_INDEX)


def _stratify(df: pl.DataFrame) -> list[dict]:
    """Pick a stratified sample of dbsect rows.

    One row per dbsect (we don't need the full provenance fanout for
    the probe). Strata: cited-by-3, cited-by-2, cited-by-1 split by
    source. Capped at MAX_PER_BUCKET per stratum.
    """
    per_dbsect = (
        df.group_by("debate_section_id")
        .agg(
            pl.col("source").n_unique().alias("source_count"),
            pl.col("source").unique().alias("sources"),
            pl.col("date").drop_nulls().first().alias("date"),
            pl.col("chamber").drop_nulls().first().alias("chamber"),
            pl.col("debate_uri").drop_nulls().first().alias("debate_uri"),
            pl.col("debate_title").drop_nulls().first().alias("debate_title"),
        )
        .filter(pl.col("date").is_not_null() & (pl.col("chamber") != ""))
    )

    picked: list[dict] = []
    seen: set[str] = set()

    def take(filter_expr, label: str, want: int) -> None:
        rows = per_dbsect.filter(filter_expr).sort("date", descending=True)
        n = 0
        for row in rows.iter_rows(named=True):
            if row["debate_section_id"] in seen:
                continue
            row["bucket"] = label
            picked.append(row)
            seen.add(row["debate_section_id"])
            n += 1
            if n >= want:
                return

    take(pl.col("source_count") == 3, "all_three_sources", MAX_PER_BUCKET)
    take(pl.col("source_count") == 2, "two_sources", MAX_PER_BUCKET)

    one = pl.col("source_count") == 1
    take(one & pl.col("sources").list.contains("bill"), "bill_only", 2)
    take(one & pl.col("sources").list.contains("question"), "question_only", 2)
    take(one & pl.col("sources").list.contains("vote"), "vote_only", 2)

    return picked


def _fetch_day_window(date: str, chamber: str) -> tuple[dict, int, str]:
    url = (
        f"{API_BASE}/debates"
        f"?date_start={date}"
        f"&date_end={date}"
        f"&chamber={quote(chamber)}"
        f"&limit=200"
        f"&lang=en"
    )
    resp = session.get(url, timeout=(10, 60))
    resp.raise_for_status()
    return resp.json(), len(resp.content), url


def _akn_xml_url(matched: dict | None, fallback_uri: str | None,
                 dbsect: str, date: str, chamber: str) -> str | None:
    """Pick the canonical AKN xml URL.

    Preference: matched.formats.xml.uri (returned inline by the API),
    falls back to constructing the .xml suffix from the dbsect URI in
    the bronze (which has no .xml extension and 403s on its own).
    """
    if matched:
        fmt = ((matched.get("formats") or {}).get("xml") or {}).get("uri")
        if fmt:
            return fmt
    if fallback_uri and fallback_uri.endswith(f"/{dbsect}"):
        # rewrite .../debate/dbsect_30 -> .../debate/mul@/dbsect_30.xml
        return fallback_uri.rsplit("/", 1)[0] + f"/mul@/{dbsect}.xml"
    if date and chamber and dbsect:
        return (
            f"https://data.oireachtas.ie/akn/ie/debateRecord/"
            f"{chamber}/{date}/debate/mul@/{dbsect}.xml"
        )
    return None


def _fetch_akn(url: str) -> tuple[bytes, int, str]:
    resp = session.get(url, timeout=(10, 60))
    resp.raise_for_status()
    return resp.content, len(resp.content), url


def _find_dbsect_in_payload(payload: dict, dbsect: str) -> dict | None:
    """Find a debateSection whose debateSectionId equals our dbsect.

    Real shape: results[*].debateRecord.debateSections[*].debateSection.
    The same dbsect (e.g. 'dbsect_2') recurs every sitting day, so the
    composite identity is (date, chamber, dbsect). The matcher here is
    just confirming the API actually exposes this dbsect for the day
    we asked for.
    """
    for r in payload.get("results") or []:
        rec = (r or {}).get("debateRecord") or {}
        for s in rec.get("debateSections") or []:
            ds = (s or {}).get("debateSection") or s
            if isinstance(ds, dict) and ds.get("debateSectionId") == dbsect:
                return ds
    return None


def _summarise_day(payload: dict, dbsect: str) -> dict:
    head = payload.get("head") or {}
    results = payload.get("results") or []
    matched = _find_dbsect_in_payload(payload, dbsect)
    speakers = (matched or {}).get("speakers") or []
    return {
        "head_total_results": head.get("counts", {}).get("resultCount")
        or head.get("totalResults"),
        "debate_record_count": len(results),
        "matched_dbsect": matched is not None,
        "matched_show_as": (matched or {}).get("showAs"),
        "matched_debate_type": (matched or {}).get("debateType"),
        "matched_speaker_count": len(speakers),
        "matched_first_speaker_keys": (
            sorted(speakers[0].keys()) if speakers and isinstance(speakers[0], dict) else []
        ),
    }


def run() -> None:
    df = _load_index()
    sample = _stratify(df)
    if not sample:
        raise SystemExit("Stratifier returned no rows. Index may be empty.")

    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    findings: list[str] = []
    by_bucket: dict[str, list[str]] = defaultdict(list)

    print(f"Probing {len(sample)} dbsects across "
          f"{len(set(r['bucket'] for r in sample))} strata ...")

    for row in sample:
        dbsect = row["debate_section_id"]
        date = row["date"]
        chamber = row["chamber"]
        debate_uri = row["debate_uri"]
        bucket = row["bucket"]

        line = f"\n## {dbsect}  ({bucket}, {date}, {chamber})"
        print(line)
        findings.append(line)
        findings.append(f"- title  : {row['debate_title']!r}")
        findings.append(f"- sources: {sorted(row['sources'])}")

        matched: dict | None = None

        # 1) day-window /v1/debates
        try:
            payload, raw_bytes, url_used = _fetch_day_window(date, chamber)
            matched = _find_dbsect_in_payload(payload, dbsect)
            (_OUT_DIR / f"{dbsect}__day.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            summary = _summarise_day(payload, dbsect)
            print(
                f"  day-window: {raw_bytes:,} bytes  "
                f"records={summary['debate_record_count']}  "
                f"matched={summary['matched_dbsect']}  "
                f"speakers={summary['matched_speaker_count']}"
            )
            findings.append(f"- day_url: `{url_used}`")
            findings.append(
                f"- day_summary: head_total={summary['head_total_results']}, "
                f"debate_records={summary['debate_record_count']}, "
                f"matched_dbsect={summary['matched_dbsect']}, "
                f"speakers={summary['matched_speaker_count']}"
            )
            if summary["matched_dbsect"]:
                findings.append(
                    f"- matched: show_as={summary['matched_show_as']!r}, "
                    f"debate_type={summary['matched_debate_type']!r}"
                )
                if summary["matched_first_speaker_keys"]:
                    findings.append(
                        f"- speaker_keys: {summary['matched_first_speaker_keys']}"
                    )
        except Exception as e:  # noqa: BLE001
            msg = f"  day-window FAILED: {e}"
            print(msg)
            findings.append(f"- day_window_error: `{e}`")

        # 2) AKN XML — uses formats.xml.uri from the matched section
        # when available, else constructs the .xml URL from date+chamber.
        akn_url = _akn_xml_url(matched, debate_uri, dbsect, date, chamber)
        if akn_url:
            try:
                content, raw_bytes, url_used = _fetch_akn(akn_url)
                ext = "xml" if (
                    b"<akomaNtoso" in content[:200] or b"<?xml" in content[:30]
                ) else "bin"
                (_OUT_DIR / f"{dbsect}__akn.{ext}").write_bytes(content)
                speech_count = content.count(b"<speech ")
                print(f"  akn       : {raw_bytes:,} bytes  speeches~{speech_count}  ({ext})")
                findings.append(
                    f"- akn_url: `{url_used}`  ({raw_bytes:,} bytes, "
                    f"~{speech_count} speech elements, {ext})"
                )
            except Exception as e:  # noqa: BLE001
                msg = f"  akn FAILED ({akn_url}): {e}"
                print(msg)
                findings.append(f"- akn_error: `{e}` for `{akn_url}`")

        by_bucket[bucket].append(dbsect)

    # bucket recap
    print("\nProbed by bucket:")
    for k, v in by_bucket.items():
        print(f"  {k:<20} {len(v)}  {v}")

    header = (
        "# dbsect_probe — findings\n\n"
        "Generated by `pipeline_sandbox/dbsect_probe.py`. Each section "
        "below is one dbsect id sampled from `dbsect_index.parquet`, "
        "with the raw API response saved beside it under "
        "`data/bronze/debates/probe/`.\n"
    )
    _FINDINGS.write_text(header + "\n".join(findings) + "\n", encoding="utf-8")
    print(f"\nWrote findings -> {_FINDINGS}")


if __name__ == "__main__":
    run()
