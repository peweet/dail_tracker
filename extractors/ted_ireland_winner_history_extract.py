"""TED legacy WINNER backfill (2016-2023) via per-notice XML -> SILVER parquet.

The eForms Search API serves Irish award notices back to 2016 but drops the WINNER for
pre-2024 legacy notices (verified: winner fields 0%; see doc/TED_ENRICHMENT.md §3.5/§6).
The per-notice XML (legacy TED_EXPORT envelope) DOES carry the full winner roster under
AWARDED_CONTRACT/CONTRACTOR with NATIONALID (CRO/VAT). This lane recovers it so the
winner-centric silver can extend 2016-2023 at the SAME (notice x winner) grain as the
2024+ API lane (ted_ireland_extract.py), enabling a clean UNION.

REUSE, not re-parse: the notice-level facts (buyer, date, CPV, total-value) already live in
the API-built BUYER layer (ted_ie_buyer_history.parquet). This lane only extracts WINNERS
from the XML and joins them to those validated notice facts — then runs the SHARED enrichment
(extractors/ted_enrich.py) so classification/CRO/value flags are byte-identical to the API lane.

Pipeline: enumerate 2016-2023 PNs from the buyer parquet -> resumable per-notice XML fetch
(services.ted_search.fetch_notice_xml) -> parse distinct winners -> join buyer facts -> enrich
-> ted_ie_winner_history.parquet (source_lane="per_notice_xml").

NOT wired into pipeline.py. Bronze XML cache is resumable (re-run continues where it stopped).

Run:
  ./.venv/Scripts/python.exe extractors/ted_ireland_winner_history_extract.py --limit 30   # smoke
  ./.venv/Scripts/python.exe extractors/ted_ireland_winner_history_extract.py               # full 2016-2023
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from extractors.ted_enrich import enrich_winner_rows  # noqa: E402
from extractors.ted_ireland_extract import (  # noqa: E402
    PAN_EU_HINT,
    PAN_EU_VALUE,
    clean_identifier,
    hr,
)
from services.parquet_io import save_parquet  # noqa: E402
from services.ted_search import fetch_notice_xml  # noqa: E402
from shared.buyer_clean import clean_buyer_display  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

BUYER_PARQUET = ROOT / "data/silver/parquet/ted_ie_buyer_history.parquet"
XML_CACHE = ROOT / "data/bronze/ted/notices"  # one {pn}.xml per notice (resumable)
OUT_SILVER = ROOT / "data/silver/parquet/ted_ie_winner_history.parquet"
OUT_COV = ROOT / "data/_meta/ted_ie_winner_history_coverage.json"

BACKFILL_YEARS = range(2016, 2024)  # 2016-2023; 2024+ is the API lane (winner present there)
FETCH_WORKERS = 4  # modest — the API 429s under load (see services.ted_search backoff)

SOURCE = {
    "dataset": "TED — Tenders Electronic Daily (legacy winner backfill 2016-2023, Ireland)",
    "publisher": "Publications Office of the European Union",
    "notice_xml_template": "https://ted.europa.eu/en/notice/{pn}/xml",
    "license": "EU open data — reuse authorised under Commission Decision 2011/833/EU",
    "attribution": "Contains information from TED (© European Union), reused under Decision 2011/833/EU.",
    "lane": "per_notice_xml (recovers the winner the pre-2024 Search API drops)",
}


def _sn(tag: str) -> str:
    return re.sub(r"\{[^}]+\}", "", tag)


def parse_winners(xml_bytes: bytes) -> list[tuple[str, str | None]]:
    """Extract distinct (winner_name, nationalid) from a legacy TED_EXPORT notice.

    Reads ONLY the ORIGINAL-language form (the multilingual TRANSLATION forms repeat every
    name ~10x). Winners are the CONTRACTOR OFFICIALNAMEs inside AWARD_CONTRACT blocks; the
    contracting authority's own OFFICIALNAME sits in CONTRACTING_BODY and is excluded.
    Deduped by name within the notice (a firm winning N lots = ONE winner row), matching the
    API lane's one-row-per-winner grain. Keeps the first non-'N/A' NATIONALID per firm.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    # locate the ORIGINAL form inside FORM_SECTION; fall back to the whole tree
    scope = root
    for el in root.iter():
        if _sn(el.tag) == "FORM_SECTION":
            for form in list(el):
                if form.attrib.get("CATEGORY") == "ORIGINAL":
                    scope = form
                    break
            break

    winners: dict[str, str | None] = {}
    for ac in (e for e in scope.iter() if _sn(e.tag) == "AWARD_CONTRACT"):
        for contr in (e for e in ac.iter() if _sn(e.tag) == "CONTRACTOR"):
            name = nat = None
            for sub in contr.iter():
                tag = _sn(sub.tag)
                txt = (sub.text or "").strip()
                if tag == "OFFICIALNAME" and txt and name is None:
                    name = txt
                elif tag == "NATIONALID" and txt and txt.upper() != "N/A" and nat is None:
                    nat = txt
            # keep first real NATIONALID seen for this firm
            if name and (name not in winners or (winners[name] is None and nat)):
                winners[name] = nat
    return list(winners.items())


def fetch_all_xml(pns: list[str]) -> int:
    """Resumable concurrent fetch of every PN's notice XML into XML_CACHE. Returns #present."""
    XML_CACHE.mkdir(parents=True, exist_ok=True)
    present = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {ex.submit(fetch_notice_xml, pn, XML_CACHE): pn for pn in pns}
        for i, fut in enumerate(concurrent.futures.as_completed(futs), start=1):
            try:
                if fut.result():
                    present += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  fetch error {futs[fut]}: {exc}")
            if i % 250 == 0 or i == len(pns):
                print(f"  fetched {i}/{len(pns)} (present={present})")
    return present


def build_rows(buyer_rows: list[dict]) -> list[dict]:
    """For each notice: read its cached XML, emit one row per distinct winner joined to the
    API-clean notice facts already in the buyer parquet."""
    rows: list[dict] = []
    parsed = no_xml = no_winner = 0
    for b in buyer_rows:
        pn = b["publication_number"]
        xml_path = XML_CACHE / f"{pn}.xml"
        if not xml_path.exists():
            no_xml += 1
            continue
        winners = parse_winners(xml_path.read_bytes())
        parsed += 1
        n_win = len(winners)
        val = b.get("total_value_eur")
        if not winners:  # award notice with no parsed contractor — keep for provenance
            no_winner += 1
            winners = [(None, None)]
            n_win = 0
        pan_eu = bool(PAN_EU_HINT.search(b.get("buyer_name") or "")) or (n_win > 1 and (val or 0) > PAN_EU_VALUE)
        for name, nat in winners:
            pan_eu_row = pan_eu or (bool(PAN_EU_HINT.search(name)) if name else False)
            rows.append(
                {
                    "publication_number": pn,
                    "notice_url": b.get("notice_url"),
                    "buyer_name": b.get("buyer_name"),
                    "winner_name": name,
                    "winner_identifier_raw": nat,
                    "winner_identifier_digits": clean_identifier(nat) if nat else None,
                    "award_value_eur": val if (val and val > 0) else None,
                    "currency": b.get("currency") or "EUR",
                    "n_winners": n_win,
                    "is_multi_supplier_framework": n_win > 1,
                    "is_pan_eu_outlier": pan_eu_row,
                    "value_kind": "framework_or_dps_ceiling" if n_win > 1 else "contract_award_value",
                    "cpv_code": b.get("cpv_code"),
                    "cpv_division": b.get("cpv_division"),
                    "dispatch_date": b.get("dispatch_date"),
                    "year": b.get("year"),
                    "month": b.get("month"),
                    "source_lane": "per_notice_xml",
                }
            )
    print(f"\nparsed {parsed} notices | missing-xml {no_xml} | award-no-winner {no_winner}")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="smoke bound: only the first N notices")
    ap.add_argument("--no-fetch", action="store_true", help="skip fetching; parse only what's cached")
    args = ap.parse_args()

    hr("TED legacy WINNER backfill 2016-2023 (per-notice XML)")
    if not BUYER_PARQUET.exists():
        print(f"ERROR: {BUYER_PARQUET} missing — run ted_ireland_buyer_history_extract.py first.")
        return

    buyer = (
        pl.read_parquet(BUYER_PARQUET)
        .filter(pl.col("year").is_in(list(BACKFILL_YEARS)))
        .select(
            "publication_number",
            "notice_url",
            "buyer_name",
            "total_value_eur",
            "currency",
            "cpv_code",
            "cpv_division",
            "dispatch_date",
            "year",
            "month",
        )
        .unique(subset=["publication_number"])
        .sort("publication_number")
    )
    if args.limit:
        buyer = buyer.head(args.limit)
    buyer_rows = buyer.to_dicts()
    pns = [r["publication_number"] for r in buyer_rows]
    print(f"target notices (2016-2023): {len(pns):,}")

    if not args.no_fetch:
        print(f"fetching per-notice XML -> {XML_CACHE} (resumable, {FETCH_WORKERS} workers)")
        present = fetch_all_xml(pns)
        print(f"XML present after fetch: {present:,}/{len(pns):,}")

    df = pl.DataFrame(build_rows(buyer_rows), infer_schema_length=None)
    if df.is_empty():
        print("no rows built — nothing written.")
        return

    df = enrich_winner_rows(df).with_columns(pl.lit("per_notice_xml").alias("source_lane"))
    # buyer_name is inherited from the (now-cleaned) buyer-history parquet, but clean defensively
    # so a stray id suffix can never reach the UNION with the API award lane.
    df = clean_buyer_display(df, "buyer_name")

    OUT_SILVER.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_SILVER)

    hr("SILVER WRITTEN (legacy winner backfill)")
    print(f"rows (notice x winner): {df.height:,}  ->  {OUT_SILVER}")
    print(f"distinct notices: {df['publication_number'].n_unique():,}")
    print(df.group_by("supplier_class").len().sort("len", descending=True))
    cro_hit = df.filter(pl.col("cro_match_method") != "none")
    print(f"CRO matched: {cro_hit.height:,} ({cro_hit.height / df.height:.0%})")
    print(df.group_by("year").len().sort("year"))

    cov = {
        "rows_notice_x_winner": df.height,
        "distinct_notices": int(df["publication_number"].n_unique()),
        "rows_with_winner": int(df["winner_name"].is_not_null().sum()),
        "cro_match_rate": round(cro_hit.height / max(1, df.height), 3),
        "by_year": {str(r["year"]): r["len"] for r in df.group_by("year").len().sort("year").iter_rows(named=True)},
        "date_span": [df["dispatch_date"].min(), df["dispatch_date"].max()],
        "layer": "silver",
        "grain": "one row per (notice x distinct winner); source_lane=per_notice_xml",
        "schema_parity": "UNION-compatible with ted_ie_awards.parquet (2024+ API lane) — same enrich_winner_rows",
        "source": SOURCE,
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "Pre-2024 winners recovered from per-notice TED_EXPORT XML (the Search API drops "
        "them). Notice-level facts (buyer/date/CPV/total-value) reused from the API buyer layer. "
        "award_value_eur is the NOTICE total repeated per winner; value_safe_to_sum excludes "
        "frameworks + pan-EU + large, same as the API lane. COUNT is the trustworthy metric.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")
    print(
        "\nLAYER=silver. UNION with ted_ie_awards.parquet via a sql_views/ted_*.sql view for the full 2016+ winner history."
    )


if __name__ == "__main__":
    main()
