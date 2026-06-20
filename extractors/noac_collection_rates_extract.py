"""Sandbox: NOAC council revenue-collection rates — the attributable "are they
collecting what's owed?" accountability signal, named by NOAC itself.

WHY ONLY THIS SLICE. NOAC's Local Authority Performance Indicator Report is the
statutory per-council scorecard (46 indicators, 11 areas), but it ships PDF-ONLY —
no CSV/Excel/dashboard exists — and the full 31-council-by-5-year grids are drawn
as CHART IMAGES, not text. So a complete per-LA/per-indicator parquet is NOT
reliably extractable from the source. What IS in the text layer, cleanly, is:
  * the NATIONAL AVERAGE for each indicator, and
  * NOAC's OWN named BEST and WORST councils (top-3 / bottom-3) per indicator.
That named best/worst is the strongest possible framing for "who's doing well /
who's delinquent": it is NOAC's published verdict, not ours (logic-firewall safe,
no inference; see [[feedback_no_inference_in_app]]).

This extractor parses the three M2 revenue-collection indicators — the sharpest
delinquency signals — from the 2024 report:
  M2A Commercial Rates · M2B Rent & Annuities · M2C Housing Loans

Grain: one row per (indicator, rank_type, council) named highlight, carrying the
indicator's national average for context. council matches the local_authority join
key used by v_constituency_la_crosswalk / v_la_chief_executives EXACTLY.

Source:  doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf (NOAC Report 77, Sept 2025, CC-BY-style public report)
Output:  pipeline_sandbox/_noac_output/noac_collection_rates.parquet
         data/_meta/noac_collection_rates_coverage.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import re
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("noac_collection_rates")
ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / "doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf"
OUT = ROOT / "pipeline_sandbox/_noac_output/noac_collection_rates.parquet"
COVERAGE = ROOT / "data/_meta/noac_collection_rates_coverage.json"
SOURCE_URL = "https://cdn.noac.ie/wp-content/uploads/2025/09/NOAC-Local-Authority-Performance-Indicator-Report-2024.pdf"
REPORT_YEAR = 2024

# The 31 local-authority join-key values (must match constituency_la_crosswalk.csv
# / la_chief_executives.csv). Any parsed council name that does not normalise into
# this set is a parse error and aborts the run rather than writing a junk row.
VALID_LA = {
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal",
    "Dublin City", "Dun Laoghaire-Rathdown", "Fingal", "Galway City",
    "Galway County", "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim",
    "Limerick", "Longford", "Louth", "Mayo", "Meath", "Monaghan", "Offaly",
    "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
}

# The three M2 collection indicators: (code, name, 0-based PDF page, "thing" word
# in the Highest/Lowest header). Page indices are physical (printed page + 1).
INDICATORS = [
    ("M2A", "Commercial Rates Collection %", 179, "commercial rates"),
    ("M2B", "Rent & Annuities Collection %", 180, "rent and annuities"),
    ("M2C", "Housing Loans Collection %", 181, "housing loans"),
]


def _norm_council(raw: str) -> str:
    """Map a NOAC-printed council name to the local_authority join key, or raise.

    Handles the mangled fada (Dún -> 'D�n'), '&' vs 'and', and the City-and-County
    amalgamations (Limerick/Waterford collapse to the bare county key).
    """
    s = raw.strip().rstrip(",.").strip()
    if "Laoghaire" in s:
        return "Dun Laoghaire-Rathdown"
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s).strip()
    # City-and-County amalgamations -> bare key
    for base in ("Limerick", "Waterford"):
        if s.startswith(base) and "City" in s and "County" in s:
            return base
    if s in VALID_LA:
        return s
    raise ValueError(f"unrecognised council name from NOAC text: {raw!r} -> {s!r}")


_PCT = re.compile(r"^\s*(\d{1,3}(?:\.\d+)?)\s*%\s*$")
_NATL = re.compile(r"average national collection level (?:for \d{4} )?(?:is|was)\s*(\d{1,3}(?:\.\d+)?)\s*%", re.I)


def _parse_block(lines: list[str], start_idx: int) -> list[tuple[str, float]]:
    """From the line after a 'Highest/Lowest ...' header, read alternating
    name / 'NN%' pairs until a non-pair line ends the block.
    """
    out: list[tuple[str, float]] = []
    i = start_idx
    while i + 1 < len(lines):
        name = lines[i].strip()
        m = _PCT.match(lines[i + 1])
        if not name or m is None:
            break
        out.append((_norm_council(name), float(m.group(1))))
        i += 2
    return out


def _parse_indicator(page_text: str, code: str, thing: str) -> tuple[float | None, list[dict]]:
    lines = [ln for ln in page_text.splitlines() if ln.strip()]
    natl = None
    if m := _NATL.search(page_text):
        natl = float(m.group(1))
    rows: list[dict] = []
    for rank_type, kw in (("highest", "Highest collection level"), ("lowest", "Lowest collection level")):
        hdr = None
        for j, ln in enumerate(lines):
            if ln.strip().lower().startswith(kw.lower()):
                hdr = j
                break
        if hdr is None:
            LOG.warning("%s: no '%s' block found", code, kw)
            continue
        for pos, (council, val) in enumerate(_parse_block(lines, hdr + 1), start=1):
            rows.append({"rank_type": rank_type, "rank_position": pos, "local_authority": council, "value_pct": val})
    return natl, rows


def extract() -> pl.DataFrame:
    if not PDF.exists():
        raise FileNotFoundError(f"NOAC PDF not found: {PDF}")
    doc = fitz.open(PDF)
    as_of = dt.date.today().isoformat()
    records: list[dict] = []
    for code, name, page, thing in INDICATORS:
        text = doc[page].get_text()
        natl, rows = _parse_indicator(text, code, thing)
        if not rows:
            raise ValueError(f"{code}: parsed zero highlight rows on page {page} — layout drift?")
        for r in rows:
            records.append(
                {
                    "indicator_code": code,
                    "indicator_name": name,
                    "year": REPORT_YEAR,
                    "national_average_pct": natl,
                    **r,
                    "source_page": page + 1,  # printed page number
                    "source_url": SOURCE_URL,
                    "as_of_date": as_of,
                }
            )
        LOG.info("%s: national avg %.1f%%, %d named councils", code, natl or float("nan"), len(rows))
    df = pl.DataFrame(records).sort(["indicator_code", "rank_type", "rank_position"])
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="parse + print, do not write")
    args = ap.parse_args()
    setup_standalone_logging("noac_collection_rates")

    df = extract()
    LOG.info("parsed %d rows across %d indicators", df.height, df["indicator_code"].n_unique())
    # ASCII-only dump (Windows cp1252 console chokes on Polars box-drawing glyphs)
    for r in df.iter_rows(named=True):
        LOG.info(
            "  %-4s %-7s #%d  %-26s %5.1f%%  (natl %.1f%%)",
            r["indicator_code"], r["rank_type"], r["rank_position"],
            r["local_authority"], r["value_pct"], r["national_average_pct"],
        )

    if args.dry_run:
        LOG.info("dry-run: not writing")
        return

    save_parquet(df, OUT)
    LOG.info("wrote %s (%d rows)", OUT, df.height)

    coverage = {
        "source": "NOAC Local Authority Performance Indicator Report 2024 (Report 77)",
        "source_url": SOURCE_URL,
        "report_year": REPORT_YEAR,
        "extracted_at": dt.date.today().isoformat(),
        "indicators": [
            {
                "code": code,
                "name": name,
                "national_average_pct": (
                    df.filter(pl.col("indicator_code") == code)["national_average_pct"].first()
                ),
                "n_named_councils": df.filter(pl.col("indicator_code") == code).height,
            }
            for code, name, _p, _t in INDICATORS
        ],
        "caveat": (
            "NOAC ships PDF-only; full 31-council grids are chart images and are NOT extractable. "
            "These are NOAC's own named best/worst (top-3/bottom-3) per indicator plus the national "
            "average — a partial, attributable highlight set, not a complete ranking."
        ),
    }
    COVERAGE.write_text(json.dumps(coverage, indent=2, default=str), encoding="utf-8")
    LOG.info("wrote coverage %s", COVERAGE)


if __name__ == "__main__":
    main()
