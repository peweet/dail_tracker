"""HSA COMAH (Seveso III) establishment register — upper + lower tier, geocoded where possible.

The HSA publishes the national register of major-accident establishments as twelve HTML pages
(6 regions x 2 tiers) under "Information to the Public", each a two-column table
(Establishment Name, Establishment Address). There is no machine-readable download and no
coordinates; per-establishment consultation distances are held by planning authorities, not
published centrally — that limitation is recorded in the coverage note, never papered over.

Geocoding is an honest NAME-JOIN, not invention: most COMAH establishments are also
EPA-licensed, so we match normalised operator names against the epa_licensed_facilities layer
(WFS points ingested the same day) and take the facility's coordinates on a confident match.
Unmatched rows keep NULL geometry in the register and are EXCLUDED from the spatial layer —
the match rate is in the coverage json, so "n mapped of m" is always visible.

Writes:
  data/silver/parquet/hsa_comah_register.parquet                 full register (tier, region,
                                                                 match status; tabular home)
  data/silver/parquet/planning_layers/hsa_comah_establishments.parquet
                                                                 matched rows only, wkb points
                                                                 (the siting layer store dir)
  data/_meta/hsa_comah_register_coverage.json

Run:
  python extractors/hsa_comah_extract.py            # fetch + parse + match + write
  python extractors/hsa_comah_extract.py --dry-run  # fetch + parse + match, print, no write
"""

from __future__ import annotations

import argparse
import html
import logging
import re
import sys
import unicodedata
from pathlib import Path

import polars as pl
import shapely

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.coverage_io import save_coverage  # noqa: E402
from services.extract_runner import run_extractor  # noqa: E402
from services.http_engine import fetch_bytes, polite_headers  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

LOG = logging.getLogger("hsa_comah")

_BASE = (
    "https://www.hsa.ie/eng/your_industry/chemicals/legislation_enforcement/comah/"
    "information_to_the_public/"
)
# Region pages are DISCOVERED from each tier's index page at runtime — the two tiers group
# counties differently (e.g. upper "dublin_louth_kildare" vs lower "dublin_louth"), the lower
# index even links two Galway variants, and hardcoded slugs silently 404'd on first build
# (curl fallback returned 404 HTML with no tables = rows quietly missing). Discovery + a
# zero-row page guard closes that hole.
_TIER_INDEX = {
    "upper": "upper_tier_comah_establishments_by_region/",
    "lower": "lower_tier_establishments_by_region/",
}
_REGION_LINK = re.compile(r'href="([^"]*_tier_establishments_in_[^"]+)"')
_EPA_LAYER = ROOT / "data" / "silver" / "parquet" / "planning_layers" / "epa_licensed_facilities.parquet"
_OUT_REGISTER = ROOT / "data" / "silver" / "parquet" / "hsa_comah_register.parquet"
_OUT_LAYER = ROOT / "data" / "silver" / "parquet" / "planning_layers" / "hsa_comah_establishments.parquet"
_COV = ROOT / "data" / "_meta" / "hsa_comah_register_coverage.json"
# ~30 upper-tier and ~40 lower-tier establishments nationally is the recent order of magnitude;
# fewer than these means pages moved or the table markup changed — do not ship a short register.
_FLOOR = {"upper": 20, "lower": 20}

# legal-form noise stripped before matching; never part of an operator's identity
_LEGAL = re.compile(
    r"\b(ltd|limited|teoranta|teo|plc|dac|clg|uc|ulc|company|co|ireland|irl|group|holdings)\b\.?",
    re.I,
)


def _fetch_page(url: str) -> str:
    data = fetch_bytes(url, headers=polite_headers(browser=True))
    if not data:
        raise SystemExit(f"fetch failed: {url}")
    return data.decode("utf-8", "ignore")


def _parse_table(page_html: str) -> list[tuple[str, str]]:
    """(name, address) rows from the page's establishment table; header row dropped."""
    tables = re.findall(r"<table.*?</table>", page_html, re.S)
    out: list[tuple[str, str]] = []
    for t in tables:
        for row in re.findall(r"<tr.*?</tr>", t, re.S):
            cells = [
                re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", c))).strip()
                for c in re.findall(r"<t[dh][^>]*>.*?</t[dh]>", row, re.S)
            ]
            if len(cells) >= 2 and cells[0] and cells[0].lower() != "establishment name":
                out.append((cells[0], cells[1]))
    return out


def _norm(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = _LEGAL.sub(" ", s.lower())
    return re.sub(r"[^a-z0-9 ]", " ", s).strip()


def _tokens(name: str) -> set[str]:
    return {t for t in _norm(name).split() if len(t) > 1}


def fetch_register() -> tuple[pl.DataFrame, list[str]]:
    rows: list[dict] = []
    dead_links: list[str] = []
    for tier, index_path in _TIER_INDEX.items():
        index_html = _fetch_page(_BASE + index_path)
        links = sorted(
            {l for l in _REGION_LINK.findall(index_html) if f"{tier}_tier_establishments_in_" in l}
        )
        if not links:
            raise SystemExit(f"{tier}-tier index page listed no region links — page moved?")
        for link in links:
            url = link if link.startswith("http") else "https://www.hsa.ie" + link
            region = url.rstrip("/").rsplit("_in_", 1)[-1]
            page_rows = _parse_table(_fetch_page(url))
            if not page_rows:
                # the HSA's own index carries stale links (galway_mayo_cavan_roscommon 404s
                # beside the live galway_mayo_cavan) — skip LOUDLY, record in coverage, and
                # let the per-tier row floors catch any real shortfall
                LOG.warning("region page parsed 0 establishments, skipping: %s", url)
                dead_links.append(url)
                continue
            for name, addr in page_rows:
                rows.append(
                    {"establishment": name, "address": addr, "tier": tier, "region": region}
                )
    # the lower index links overlapping Galway variants — dedupe on establishment identity
    df = pl.DataFrame(rows).unique(subset=["establishment", "address", "tier"], keep="first")
    for tier, floor in _FLOOR.items():
        n = df.filter(pl.col("tier") == tier).height
        if n < floor:
            raise SystemExit(f"{tier}-tier register parsed only {n} rows (< {floor}) — page layout changed?")
    return df, dead_links


def match_epa(df: pl.DataFrame) -> pl.DataFrame:
    """Best-token-overlap name match against EPA licensed facilities; county must agree."""
    epa = pl.read_parquet(_EPA_LAYER)
    facilities = []
    for r in epa.iter_rows(named=True):
        pt = shapely.from_wkb(r["wkb"]).centroid
        facilities.append(
            {
                "name_tokens": _tokens(r["Name"] or ""),
                "blob": _norm((r["Name"] or "") + " " + (r["Address"] or "")),
                "reg": r["RegCD"],
                "epa_name": r["Name"],
                "lon": pt.x,
                "lat": pt.y,
            }
        )
    county_re = re.compile(r"co\.?\s+([a-z]+)|county\s+([a-z]+)", re.I)

    matched = []
    for r in df.iter_rows(named=True):
        toks = _tokens(r["establishment"])
        m = county_re.search(r["address"] or "")
        county = ((m.group(1) or m.group(2)) if m else "").lower()
        best, best_score = None, 0.0
        for f in facilities:
            if not f["name_tokens"]:
                continue
            inter = len(toks & f["name_tokens"])
            score = inter / max(len(toks | f["name_tokens"]), 1)
            # county gate: only enforce when the HSA address names one and the EPA blob has any
            if county and county not in f["blob"] and inter < len(toks):
                continue
            if score > best_score:
                best, best_score = f, score
        ok = best is not None and best_score >= 0.5
        matched.append(
            {
                **r,
                "epa_reg_cd": best["reg"] if ok else None,
                "epa_name": best["epa_name"] if ok else None,
                "match_score": round(best_score, 3) if ok else None,
                "lon": best["lon"] if ok else None,
                "lat": best["lat"] if ok else None,
            }
        )
    return pl.DataFrame(matched)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    register, dead_links = fetch_register()
    df = match_epa(register)
    n_matched = df.filter(pl.col("lat").is_not_null()).height
    LOG.info(
        "register %d rows (%d upper / %d lower) | geocoded via EPA join: %d (%.0f%%)",
        df.height,
        df.filter(pl.col("tier") == "upper").height,
        df.filter(pl.col("tier") == "lower").height,
        n_matched,
        100 * n_matched / max(df.height, 1),
    )
    if args.dry_run:
        print(df.group_by("tier").agg(n=pl.len(), geocoded=pl.col("lat").is_not_null().sum()))
        print(df.filter(pl.col("lat").is_null()).select("establishment", "tier").head(20))
        return

    save_parquet(df, _OUT_REGISTER)
    spatial = df.filter(pl.col("lat").is_not_null())
    spatial = spatial.with_columns(
        pl.struct(["lon", "lat"])
        .map_elements(
            lambda s: shapely.to_wkb(shapely.Point(s["lon"], s["lat"])), return_dtype=pl.Binary
        )
        .alias("wkb")
    ).drop("lon", "lat")
    save_parquet(spatial, _OUT_LAYER)
    save_coverage(
        {
            "source": _BASE + "(12 region/tier pages)",
            "rows": df.height,
            "upper_tier": df.filter(pl.col("tier") == "upper").height,
            "lower_tier": df.filter(pl.col("tier") == "lower").height,
            "geocoded_rows": n_matched,
            "geocode_method": "name-join to epa_licensed_facilities (token overlap >= 0.5, county-gated); unmatched rows carry NULL geometry and are excluded from the spatial layer",
            "grain": "one row per establishment x tier (HSA register as published)",
            "not_available": "consultation distances — held by planning authorities per establishment, not centrally published; the spatial layer marks WHERE establishments are, never the size of the zone around them",
            "dead_index_links_skipped": dead_links,
            "licence": "hsa.ie public register pages; facts-only extraction",
        },
        _COV,
    )
    print(
        f"OK {_OUT_REGISTER.name}: {df.height} establishments | layer: {spatial.height} geocoded"
    )


if __name__ == "__main__":
    run_extractor(main)
