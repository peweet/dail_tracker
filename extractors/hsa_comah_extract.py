"""HSA COMAH (Seveso III) establishment register — upper + lower tier, geocoded where possible.

The HSA publishes the national register of major-accident establishments as twelve HTML pages
(6 regions x 2 tiers) under "Information to the Public", each a two-column table
(Establishment Name, Establishment Address). There is no machine-readable download and no
coordinates; per-establishment consultation distances are held by planning authorities, not
published centrally — that limitation is recorded in the coverage note, never papered over.

Geocoding is layered and honest, never invention (per-row `geocode_source` + `geocode_precision`
say which layer produced each point):
  1. NAME-JOIN (`name_join`/`site`): most COMAH establishments are also EPA-licensed, so we match
     normalised operator names against the epa_licensed_facilities layer (WFS points ingested the
     same day) and take the facility's coordinates on a confident match.
  2. ADDRESS GEOCODE (`nominatim`/`address`, added 2026-07-31): unmatched rows are geocoded from
     the HSA-published establishment address via Nominatim (1 req/s, results cached to a sidecar
     json so re-runs are free). An address hit is the published postal address, not a surveyed
     site point — approximate by nature.
  3. TOWN FALLBACK (`nominatim`/`town`): where the full address misses, the town+county tail is
     tried; a hit is a TOWN CENTROID, marked as such, and must never be presented as a site
     location downstream.
Rows failing all three keep NULL geometry and are EXCLUDED from the spatial layer — the per-source
split is in the coverage json, so "n mapped of m" (and how) is always visible.

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
import json
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.parse import urlencode

import polars as pl
import shapely

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.coverage_io import save_coverage  # noqa: E402
from services.extract_runner import run_extractor  # noqa: E402
from services.http_engine import fetch_bytes, polite_headers  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

LOG = logging.getLogger("hsa_comah")

_BASE = "https://www.hsa.ie/eng/your_industry/chemicals/legislation_enforcement/comah/information_to_the_public/"
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

# Nominatim address geocoding (2026-07-31). Usage-policy compliance: max 1 req/s (enforced by
# _NOMINATIM_MIN_INTERVAL between UNCACHED requests), identifying UA (RESEARCH_UA carries a
# contact address), and a persistent sidecar cache so re-runs send zero requests.
_NOMINATIM = "https://nominatim.openstreetmap.org/search"
_GEO_CACHE = ROOT / "data" / "_meta" / "hsa_comah_nominatim_cache.json"
_NOMINATIM_MIN_INTERVAL = 1.05  # seconds between live requests
# Ireland sanity box — a hit outside it is treated as a miss, never shipped.
_IE_BOUNDS = (-11.0, 51.2, -5.3, 55.6)  # lon_min, lat_min, lon_max, lat_max
_last_request_ts = 0.0


def _load_geo_cache() -> dict:
    if _GEO_CACHE.exists():
        try:
            return json.loads(_GEO_CACHE.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 — a corrupt cache is rebuilt, never fatal
            LOG.warning("geocode cache unreadable, starting fresh: %s", _GEO_CACHE)
    return {}


def _save_geo_cache(cache: dict) -> None:
    # Atomic via save_coverage (its payload is generic JSON, not just coverage):
    # this cache is rewritten after every geocode batch, and a crash mid-write
    # would previously have corrupted it and thrown away all cached lookups.
    from services.coverage_io import save_coverage

    save_coverage(cache, _GEO_CACHE, indent=1, sort_keys=True)


def _nominatim_query(query: str, cache: dict) -> dict | None:
    """One cached Nominatim lookup: {'lon','lat','display_name','osm_type','type'} or None.

    Cache stores misses as None too, so a re-run is free either way. The 1 req/s wait applies
    only to live requests.
    """
    global _last_request_ts
    if query in cache:
        return cache[query]
    wait = _NOMINATIM_MIN_INTERVAL - (time.monotonic() - _last_request_ts)
    if wait > 0:
        time.sleep(wait)
    url = (
        _NOMINATIM
        + "?"
        + urlencode({"q": query, "format": "jsonv2", "countrycodes": "ie", "limit": 1, "addressdetails": 0})
    )
    data = fetch_bytes(url, headers=polite_headers(), curl_fallback=False)
    _last_request_ts = time.monotonic()
    result = None
    if data:
        try:
            rows = json.loads(data)
            if rows:
                r = rows[0]
                lon, lat = float(r["lon"]), float(r["lat"])
                if _IE_BOUNDS[0] <= lon <= _IE_BOUNDS[2] and _IE_BOUNDS[1] <= lat <= _IE_BOUNDS[3]:
                    result = {
                        "lon": lon,
                        "lat": lat,
                        "display_name": r.get("display_name", ""),
                        "osm_type": r.get("osm_type", ""),
                        "type": r.get("type", ""),
                    }
        except (ValueError, KeyError, TypeError):
            LOG.warning("nominatim response unparseable for %r", query)
    cache[query] = result
    return result


def _town_fallback_query(address: str) -> str | None:
    """Coarse retry: the last two comma segments of the address (town + county), or None."""
    parts = [p.strip() for p in (address or "").split(",") if p.strip()]
    parts = [p for p in parts if p.lower() not in ("ireland", "eire", "éire")]
    if len(parts) < 2:
        return None
    return ", ".join(parts[-2:])


def geocode_unmatched(df: pl.DataFrame) -> pl.DataFrame:
    """Fill lon/lat for rows the EPA name-join missed, from the published address.

    Adds three columns to every row: geocode_source ('name_join'|'nominatim'|null),
    geocode_precision ('site'|'address'|'town'|null), geocode_note (audit trail). A 'town' hit
    is a town centroid — downstream copy must present it as approximate, never a site point.
    """
    cache = _load_geo_cache()
    out_rows = []
    n_cached_before = len(cache)
    for r in df.iter_rows(named=True):
        row = dict(r)
        if row["lat"] is not None:
            row["geocode_source"] = "name_join"
            row["geocode_precision"] = "site"
            row["geocode_note"] = (
                f"EPA licensed-facility point ({row['epa_reg_cd']}, token overlap {row['match_score']})"
            )
            out_rows.append(row)
            continue
        addr = (row["address"] or "").strip()
        hit, precision = None, None
        if addr:
            hit = _nominatim_query(addr, cache)
            precision = "address" if hit else None
            if hit is None:
                coarse = _town_fallback_query(addr)
                if coarse:
                    hit = _nominatim_query(coarse, cache)
                    precision = "town" if hit else None
        if hit:
            row["lon"], row["lat"] = hit["lon"], hit["lat"]
            row["geocode_source"] = "nominatim"
            row["geocode_precision"] = precision
            row["geocode_note"] = (
                f"Nominatim {precision} match ({hit['osm_type']}/{hit['type']}): {hit['display_name'][:120]}"
            )
        else:
            row["geocode_source"] = None
            row["geocode_precision"] = None
            row["geocode_note"] = "ungeocoded — EPA name-join missed and Nominatim returned no usable hit"
        out_rows.append(row)
    _save_geo_cache(cache)
    LOG.info("geocode cache: %d entries (%d before run)", len(cache), n_cached_before)
    return pl.DataFrame(out_rows)


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
        links = sorted({link for link in _REGION_LINK.findall(index_html) if f"{tier}_tier_establishments_in_" in link})
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
                rows.append({"establishment": name, "address": addr, "tier": tier, "region": region})
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
    n_join = df.filter(pl.col("lat").is_not_null()).height
    df = geocode_unmatched(df)
    n_geo = df.filter(pl.col("lat").is_not_null()).height
    n_addr = df.filter(pl.col("geocode_precision") == "address").height
    n_town = df.filter(pl.col("geocode_precision") == "town").height
    LOG.info(
        "register %d rows (%d upper / %d lower) | geocoded %d (%.0f%%): %d EPA join + %d address + %d town",
        df.height,
        df.filter(pl.col("tier") == "upper").height,
        df.filter(pl.col("tier") == "lower").height,
        n_geo,
        100 * n_geo / max(df.height, 1),
        n_join,
        n_addr,
        n_town,
    )
    if args.dry_run:
        print(df.group_by("tier").agg(n=pl.len(), geocoded=pl.col("lat").is_not_null().sum()))
        print(df.group_by("geocode_source", "geocode_precision").agg(n=pl.len()).sort("n", descending=True))
        print(df.filter(pl.col("lat").is_null()).select("establishment", "tier"))
        return

    save_parquet(df, _OUT_REGISTER)
    spatial = df.filter(pl.col("lat").is_not_null())
    spatial = spatial.with_columns(
        pl.struct(["lon", "lat"])
        .map_elements(lambda s: shapely.to_wkb(shapely.Point(s["lon"], s["lat"])), return_dtype=pl.Binary)
        .alias("wkb")
    ).drop("lon", "lat")
    save_parquet(spatial, _OUT_LAYER, geoparquet=True, source_crs="EPSG:4326_XY")
    save_coverage(
        {
            "source": _BASE + "(12 region/tier pages)",
            "rows": df.height,
            "upper_tier": df.filter(pl.col("tier") == "upper").height,
            "lower_tier": df.filter(pl.col("tier") == "lower").height,
            "geocoded_rows": n_geo,
            "geocoded_name_join": n_join,
            "geocoded_nominatim_address": n_addr,
            "geocoded_nominatim_town": n_town,
            "geocode_method": (
                "layered: (1) name-join to epa_licensed_facilities (token overlap >= 0.5, "
                "county-gated, precision=site); (2) Nominatim on the published address "
                "(precision=address — postal address, approximate); (3) Nominatim on the "
                "town+county tail (precision=town — TOWN CENTROID, never a site location). "
                "Rows failing all three carry NULL geometry and are excluded from the spatial "
                "layer; per-row geocode_source/geocode_precision/geocode_note carry the audit trail"
            ),
            "grain": "one row per establishment x tier (HSA register as published)",
            "not_available": "consultation distances — held by planning authorities per establishment, not centrally published; the spatial layer marks WHERE establishments are, never the size of the zone around them",
            "dead_index_links_skipped": dead_links,
            "licence": "hsa.ie public register pages; facts-only extraction; geocoding © OpenStreetMap contributors (ODbL) via Nominatim",
        },
        _COV,
    )
    print(
        f"OK {_OUT_REGISTER.name}: {df.height} establishments | layer: {spatial.height} geocoded "
        f"({n_join} EPA join, {n_addr} address, {n_town} town)"
    )


if __name__ == "__main__":
    run_extractor(main)
