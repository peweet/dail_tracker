"""pq_ida_sites_register.py — clean site-level IDA land register + honest
geocoding, built from the PQ 52988/22 disclosure (Farrell, 2022-10-25:
"the full list of IDA-owned sites").

Input is the long-format cell dump already harvested by pq_ida_land_tables.py
(`data/_sandbox/pq_ida_land_attachment_cells.parquet`) — no network here.

Why col_index, not col_name, drives the pivot
-----------------------------------------------
The docx header row's 4th cell renders as literal text "Property" (a
merged/wrapped header artefact in the source file, verified against the raw
cells: col_index==3 holds every value that's actually a county name — Louth,
Cork, Galway, ...). Trusting col_name would silently mislabel the county
column. Per the attachment-harvest recipe, col_index is the reliable key for
a table whose header text doesn't match its data; col_name is kept alongside
for audit but never used to select the column.

Geocoding
---------
"Property Town" is free text with no coordinates (e.g. "Greenore", "Cork
City", "Mountbellew Ballygar" — the last is two settlement names run
together by the docx extraction). This follows the project's established
name-matching ethic (planning/product/core/sales.py, extractors/diary_org_
match.py): match by name against a held place-name layer, use the
register's own County column to break ties, and mark a name AMBIGUOUS
rather than guess. A site that can't be placed uniquely is returned with
match_status="unmatched"/"ambiguous" and NO coordinate — never a best-guess
point silently rendered as fact (the UI one-way-gate rule: only a Verified
match is a coordinate).

Status: sandbox — not wired into pipeline.py or the siting LayerStore yet.
Run: python -m pipeline_sandbox.pq_disclosures.pq_ida_sites_register
"""

from __future__ import annotations

import logging
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import shapely

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_CELLS = Path("data/_sandbox/pq_ida_land_attachment_cells.parquet")
_QUESTION_REF = "52988/22"
_OUT_REGISTER = Path("data/_sandbox/ida_sites_register.parquet")

_LAYERS_DIR = Path("data/silver/parquet/planning_layers")
_SETTLEMENTS = _LAYERS_DIR / "osi_settlements.parquet"
_TOWNLANDS = _LAYERS_DIR / "osi_townlands.parquet"

_SENTINEL_NO_LAND = "Occupied - No Marketable Land"
MIN_NAME_CHARS = 4  # shorter than sales.py's 5 — settlement names run shorter than townlands


def fold(value: str | None) -> str:
    """Accent-fold, upper-case and reduce to A-Z0-9 and single spaces (matches
    extractors/diary_org_match.py and planning/product/core/sales.py so the same
    folded key is comparable across all three name-matching call sites)."""
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]+", " ", text.upper())).strip()


# ── Step 1: pivot the long-format cells into a clean register ──────────────


def build_register() -> pl.DataFrame:
    """317-row register: property_ref, property_name, property_town, county,
    net_ha_marketable (float, null where the sentinel applies),
    occupied_no_marketable_land (bool)."""
    cells = pl.scan_parquet(_CELLS).filter(pl.col("question_ref") == _QUESTION_REF)
    wide = (
        cells.collect()
        .pivot(index="row_index", on="col_index", values="value")
        .rename(
            {
                "0": "property_ref",
                "1": "property_name",
                "2": "property_town",
                "3": "county",
                "4": "net_ha_marketable_raw",
            }
        )
        .sort("row_index")
        .drop("row_index")
    )
    return wide.with_columns(
        occupied_no_marketable_land=pl.col("net_ha_marketable_raw") == _SENTINEL_NO_LAND,
        net_ha_marketable=pl.col("net_ha_marketable_raw")
        .str.strip_chars()
        .replace(_SENTINEL_NO_LAND, None)
        .cast(pl.Float64, strict=False),
        source_question_ref=pl.lit(_QUESTION_REF),
    ).drop("net_ha_marketable_raw")


# ── Step 2: name-matching to a coordinate ───────────────────────────────────


@dataclass(frozen=True)
class _Place:
    name: str
    lon: float
    lat: float
    county: str | None  # None for settlements until resolved via the townland index


def _centroid_lonlat(wkb_bytes: bytes) -> tuple[float, float]:
    c = shapely.from_wkb(wkb_bytes).centroid
    return float(c.x), float(c.y)


def _load_townland_index() -> tuple[dict[str, list[_Place]], "shapely.STRtree", list[str]]:
    """folded ENGLISH name -> list of _Place (one per townland with that name),
    plus an STRtree of every townland polygon (for settlement county resolution)
    and the parallel COUNTY list indexed the same as the tree's geometry order."""
    df = pl.read_parquet(_TOWNLANDS, columns=["ENGLISH", "COUNTY", "wkb"])
    name_index: dict[str, list[_Place]] = {}
    geoms = []
    counties = []
    for row in df.iter_rows(named=True):
        geom = shapely.from_wkb(row["wkb"])
        lon, lat = float(geom.centroid.x), float(geom.centroid.y)
        key = fold(row["ENGLISH"])
        if len(key) >= MIN_NAME_CHARS:
            name_index.setdefault(key, []).append(
                _Place(name=row["ENGLISH"], lon=lon, lat=lat, county=row["COUNTY"])
            )
        geoms.append(geom)
        counties.append(row["COUNTY"])
    return name_index, shapely.STRtree(geoms), counties


def _load_settlement_index(
    townland_tree: "shapely.STRtree", townland_counties: list[str]
) -> dict[str, list[_Place]]:
    """folded SETTL_NAME -> list of _Place, county resolved by point-in-polygon
    against the townland layer (settlements carry no COUNTY column of their
    own). A settlement centroid landing outside every townland polygon (can
    happen right at a coastal/administrative edge) keeps county=None — it can
    still match by name, it just can't break a same-name tie."""
    df = pl.read_parquet(_SETTLEMENTS, columns=["SETTL_NAME", "wkb"])
    index: dict[str, list[_Place]] = {}
    for row in df.iter_rows(named=True):
        centroid = shapely.from_wkb(row["wkb"]).centroid
        hit_idx = townland_tree.query(centroid, predicate="within")
        county = townland_counties[hit_idx[0]] if len(hit_idx) else None
        key = fold(row["SETTL_NAME"])
        if len(key) >= MIN_NAME_CHARS:
            index.setdefault(key, []).append(
                _Place(name=row["SETTL_NAME"], lon=float(centroid.x), lat=float(centroid.y), county=county)
            )
    return index


@dataclass(frozen=True)
class SiteMatch:
    match_status: str  # "matched" | "ambiguous" | "unmatched"
    match_method: str
    lon: float | None
    lat: float | None
    matched_place_name: str | None


_STOPWORDS = {
    "BUSINESS", "PARK", "ESTATE", "INDUSTRIAL", "TECHNOLOGY", "SCIENCE",
    "STRATEGIC", "SITE", "CENTRE", "CENTER", "TECH", "ADVANCE", "BUILDING",
    "BUILDINGS", "ROAD", "RD", "LANE", "MODEL", "FARM", "NATIONAL", "IND",
    "EST", "B", "T",
}


def _ngram_candidates(folded_text: str, max_n: int = 3) -> set[str]:
    """Contiguous 1-3 word runs of `folded_text`, stopword-filtered, each a
    candidate lookup key into the place indexes."""
    tokens = [t for t in folded_text.split() if t not in _STOPWORDS]
    grams: set[str] = set()
    for n in range(1, max_n + 1):
        for i in range(len(tokens) - n + 1):
            gram = " ".join(tokens[i : i + n])
            if len(gram) >= MIN_NAME_CHARS:
                grams.add(gram)
    return grams


def _match_one(
    name: str, town: str, county: str,
    townland_idx: dict[str, list[_Place]], settlement_idx: dict[str, list[_Place]],
) -> SiteMatch:
    folded_town = fold(town)
    folded_county = fold(county)

    # 1. Townland exact name, disambiguated by the register's own County column
    #    — the strongest signal available, mirrors sales.py's county pre-filter.
    if len(folded_town) >= MIN_NAME_CHARS:
        tl_hits = townland_idx.get(folded_town, [])
        tl_same_county = [p for p in tl_hits if fold(p.county or "") == folded_county]
        if len(tl_same_county) == 1:
            p = tl_same_county[0]
            return SiteMatch("matched", "townland name unique within register county", p.lon, p.lat, p.name)

        # 2. Settlement exact name (fits "Property Town" values better than townlands)
        settle_hits = settlement_idx.get(folded_town, [])
        if len(settle_hits) == 1:
            p = settle_hits[0]
            return SiteMatch("matched", "settlement name unique nationally", p.lon, p.lat, p.name)
        if len(settle_hits) > 1:
            same_county = [p for p in settle_hits if p.county and fold(p.county) == folded_county]
            if len(same_county) == 1:
                p = same_county[0]
                return SiteMatch("matched", "settlement name + register county disambiguation", p.lon, p.lat, p.name)

    # 3. Search property_name + property_town together for a place name run —
    #    recovers rows like "Ringaskiddy Estate" / "Cork City" where the real
    #    identifying name sits in property_name, not property_town. Each
    #    candidate gram is disambiguated by register county the same way; a
    #    gram whose candidates don't collapse to one place is dropped, not
    #    guessed, and >1 surviving DISTINCT place still reports ambiguous.
    # County agreement is REQUIRED here, unlike the property_town-only steps
    # above: an n-gram is noise pulled from two concatenated free-text fields,
    # so a "nationally unique" name is a much weaker signal than it is for a
    # clean single-field match — a county name embedded in "Galway City"
    # nationally-uniquely matches a townland literally called Galway 70km
    # from Galway city (measured 2026-08-01: 15/55 ngram-fallback matches
    # landed outside their own county's bounding box before this check was
    # added; the property_town-only steps had zero such failures).
    combined = fold(f"{name} {town}")
    resolved: list[tuple[str, _Place]] = []
    for gram in _ngram_candidates(combined):
        places = townland_idx.get(gram, []) + settlement_idx.get(gram, [])
        same_county = [p for p in places if fold(p.county or "") == folded_county]
        if len(same_county) == 1:
            resolved.append((gram, same_county[0]))

    if resolved:
        distinct_coords = {(round(p.lon, 3), round(p.lat, 3)) for _, p in resolved}
        if len(distinct_coords) == 1:
            gram, p = max(resolved, key=lambda gp: len(gp[0]))  # prefer the longest matched run
            return SiteMatch(
                "matched", f"place name '{gram}' found in property name/town text, county-disambiguated",
                p.lon, p.lat, p.name,
            )
        return SiteMatch(
            "ambiguous",
            f"{len(distinct_coords)} distinct places found in property name/town text, none exclusive",
            None, None, None,
        )

    if len(folded_town) >= MIN_NAME_CHARS and (townland_idx.get(folded_town) or settlement_idx.get(folded_town)):
        return SiteMatch("ambiguous", "name resolves to more than one place and county did not disambiguate", None, None, None)
    return SiteMatch("unmatched", "no townland or settlement carries this name", None, None, None)


def geocode_register(register: pl.DataFrame) -> pl.DataFrame:
    """Add match_status / match_method / lon / lat / matched_place_name to the
    register. Never fabricates a coordinate: 'ambiguous' and 'unmatched' rows
    carry lon=lat=None by construction."""
    townland_idx, townland_tree, townland_counties = _load_townland_index()
    settlement_idx = _load_settlement_index(townland_tree, townland_counties)

    matches = [
        _match_one(row["property_name"] or "", row["property_town"] or "", row["county"] or "", townland_idx, settlement_idx)
        for row in register.iter_rows(named=True)
    ]
    return register.with_columns(
        match_status=pl.Series([m.match_status for m in matches]),
        match_method=pl.Series([m.match_method for m in matches]),
        lon=pl.Series([m.lon for m in matches], dtype=pl.Float64),
        lat=pl.Series([m.lat for m in matches], dtype=pl.Float64),
        matched_place_name=pl.Series([m.matched_place_name for m in matches]),
    )


def main() -> int:
    setup_standalone_logging("pq_ida_sites_register")

    register = build_register()
    logger.info("register: %d sites", register.height)

    geocoded = geocode_register(register)
    counts = geocoded.group_by("match_status").len().sort("len", descending=True)
    logger.info("match_status breakdown:\n%s", counts)

    save_parquet(geocoded, _OUT_REGISTER, min_rows=300)
    logger.info("wrote %s", _OUT_REGISTER)
    return 0


if __name__ == "__main__":
    sys.exit(main())
