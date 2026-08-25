"""Tailte Éireann cadastral parcels — the registered-title geometry for a point.

Both registers, selected with `--tenure`: freehold (3.1M parcels, national) and leasehold
(131,073, added 2026-08-25). They are the same service family with the same two attributes, so
one code path serves both; everything that differs lives in the `TENURES` table below.

WHY THIS IS NOT A PLANNING LAYER. Every file under data/silver/parquet/planning_layers/ is a
CONSTRAINT: some rulebook node in planning/product/core/engine.py fires on it. This one is an
INPUT HELPER — no node queries it. It exists to turn a map click into a candidate site geometry,
so the engine can be asked about a boundary instead of a dimensionless pin. It therefore lands in
a sibling directory and must stay OUT of LayerStore.available(), or a boundary sweep would treat
3.1M title parcels as if they were designations.

⚠ THE PARCEL IS NOT THE RED-LINE SITE. Measured against 60 real one-off-house pins from
planning_applications_silver (2026-07-27): 98.3% of pins fall inside a freehold parcel, but the
matched parcel areas run p25 0.29 / p50 0.82 / p75 6.02 / max 60.07 ha against a DECLARED
application-site p50 of 0.30 ha. The title parcel is frequently the whole landholding (a farm),
not the application site. Treat it as a starting geometry to trim, never as the red line.

⚠ GENERALISED GEOMETRY. Tailte's open data licence states the boundaries are "generalised
resulting in reduction and simplification of features which may affect accuracies and should be
used for reference purposes only". Same standing as HM Land Registry's INSPIRE polygons: an
index, not the title plan. Never present a parcel edge as a legal boundary.

⚠ LEASEHOLD IS NOT A NATIONAL COMPLEMENT TO FREEHOLD. Dublin holds 65,019 of the 131,073
leasehold parcels (49.6%, from the live per-county groupBy on 2026-08-25) because leasehold
title is an urban apartment/commercial instrument. Outside the cities, "no leasehold parcel
here" is the expected answer and carries no information about the site.

Source  : https://data.gov.ie — "High Value Dataset - Cadastral Parcels Freehold" / "… Leasehold"
          (Tailte Éireann)
Licence : CC BY 4.0 (per the ArcGIS item licenceInfo, both items, re-read 2026-08-25)
Writes  : data/silver/parquet/cadastre/parcels_freehold.parquet
          data/silver/parquet/cadastre/parcels_leasehold.parquet

Paged per COUNTY rather than by global offset: each county is spatially coherent, so a
Morton sort WITHIN the county gives row groups tight enough for statistics pruning, while peak
memory stays at one county (Dublin, the largest, is ~331k rows) instead of 3.1M.

Resumable since 2026-08-21: each county lands in its own part file under cadastre/parts/ and the
national parquet is assembled only when every part exists and the total clears ROW_FLOOR. A
one-county run writes its part and stops — it can no longer replace the national file (the
previous `--county` path lowered the floor to 1 and wrote straight onto DEST).

    python -m extractors.cadastre_parcels_fetch                       # freehold; fetch + assemble
    python -m extractors.cadastre_parcels_fetch --tenure leasehold    # the leasehold register
    python -m extractors.cadastre_parcels_fetch --county Galway       # one part only; DEST untouched
    python -m extractors.cadastre_parcels_fetch --assemble            # assemble from existing parts
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before polars/numpy load. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import json
import logging
import urllib.parse
from pathlib import Path
from typing import NamedTuple

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import shape

from services.http_engine import fetch_json
from services.logging_setup import setup_standalone_logging

LOG = logging.getLogger("cadastre_parcels")

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "silver" / "parquet" / "cadastre"

PAGE = 1000  # the service's maxRecordCount
ROW_GROUP = 20_000  # matches planning/product/tools/build_point_scoped_layers.py — prunable groups

_ARCGIS = "https://services-eu1.arcgis.com/FH5XCsx8rYXqnjF5/arcgis/rest/services"


class Tenure(NamedTuple):
    """Everything that differs between the freehold and leasehold registers.

    The two services are schema-identical (SP_ID, COUNTY_NAM, polygon) and differ only in
    endpoint and size, so one code path serves both. ⚠ `parts` MUST differ per tenure: the
    parts directory is keyed by county name alone, so a shared directory would let a leasehold
    run assemble freehold parts into a file labelled leasehold, silently and with no row-floor
    complaint. Separate directories are the only thing preventing that.
    """

    name: str
    service: str
    dest_name: str
    parts_name: str
    expected_total: int
    row_floor: int


TENURES: dict[str, Tenure] = {
    # Published national total, cross-checked two ways on 2026-07-27 (returnCountOnly, and the
    # sum of the per-county groupBy). The floor is deliberately slack: a re-register can move it.
    "freehold": Tenure(
        "freehold",
        f"{_ARCGIS}/Cadastral_Parcels_Freehold/FeatureServer/12/query",
        "parcels_freehold.parquet",
        "parts",
        3_086_691,
        2_700_000,
    ),
    # Added 2026-08-25. 131,073 parcels over 27 counties, both figures from the live service
    # (returnCountOnly, and the per-county groupBy summing to the same total). Leasehold title is
    # overwhelmingly urban — Dublin alone holds 65,019 (49.6%) — so it is NOT a national
    # complement to freehold coverage, and a point outside Dublin/Cork returning no leasehold
    # parcel says almost nothing. Same slack-floor convention as freehold, at ~88% of today's
    # count.
    "leasehold": Tenure(
        "leasehold",
        f"{_ARCGIS}/Cadastral_Parcels_Leasehold/FeatureServer/13/query",
        "parcels_leasehold.parquet",
        "parts_leasehold",
        131_073,
        115_000,
    ),
}

# The ACTIVE tenure. These stay module-level rather than becoming parameters because the test
# suite monkeypatches DEST/PARTS_DIR/ROW_FLOOR directly; `use_tenure()` is the only supported
# way to move them, and `main()` calls it before any fetch.
TENURE = TENURES["freehold"]
SERVICE = TENURE.service
DEST = OUT_DIR / TENURE.dest_name
PARTS_DIR = OUT_DIR / TENURE.parts_name
EXPECTED_TOTAL = TENURE.expected_total
ROW_FLOOR = TENURE.row_floor


def use_tenure(name: str) -> Tenure:
    """Point the module at one register. Returns the selected tenure."""
    global TENURE, SERVICE, DEST, PARTS_DIR, EXPECTED_TOTAL, ROW_FLOOR
    try:
        TENURE = TENURES[name]
    except KeyError:
        raise SystemExit(f"unknown tenure {name!r}; choices: {', '.join(TENURES)}") from None
    SERVICE = TENURE.service
    DEST = OUT_DIR / TENURE.dest_name
    PARTS_DIR = OUT_DIR / TENURE.parts_name
    EXPECTED_TOTAL = TENURE.expected_total
    ROW_FLOOR = TENURE.row_floor
    return TENURE

SCHEMA = pa.schema(
    [
        ("wkb", pa.binary()),
        ("sp_id", pa.string()),
        ("county", pa.string()),
        ("area_m2", pa.float64()),
        ("bbox_minx", pa.float32()),
        ("bbox_miny", pa.float32()),
        ("bbox_maxx", pa.float32()),
        ("bbox_maxy", pa.float32()),
    ]
)


def _counties() -> list[str | None]:
    """Distinct COUNTY_NAM values, largest first — the paging plan."""
    params = {
        "where": "1=1",
        "outFields": "COUNTY_NAM",
        "outStatistics": json.dumps(
            [{"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "n"}]
        ),
        "groupByFieldsForStatistics": "COUNTY_NAM",
        "f": "json",
    }
    payload, _ = fetch_json(f"{SERVICE}?{urllib.parse.urlencode(params)}")
    rows = [(f["attributes"]["COUNTY_NAM"], f["attributes"]["n"]) for f in payload.get("features", [])]
    rows.sort(key=lambda kv: -kv[1])
    LOG.info("counties=%d parcels=%s", len(rows), f"{sum(n for _, n in rows):,}")
    return [c for c, _ in rows]


def _where(county: str | None) -> str:
    if county is None:
        return "COUNTY_NAM IS NULL"
    return "COUNTY_NAM = '" + county.replace("'", "''") + "'"


def _fetch_county(county: str | None) -> list[tuple]:
    """Every parcel in one county as (wkb, sp_id, county, area_m2)."""
    out: list[tuple] = []
    offset = 0
    while True:
        params = {
            "where": _where(county),
            "outFields": "SP_ID,COUNTY_NAM,Shape__Area",
            "returnGeometry": "true",
            "outSR": "4326",
            "resultOffset": str(offset),
            "resultRecordCount": str(PAGE),
            "f": "geojson",
        }
        payload, _ = fetch_json(f"{SERVICE}?{urllib.parse.urlencode(params)}")
        feats = payload.get("features") or []
        for ft in feats:
            geom = ft.get("geometry")
            if not geom:
                continue  # attribute-only row: nothing to place, so nothing to store
            props = ft.get("properties") or {}
            out.append(
                (
                    shapely.to_wkb(shape(geom)),
                    props.get("SP_ID"),
                    props.get("COUNTY_NAM"),
                    props.get("Shape__Area"),
                )
            )
        if len(feats) < PAGE:
            return out
        offset += PAGE


def _outward_f32(vals: np.ndarray, *, toward_pos: bool) -> np.ndarray:
    """f64 → f32 nudged one ULP OUTWARD so the stored bound brackets the true value.

    ORDER IS THE CONTRACT: cast first, THEN nextafter in f32 space. Doing it the other way
    (nextafter in f64, then cast) is silently wrong — the cast rounds to nearest and shrinks
    the box on most rows, so a point inside a parcel gets filtered out before the exact
    predicate runs. Same helper and same trap as planning/product/tools/build_point_scoped_layers.py:66;
    duplicated because tools/ is not an importable package.
    """
    f32 = vals.astype(np.float32)
    return np.nextafter(f32, np.float32(np.inf if toward_pos else -np.inf))


def _morton(x: np.ndarray, y: np.ndarray, bits: int = 16) -> np.ndarray:
    """Interleaved-bit spatial key: sorting on it keeps a row group spatially tight."""

    def _scale(v: np.ndarray) -> np.ndarray:
        span = float(np.ptp(v)) or 1.0
        return np.clip(((v - v.min()) / span * (2**bits - 1)).astype(np.uint64), 0, 2**bits - 1)

    xi, yi = _scale(x), _scale(y)
    z = np.zeros_like(xi)
    for i in range(bits):
        z |= ((xi >> np.uint64(i)) & np.uint64(1)) << np.uint64(2 * i)
        z |= ((yi >> np.uint64(i)) & np.uint64(1)) << np.uint64(2 * i + 1)
    return z


def _to_table(rows: list[tuple]) -> pa.Table:
    """Morton-sorted arrow table with OUTWARD-rounded f32 bbox columns.

    Outward rounding is the correctness invariant the point-scoped store relies on: an f32
    bbox must be a SUPERSET of the true f64 bounds, so a window filter can only ever
    over-select. See PointScopedLayerStore in planning/product/core/layers.py.
    """
    df = pl.DataFrame(rows, schema=["wkb", "sp_id", "county", "area_m2"], orient="row")
    bounds = shapely.bounds(shapely.from_wkb(df["wkb"].to_numpy()))
    df = (
        df.with_columns(
            pl.Series("bbox_minx", _outward_f32(bounds[:, 0], toward_pos=False)),
            pl.Series("bbox_miny", _outward_f32(bounds[:, 1], toward_pos=False)),
            pl.Series("bbox_maxx", _outward_f32(bounds[:, 2], toward_pos=True)),
            pl.Series("bbox_maxy", _outward_f32(bounds[:, 3], toward_pos=True)),
            pl.Series("_z", _morton((bounds[:, 0] + bounds[:, 2]) / 2, (bounds[:, 1] + bounds[:, 3]) / 2)),
        )
        .sort("_z")
        .drop("_z")
    )

    # Verify the superset invariant on the SORTED frame, before anything reaches disk — the
    # same refuse-to-write guard as planning/product/tools/build_point_scoped_layers.py:102.
    sorted_bounds = shapely.bounds(shapely.from_wkb(df["wkb"].to_numpy()))
    ok = (
        (df["bbox_minx"].to_numpy() <= sorted_bounds[:, 0]).all()
        and (df["bbox_miny"].to_numpy() <= sorted_bounds[:, 1]).all()
        and (df["bbox_maxx"].to_numpy() >= sorted_bounds[:, 2]).all()
        and (df["bbox_maxy"].to_numpy() >= sorted_bounds[:, 3]).all()
    )
    if not ok:
        raise AssertionError("outward-rounded bbox is not a superset — refusing to write")
    return df.to_arrow().cast(SCHEMA)


def _part_path(county: str | None) -> Path:
    slug = "_null" if county is None else "".join(ch if ch.isalnum() else "_" for ch in county).strip("_").lower()
    return PARTS_DIR / f"{slug or 'county'}.parquet"


def fetch_county_part(county: str | None, *, refresh: bool = False) -> Path | None:
    """One county → its own complete, atomically written part file. Never touches DEST.

    Resumable by construction: a part that already exists is kept unless `refresh` is set, so
    a run that dies at county 3 of 27 resumes at county 4 instead of starting over — the
    2026-08-09 national build got exactly that far and left nothing reusable behind.
    """
    part = _part_path(county)
    if part.exists() and not refresh:
        LOG.info("part exists, kept: %s", part.name)
        return part
    rows = _fetch_county(county)
    if not rows:
        LOG.warning("county %s returned 0 parcels", county)
        return None
    table = _to_table(rows)
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = part.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd", write_statistics=True, row_group_size=ROW_GROUP)
    tmp.replace(part)
    LOG.info("%-12s %7d parcels -> %s", county, table.num_rows, part.name)
    del rows, table  # one county at a time is the memory contract
    return part


def assemble(counties: list[str | None]) -> Path:
    """Stream every county part into one Morton-ordered parquet, then replace DEST atomically.

    Refuses when any county's part is missing and when the total is under ROW_FLOOR — the same
    two guarantees as before, but a refusal now costs nothing: the parts stay, and the next run
    fetches only what is missing. Atomicity and the row floor are enforced here rather than via
    services.parquet_io.save_parquet because that helper takes a materialised frame and 3.1M
    parcels will not fit in this box's headroom.
    """
    missing = [c for c in counties if not _part_path(c).exists()]
    if missing:
        raise SystemExit(
            f"assemble: {len(missing)} county part(s) missing ({', '.join(str(c) for c in missing[:6])}"
            f"{', …' if len(missing) > 6 else ''}) — fetch them first; {DEST.name} is untouched"
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    staging = DEST.with_suffix(".parquet.part")
    total = 0
    writer = pq.ParquetWriter(staging, SCHEMA, compression="zstd", write_statistics=True)
    try:
        for i, county in enumerate(counties, 1):
            table = pq.read_table(_part_path(county)).cast(SCHEMA)
            writer.write_table(table, row_group_size=ROW_GROUP)
            total += table.num_rows
            LOG.info("[%2d/%d] %-12s %7d parcels (running %s)", i, len(counties), county, table.num_rows, f"{total:,}")
            del table
    finally:
        writer.close()
    if total < ROW_FLOOR:
        staging.unlink(missing_ok=True)
        raise SystemExit(f"row floor: {total:,} parcels < {ROW_FLOOR:,} — refusing to replace {DEST.name}")
    staging.replace(DEST)
    LOG.info("wrote %s — %s parcels, %.2f GB", DEST, f"{total:,}", DEST.stat().st_size / 1e9)
    return DEST


def build(only: str | None = None, *, refresh: bool = False, assemble_only: bool = False) -> Path | None:
    """Fetch the missing county parts (or one county), then assemble the national file.

    `only` fetches ONE county's part and stops — it never replaces DEST. Until 2026-08-21 a
    one-county run wrote straight onto DEST with the row floor lowered to 1, so
    `--county Galway` would have overwritten the national (then Dublin-only) file with Galway.
    """
    stale = DEST.with_suffix(".parquet.part")
    if stale.exists():
        LOG.warning(
            "removing stale partial write from an earlier run: %s (%.0f MB)", stale.name, stale.stat().st_size / 1e6
        )
        stale.unlink()
    if only:
        return fetch_county_part(only, refresh=refresh)
    counties = _counties()
    if not assemble_only:
        for i, county in enumerate(counties, 1):
            LOG.info("[%2d/%d] fetching %s", i, len(counties), county)
            fetch_county_part(county, refresh=refresh)
    return assemble(counties)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--tenure",
        choices=sorted(TENURES),
        default="freehold",
        help="which register to fetch (default: freehold)",
    )
    ap.add_argument("--county", help="fetch ONE county's part only (never replaces the national file)")
    ap.add_argument("--refresh", action="store_true", help="re-fetch counties whose part already exists")
    ap.add_argument(
        "--assemble", action="store_true", help="assemble the national file from existing parts without fetching"
    )
    args = ap.parse_args()
    setup_standalone_logging("cadastre_parcels_fetch")
    tenure = use_tenure(args.tenure)
    LOG.info("tenure=%s -> %s (floor %s)", tenure.name, DEST.name, f"{ROW_FLOOR:,}")
    build(only=args.county, refresh=args.refresh, assemble_only=args.assemble)


if __name__ == "__main__":
    main()
