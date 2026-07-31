"""Tailte Éireann cadastral parcels (freehold) — the registered-title geometry for a point.

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

Source  : https://data.gov.ie — "High Value Dataset - Cadastral Parcels Freehold" (Tailte Éireann)
Licence : CC BY 4.0 (per the ArcGIS item licenceInfo)
Writes  : data/silver/parquet/cadastre/parcels_freehold.parquet

Paged per COUNTY rather than by global offset: each county is spatially coherent, so a
Morton sort WITHIN the county gives row groups tight enough for statistics pruning, while peak
memory stays at one county (Dublin, the largest, is ~331k rows) instead of 3.1M.

    python -m extractors.cadastre_parcels_fetch
    python -m extractors.cadastre_parcels_fetch --county Galway   # one county, for a smoke test
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
DEST = OUT_DIR / "parcels_freehold.parquet"

SERVICE = (
    "https://services-eu1.arcgis.com/FH5XCsx8rYXqnjF5/arcgis/rest/services"
    "/Cadastral_Parcels_Freehold/FeatureServer/12/query"
)
PAGE = 1000  # the service's maxRecordCount
ROW_GROUP = 20_000  # matches planning/product/tools/build_point_scoped_layers.py — prunable groups
# Published national total, cross-checked two ways on 2026-07-27 (returnCountOnly, and the
# sum of the per-county groupBy). The floor is deliberately slack: a re-register can move it.
EXPECTED_TOTAL = 3_086_691
ROW_FLOOR = 2_700_000

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


def build(only: str | None = None) -> Path:
    """Fetch every county and stream it into one Morton-ordered parquet.

    Atomicity and the row floor are enforced here rather than via services.parquet_io.
    save_parquet, because that helper takes a materialised frame and 3.1M parcels will not fit
    in the ~1.7 GB of headroom this box has. Same two guarantees, streamed: write to
    <dest>.part, refuse to replace on a short harvest, then os.replace onto dest.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    counties = [only] if only else _counties()
    part = DEST.with_suffix(".parquet.part")
    total = 0
    writer = pq.ParquetWriter(part, SCHEMA, compression="zstd", write_statistics=True)
    try:
        for i, county in enumerate(counties, 1):
            rows = _fetch_county(county)
            if not rows:
                LOG.warning("county %s returned 0 parcels", county)
                continue
            table = _to_table(rows)
            writer.write_table(table, row_group_size=ROW_GROUP)
            total += table.num_rows
            LOG.info("[%2d/%d] %-12s %7d parcels (running %s)", i, len(counties), county, table.num_rows, f"{total:,}")
            del rows, table  # one county at a time is the memory contract
    finally:
        writer.close()

    floor = 1 if only else ROW_FLOOR
    if total < floor:
        part.unlink(missing_ok=True)
        raise SystemExit(f"row floor: {total:,} parcels < {floor:,} — refusing to replace {DEST.name}")
    part.replace(DEST)
    LOG.info("wrote %s — %s parcels, %.2f GB", DEST, f"{total:,}", DEST.stat().st_size / 1e9)
    return DEST


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--county", help="fetch ONE county only (smoke test; skips the row floor)")
    args = ap.parse_args()
    setup_standalone_logging("cadastre_parcels_fetch")
    build(only=args.county)


if __name__ == "__main__":
    main()
