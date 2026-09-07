"""DAFM forestry licence spatial extracts — Forestry Licence Viewer layers to silver parquet.

Why this exists: both the SCSI/Teagasc and IPAV land reports name forestry-investor demand as
what supports marginal/poorer land prices, so licence application activity near a site is that
market operating, observably — and afforestation or felling beside a site is siting-relevant
in its own right. DAFM publishes the Forestry Licence Viewer layers as zipped shapefiles on
the open data portal (Open Data Directive; the forest-roads dataset is tagged CC-BY and the
rest carry no tag — the portal's blanket reuse statement plus the owner's 2026-09-04 direction
covers ingestion; the Tailte absent-tag precedent applies).

Reads  : opendata.agriculture.gov.ie zipped shapefiles (downloaded fresh each run; 0.5-15 MB)
Writes : data/silver/parquet/dafm_<layer>.parquet — attributes + WGS84 plain-WKB `wkb` +
         `bbox` struct, the same shape the siting layer store consumes.

    python -m extractors.dafm_forestry_extract [--dry-run] [--layers name ...]
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before heavy imports. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import io
import logging
import zipfile
from pathlib import Path

import polars as pl
import shapefile  # pyshp — the same reader planning_layers_ingest uses
import shapely
import shapely.geometry
import shapely.ops
from pyproj import CRS, Transformer

from services.http_engine import fetch_bytes, polite_headers
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("dafm_forestry")

_ROOT = Path(__file__).resolve().parents[1]
_OUT = _ROOT / "data" / "silver" / "parquet"

_BASE = "https://opendata.agriculture.gov.ie/dataset"
# name -> (resource url, row floor). Floors are deliberately loose first-run values; tighten
# once a refresh history exists.
DATASETS: dict[str, tuple[str, int]] = {
    "dafm_afforestation_licences": (
        f"{_BASE}/b00925d3-49a3-45b1-bba9-8f2fab5881ad/resource/5002a227-2fb7-4ca9-99c1-4aa092fd5bff/download/flv_affor_extract-afforestation.zip",
        500,
    ),
    "dafm_private_felling_licences": (
        f"{_BASE}/3a47d23e-16ee-49b8-958b-309ba4c764f9/resource/07682ed1-cd86-4279-8b34-14caa8ba21b0/download/flv_priv_felling_extract-private-clearfell-and-thinning.zip",
        500,
    ),
    "dafm_coillte_thinning_licences": (
        f"{_BASE}/8cade858-f7ab-4ab5-a857-472858beec4d/resource/8a1357c1-7715-49f9-9de5-7716ab8fe7a5/download/flv_coillte_thin_extract-coillte-thinning.zip",
        200,
    ),
    "dafm_coillte_clearfell_licences": (
        f"{_BASE}/7063f35f-49df-4190-a9ea-3c2d06759a99/resource/202f9345-bd62-4749-86fe-fcec4efdf08d/download/flv_coillte_cf_extract-coillte-clearfell.zip",
        200,
    ),
    "dafm_forest_road_polygons": (
        f"{_BASE}/718bae55-6391-4478-bfb5-ab979bb7f7f0/resource/d4eafd80-08c3-4178-824d-ce17aae9417a/download/flv_for_roads_extract_area-forest-road.zip",
        50,
    ),
    "dafm_forest_road_polylines": (
        f"{_BASE}/5df829c2-e039-42c9-8f16-d8e210aa343f/resource/3d0de6d5-a82d-4857-ae13-08f42cc95230/download/flv_for_roads_extract_line-forest-road.zip",
        50,
    ),
    "dafm_reconstitution_underplanting": (
        f"{_BASE}/0a4ba682-30a9-416c-a1ed-aee57e8eccef/resource/12624226-a4da-406a-9d40-a6bb00cd0395/download/flv_rus_extract-reconstitution-and-underplaning.zip",
        50,
    ),
}

# Every layer must land inside this window or the CRS handling is wrong — the tripwire that
# catches a missed reprojection (raw ITM metres read as degrees land nowhere near it).
_IRELAND_LON = (-11.5, -5.0)
_IRELAND_LAT = (51.0, 56.0)


def _read_shapefile(blob: bytes):
    """(records, shapes, prj_wkt) from a zipped shapefile, wholly in memory."""
    zf = zipfile.ZipFile(io.BytesIO(blob))
    members = {name.lower().rsplit(".", 1)[-1]: name for name in zf.namelist() if "." in name}
    for required in ("shp", "dbf", "shx"):
        if required not in members:
            raise ValueError(f"zip holds no .{required} member: {sorted(members)}")
    reader = shapefile.Reader(
        shp=io.BytesIO(zf.read(members["shp"])),
        dbf=io.BytesIO(zf.read(members["dbf"])),
        shx=io.BytesIO(zf.read(members["shx"])),
    )
    prj = zf.read(members["prj"]).decode("utf-8", "replace") if "prj" in members else ""
    return reader, prj


def _to_wgs84(prj_wkt: str) -> Transformer | None:
    """Transformer to EPSG:4326, or None when the source already is geographic WGS84."""
    crs = CRS.from_wkt(prj_wkt) if prj_wkt else CRS.from_epsg(2157)  # FLV ships ITM; missing .prj -> assume it
    if crs.to_epsg() == 4326:
        return None
    return Transformer.from_crs(crs, CRS.from_epsg(4326), always_xy=True)


def build(name: str, url: str) -> pl.DataFrame:
    blob = fetch_bytes(url, headers=polite_headers(), timeout=300, validate=lambda b: b[:2] == b"PK")
    if blob is None:
        raise RuntimeError(f"{name}: download failed")
    reader, prj = _read_shapefile(blob)
    transformer = _to_wgs84(prj)
    fields = [f[0] for f in reader.fields[1:]]  # first field is the deletion flag
    rows = []
    for rec in reader.iterShapeRecords():
        geom = shapely.geometry.shape(rec.shape.__geo_interface__)
        if transformer is not None:
            geom = shapely.ops.transform(transformer.transform, geom)
        if geom.is_empty:
            continue
        xmin, ymin, xmax, ymax = geom.bounds
        row = {k: (str(v) if v is not None else None) for k, v in zip(fields, rec.record, strict=False)}
        row["wkb"] = shapely.to_wkb(geom)
        row["bbox"] = {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}
        rows.append(row)
    return pl.DataFrame(rows)


def validate(name: str, df: pl.DataFrame) -> list[str]:
    problems: list[str] = []
    if df.is_empty():
        return [f"{name}: no rows"]
    xmin = df["bbox"].struct.field("xmin")
    ymin = df["bbox"].struct.field("ymin")
    if float(xmin.min()) < _IRELAND_LON[0] or float(df["bbox"].struct.field("xmax").max()) > _IRELAND_LON[1]:
        problems.append(f"{name}: longitudes outside Ireland — CRS handling wrong")
    if float(ymin.min()) < _IRELAND_LAT[0] or float(df["bbox"].struct.field("ymax").max()) > _IRELAND_LAT[1]:
        problems.append(f"{name}: latitudes outside Ireland — CRS handling wrong")
    if df["wkb"].null_count():
        problems.append(f"{name}: null wkb rows")
    return problems


def main() -> int:
    setup_standalone_logging("dafm_forestry")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="validate only; write nothing")
    ap.add_argument("--layers", nargs="*", default=list(DATASETS), help="subset of layer names")
    args = ap.parse_args()

    status = 0
    for name in args.layers:
        url, floor = DATASETS[name]
        try:
            df = build(name, url)
        except Exception as e:  # noqa: BLE001 — one bad layer must not hide the others
            LOG.error("%s: build failed: %s: %s", name, type(e).__name__, e)
            status = 1
            continue
        problems = validate(name, df)
        if problems:
            status = 1
            for p in problems:
                LOG.error("%s", p)
            continue
        LOG.info("%s: %d rows, valid", name, df.height)
        if not args.dry_run:
            save_parquet(df, _OUT / f"{name}.parquet", min_rows=floor)
            LOG.info("wrote data/silver/parquet/%s.parquet", name)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
