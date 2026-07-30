"""Public-transport service level at a site, from the NTA's national GTFS schedule.

WHY THIS LAYER EXISTS. Every competitor site report claims "public transport coverage", and
until now we had nothing. The planning question is not "is there a bus stop nearby" — it is
"is this site served well enough that a sustainable-transport policy is satisfiable". A stop
with two school-day trips a week and a stop with a 10-minute headway are the same dot on a map
and completely different planning facts. So this ingest keeps the SERVICE LEVEL, not just the
geometry: trips per average service day at each stop, the modes serving it, and the route
short-names.

WHAT IT IS NOT. Trips-per-day is what the published timetable says, for the busiest weekday
pattern in the feed. It is not a walk-time isochrone (that needs a network route, not a
straight line), it is not capacity, and it says nothing about whether the service survives the
next NTA review. The engine must present it as a measured service level with its feed date,
never as "well served" / "poorly served" — that judgement belongs to the planning authority's
own accessibility standard.

    python -m extractors.planning_transport_gtfs             # uses the cached zip if present
    python -m extractors.planning_transport_gtfs --refresh   # re-download the feed

Output (same store + coverage convention as planning_layers_ingest):
    data/silver/parquet/planning_layers/gtfs_stops.parquet   {stop fields + wkb point}
    data/silver/parquet/planning_layers/gtfs_stops_coverage.json
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before polars/numpy load. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import io
import logging
import zipfile
from pathlib import Path

import polars as pl
import shapely

from services.coverage_io import save_coverage
from services.http_engine import fetch_bytes, polite_headers
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_transport_gtfs")

GTFS_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_All.zip"
OUT = Path(__file__).resolve().parents[1] / "data/silver/parquet/planning_layers"
CACHE = Path(__file__).resolve().parents[1] / "data/_cache/gtfs_all.zip"
IRELAND_BBOX = (-11.0, 51.0, -5.0, 56.0)  # same envelope the layer ingest quarantines against

# GTFS route_type -> the mode word a report should use (spec values 0-7 plus the extended set
# we actually see in the Irish feed).
ROUTE_TYPE = {
    0: "tram",
    1: "metro",
    2: "rail",
    3: "bus",
    4: "ferry",
    5: "cable tram",
    6: "aerial lift",
    7: "funicular",
    11: "trolleybus",
    200: "coach",
}


def download(*, use_cache: bool = True) -> bytes:
    """Fetch the national GTFS zip (or reuse a cached copy)."""
    if use_cache and CACHE.exists():
        LOG.info("using cached GTFS zip %s (%.1f MB)", CACHE, CACHE.stat().st_size / 1e6)
        return CACHE.read_bytes()
    LOG.info("downloading %s", GTFS_URL)
    body = fetch_bytes(
        GTFS_URL,
        headers=polite_headers(),
        timeout=300,
        # a WAF interstitial returns HTML with HTTP 200; a real GTFS zip starts "PK"
        validate=lambda b: b[:2] == b"PK" and len(b) > 1_000_000,
    )
    if body is None:
        raise SystemExit(f"GTFS download failed (no valid zip from {GTFS_URL})")
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_bytes(body)
    LOG.info("downloaded %.1f MB -> %s", len(body) / 1e6, CACHE)
    return body


def _read(zf: zipfile.ZipFile, name: str, columns: list[str]) -> pl.DataFrame:
    """Read one GTFS text file, keeping only `columns` (they are CSV with a header row)."""
    with zf.open(name) as fh:
        df = pl.read_csv(io.BytesIO(fh.read()), columns=columns, infer_schema_length=None)
    LOG.info("  %-16s %8d rows", name, df.height)
    return df


def build(body: bytes) -> tuple[pl.DataFrame, dict]:
    """Turn the GTFS zip into one stop-level frame carrying service level and modes."""
    zf = zipfile.ZipFile(io.BytesIO(body))
    stops = _read(zf, "stops.txt", ["stop_id", "stop_name", "stop_lat", "stop_lon"])
    routes = _read(zf, "routes.txt", ["route_id", "route_short_name", "route_type"])
    trips = _read(zf, "trips.txt", ["trip_id", "route_id", "service_id"])
    stop_times = _read(zf, "stop_times.txt", ["trip_id", "stop_id"])
    calendar = _read(
        zf, "calendar.txt", ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    )

    # SERVICE DAY CHOICE, stated so the number is auditable: we count trips on the busiest
    # single weekday pattern in the feed rather than averaging, because an average over a feed
    # that spans school terms understates term-time service and overstates summer service.
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    cal = calendar.with_columns([pl.col(d).cast(pl.Int8, strict=False) for d in days])
    trips_days = trips.join(cal, on="service_id", how="left")

    # one row per (stop, trip); a trip can call at a stop twice on a loop, which is genuinely two
    # departures, so we do NOT de-duplicate here.
    calls = stop_times.join(trips_days, on="trip_id", how="left")
    per_day = calls.group_by("stop_id").agg([pl.col(d).sum().alias(f"trips_{d}") for d in days])
    per_day = per_day.with_columns(
        pl.max_horizontal([pl.col(f"trips_{d}") for d in days]).alias("trips_busiest_day"),
        pl.max_horizontal([pl.col(f"trips_{d}") for d in ("saturday", "sunday")]).alias("trips_weekend_day"),
    ).select(["stop_id", "trips_busiest_day", "trips_weekend_day"])

    # modes + route names serving each stop
    calls_routes = (
        stop_times.join(trips.select(["trip_id", "route_id"]), on="trip_id", how="left")
        .join(routes, on="route_id", how="left")
        .unique(subset=["stop_id", "route_id"])
    )
    modes = (
        calls_routes.with_columns(
            pl.col("route_type").cast(pl.Int32, strict=False).replace_strict(ROUTE_TYPE, default="other").alias("mode")
        )
        .group_by("stop_id")
        .agg(
            pl.col("mode").unique().sort().str.join(", ").alias("modes"),
            pl.col("route_short_name").drop_nulls().unique().sort().str.join(", ").alias("routes"),
            pl.col("route_id").n_unique().alias("n_routes"),
        )
    )

    df = (
        stops.join(per_day, on="stop_id", how="left")
        .join(modes, on="stop_id", how="left")
        .with_columns(
            pl.col("stop_lat").cast(pl.Float64, strict=False),
            pl.col("stop_lon").cast(pl.Float64, strict=False),
        )
        .drop_nulls(["stop_lat", "stop_lon"])
    )

    # Same two-axis gate as the layer ingest: anything outside the Ireland envelope is
    # quarantined, never repaired (a stray 0,0 stop would otherwise sit off West Africa).
    minx, miny, maxx, maxy = IRELAND_BBOX
    before = df.height
    df = df.filter(pl.col("stop_lon").is_between(minx, maxx) & pl.col("stop_lat").is_between(miny, maxy))
    quarantined = before - df.height
    if quarantined:
        LOG.warning("quarantined %d stop(s) outside the Ireland envelope", quarantined)

    df = df.with_columns(
        pl.Series(
            "wkb", [shapely.to_wkb(shapely.Point(x, y)) for x, y in zip(df["stop_lon"], df["stop_lat"], strict=True)]
        )
    )

    feed_files = {n.filename: n.file_size for n in zf.infolist()}
    stats = {
        "layer": "gtfs_stops",
        "source_url": GTFS_URL,
        "kept": df.height,
        "quarantined": quarantined,
        "n_routes": routes.height,
        "n_trips": trips.height,
        "modes": sorted(set(m for row in df["modes"].drop_nulls() for m in row.split(", "))),
        "feed_files": sorted(feed_files),
        "service_day_basis": "busiest single weekday pattern in the feed (not an average)",
    }
    return df, stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="ignore the cached zip and re-download")
    args = ap.parse_args()
    setup_standalone_logging("planning_transport_gtfs")
    df, stats = build(download(use_cache=not args.refresh))
    dest = save_parquet(df, OUT / "gtfs_stops.parquet", min_rows=1000, compression_level=9)
    save_coverage(stats, OUT / "gtfs_stops_coverage.json")
    LOG.info("wrote %s (%d stops)", dest, df.height)
    print(f"OK gtfs_stops: {dest} | {df.height} stops | modes={stats['modes']}")


if __name__ == "__main__":
    main()
