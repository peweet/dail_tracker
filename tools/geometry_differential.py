"""Differential check: `planning_applications_ingest._polygonal_geometry` (shapely, per-feature)
vs `services.geometry.polygonal_geometries` (DuckDB, batched).

THE GATE, NOT A BENCHMARK. It reports PER-ROW disagreement — reason code, geometry equality,
geometry type — because a geometry rewrite that is 99.99% identical is a provenance incident, not
a success. Nothing migrates until this reports zero unexplained disagreements.

⚠ COVERAGE, NOT JUST AGREEMENT. A reason code with no observed row is UNTESTED, not passing. The
run prints an observed-reason histogram and exits non-zero if any reason code went unexercised,
because the stored silver parquet cannot produce `repaired`/`unreadable`/`bounds_escape` rows at
all — it is already post-repair and post-filter. That is exactly the blind spot this harness
exists to cover, so a clean run against `--source stored` alone proves very little.

    python -m tools.geometry_differential --source synthetic      # every reason code, offline
    python -m tools.geometry_differential --source stored         # 20k real silver polygons
    python -m tools.geometry_differential --source live --authority "Carlow County Council"

`--source live` is the only mode that exercises repair on real upstream data; the other two are
fast pre-checks.
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401
# isort: on

import argparse
from pathlib import Path

import shapely
from shapely.geometry import mapping

from planning.civic.extractors import planning_applications_ingest as ingest
from services.geometry import polygonal_geometries

ROOT = Path(__file__).resolve().parents[1]
STORED = ROOT / "data/silver/parquet/planning_application_sites.parquet"

# One geometry per reason code the original can emit, so a run cannot report "all agree" while
# silently never exercising a branch. `bounds_escape` sits outside IRELAND_BBOX on purpose.
SYNTHETIC: list[tuple[str, dict | None]] = [
    ("ok", {"type": "Polygon", "coordinates": [[[-9, 53], [-8.9, 53], [-8.9, 53.1], [-9, 53.1], [-9, 53]]]}),
    ("empty/none", None),
    ("empty/degenerate", {"type": "Polygon", "coordinates": [[[-9, 53], [-9, 53], [-9, 53], [-9, 53]]]}),
    ("unreadable", {"type": "NotAThing", "coordinates": "x"}),
    ("not_polygonal", {"type": "Point", "coordinates": [-6.3, 53.3]}),
    ("bounds_escape", {"type": "Polygon", "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]]}),
    # bowtie: self-intersecting, so make_valid must fire -> `repaired`
    ("repaired", {"type": "Polygon", "coordinates": [[[-9, 53], [-8.9, 53.1], [-8.9, 53], [-9, 53.1], [-9, 53]]]}),
    (
        "geometrycollection",
        {
            "type": "GeometryCollection",
            "geometries": [
                {"type": "Polygon", "coordinates": [[[-9, 53], [-8.9, 53], [-8.9, 53.1], [-9, 53.1], [-9, 53]]]},
                {"type": "Point", "coordinates": [-8.95, 53.05]},
            ],
        },
    ),
]


def _load_stored(limit: int) -> list[dict]:
    import polars as pl

    if not STORED.exists():
        raise SystemExit(f"stored source missing: {STORED}")
    frame = pl.read_parquet(STORED, columns=["wkb"]).head(limit)
    return [mapping(g) for g in shapely.from_wkb(frame["wkb"].to_numpy())]


def _load_live(authority: str | None, max_pages: int) -> list[dict]:
    where = "1=1" if not authority else f"PlanningAuthority = '{authority.replace(chr(39), chr(39) * 2)}'"
    features: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        response = ingest._query(
            ingest.L1,
            where=where,
            outFields=",".join(ingest.SITE_FIELDS),
            returnGeometry="true",
            outSR="4326",
            resultOffset=offset,
            resultRecordCount=ingest.PAGE,
            orderByFields="OBJECTID",
            f="geojson",
        )
        page = response.get("features", [])
        if not page:
            break
        features.extend(feature.get("geometry") for feature in page)
        offset += len(page)
        if len(page) < ingest.PAGE:
            break
    return features


def _reference(value: dict | None) -> tuple[object | None, str]:
    """The shapely original, with its raises captured rather than propagated.

    ⚠ It DOES raise, on real input shapes: it catches only `(TypeError, ValueError)`, but shapely
    raises `GeometryTypeError` (a ShapelyError, NOT a ValueError) for an unknown geometry type,
    `KeyError` for a missing `coordinates`, and `AttributeError` for a missing `type`. Those
    escape `_polygonal_geometry` and abort the whole ingest instead of being counted `unreadable`.
    Captured here as the pseudo-reason `RAISED:<type>` so the differential can REPORT the
    divergence instead of dying at the first one.
    """
    try:
        return ingest._polygonal_geometry(value)
    except Exception as exc:  # noqa: BLE001 - the point is to observe whatever escapes
        return None, f"RAISED:{type(exc).__name__}"


def _compare(geometries: list[dict | None], labels: list[str] | None) -> int:
    expected = [_reference(value) for value in geometries]
    actual = polygonal_geometries(geometries, ireland_bbox=ingest.IRELAND_BBOX)

    mismatches = 0
    seen: dict[str, int] = {}
    for position, ((ref_geom, ref_reason), got) in enumerate(zip(expected, actual, strict=True)):
        seen[ref_reason] = seen.get(ref_reason, 0) + 1
        label = labels[position] if labels else f"row {position}"
        if ref_reason != got.reason:
            print(f"  REASON  {label}: shapely={ref_reason!r} duckdb={got.reason!r}")
            mismatches += 1
            continue
        if ref_geom is None:
            continue
        rebuilt = shapely.from_wkb(got.wkb)
        if not ref_geom.equals(rebuilt):
            print(f"  GEOMETRY {label}: shapely={ref_geom.geom_type} duckdb={rebuilt.geom_type} not equal")
            mismatches += 1
        elif ref_geom.geom_type != rebuilt.geom_type:
            # Equal ground, different label — ST_CollectionExtract returns MULTIPOLYGON where
            # shapely's union_all collapses a single member to POLYGON. Reported, not counted:
            # it cannot change a reason code, but it CAN change a stored geometry_types tuple.
            print(f"  note    {label}: equal geometry, type differs ({ref_geom.geom_type} vs {rebuilt.geom_type})")

    print(f"\n  rows={len(geometries)} mismatches={mismatches}")
    print(f"  reasons observed: {dict(sorted(seen.items()))}")
    unexercised = {"ok", "empty", "unreadable", "not_polygonal", "bounds_escape", "repaired"} - set(seen)
    if unexercised:
        print(f"  ⚠ UNEXERCISED reason codes (untested, not passing): {sorted(unexercised)}")
    return mismatches + len(unexercised)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("synthetic", "stored", "live"), default="synthetic")
    parser.add_argument("--authority", help="live mode: restrict to one planning authority")
    parser.add_argument("--limit", type=int, default=20_000, help="stored mode: rows to read")
    parser.add_argument("--max-pages", type=int, default=2, help="live mode: pages to pull")
    args = parser.parse_args()

    if args.source == "synthetic":
        labels = [label for label, _ in SYNTHETIC]
        geometries = [geometry for _, geometry in SYNTHETIC]
    elif args.source == "stored":
        geometries, labels = _load_stored(args.limit), None
        print("  ⚠ stored source is POST-repair: `repaired`/`unreadable` cannot appear here")
    else:
        geometries, labels = _load_live(args.authority, args.max_pages), None

    print(f"differential: {args.source} ({len(geometries)} geometries)")
    failures = _compare(geometries, labels)
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
