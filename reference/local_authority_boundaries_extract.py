"""Local-authority outline extractor — tiny simplified SVG paths for the council map.

The council equivalent of ``constituency_boundaries_extract.py``. Pulls the official
Local Authority boundaries (Tailte Éireann / OSi National Statutory Boundaries 2026,
CC-BY-4.0), dissolves the ~9,000 polygon parts to ONE shape per local authority,
simplifies hard, and projects all 31 into one shared Ireland-wide SVG viewbox. The
result is a small JSON of 31 SVG path strings used to draw the clickable national
choropleth on "Who runs your county" — no JS map library, no runtime geometry, no
multi-MB file shipped (only the ~tens-of-KB JSON).

The source layer is split into thousands of boundary polygons (one detailed part per
coastline/island segment), so geometry is fetched in pages with server-side
generalisation (``maxAllowableOffset`` in degrees, outSR 4326) to keep the transfer
small, then dissolved per authority in shapely.

Source : OSi National Statutory Boundaries — Local Authorities (Ungeneralised) 2026
         FeatureServer, name field ENG_NAME_VALUE = "CORK COUNTY COUNCIL".
Writes : data/_meta/local_authority_outlines.json  (--write; committed reference)

Integrity self-check (before --write): all 31 canonical local authorities (the
``local_authority`` column of data/_meta/la_chief_executives.csv) have a path.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import math
import sys
from pathlib import Path

import requests

from paths import PROJECT_ROOT as _ROOT

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_LAYER = (
    "https://services-eu1.arcgis.com/FH5XCsx8rYXqnjF5/arcgis/rest/services/"
    "National_Statutory_Boundaries_-_Local_Authorities__Ungeneralised_-_2026/FeatureServer/3"
)
_CACHE = Path("c:/tmp/local_authority_boundaries_2026.geojson")  # NOT committed
_ROSTER = _ROOT / "data" / "_meta" / "la_chief_executives.csv"
_OUT = _ROOT / "data" / "_meta" / "local_authority_outlines.json"

_VIEW = 1000.0  # SVG viewbox is 0..1000 on the long axis
_SERVER_OFFSET_DEG = 0.004  # server-side generalisation (~400 m) to shrink the transfer
_SIMPLIFY_DEG = 0.01  # final shapely simplify (~1 km) — a coarse choropleth, not a precise map
_MIN_PART_DEG2 = 0.0015  # drop polygon parts smaller than this (tiny islands)
_PAGE = 2000  # FeatureServer maxRecordCount

# OSi ENG_NAME_VALUE → canonical local_authority (data/_meta/la_chief_executives.csv).
# Explicit, not derived: "CORK COUNTY COUNCIL"→"Cork County" but "CORK CITY COUNCIL"→
# "Cork City"; Limerick/Waterford drop the "City and County" suffix entirely.
_NAME_MAP = {
    "CARLOW COUNTY COUNCIL": "Carlow",
    "CAVAN COUNTY COUNCIL": "Cavan",
    "CLARE COUNTY COUNCIL": "Clare",
    "CORK CITY COUNCIL": "Cork City",
    "CORK COUNTY COUNCIL": "Cork County",
    "DONEGAL COUNTY COUNCIL": "Donegal",
    "DUBLIN CITY COUNCIL": "Dublin City",
    "DUN LAOGHAIRE-RATHDOWN COUNTY COUNCIL": "Dun Laoghaire-Rathdown",
    "FINGAL COUNTY COUNCIL": "Fingal",
    "GALWAY CITY COUNCIL": "Galway City",
    "GALWAY COUNTY COUNCIL": "Galway County",
    "KERRY COUNTY COUNCIL": "Kerry",
    "KILDARE COUNTY COUNCIL": "Kildare",
    "KILKENNY COUNTY COUNCIL": "Kilkenny",
    "LAOIS COUNTY COUNCIL": "Laois",
    "LEITRIM COUNTY COUNCIL": "Leitrim",
    "LIMERICK CITY AND COUNTY COUNCIL": "Limerick",
    "LONGFORD COUNTY COUNCIL": "Longford",
    "LOUTH COUNTY COUNCIL": "Louth",
    "MAYO COUNTY COUNCIL": "Mayo",
    "MEATH COUNTY COUNCIL": "Meath",
    "MONAGHAN COUNTY COUNCIL": "Monaghan",
    "OFFALY COUNTY COUNCIL": "Offaly",
    "ROSCOMMON COUNTY COUNCIL": "Roscommon",
    "SLIGO COUNTY COUNCIL": "Sligo",
    "SOUTH DUBLIN COUNTY COUNCIL": "South Dublin",
    "TIPPERARY COUNTY COUNCIL": "Tipperary",
    "WATERFORD CITY AND COUNTY COUNCIL": "Waterford",
    "WESTMEATH COUNTY COUNCIL": "Westmeath",
    "WEXFORD COUNTY COUNCIL": "Wexford",
    "WICKLOW COUNTY COUNCIL": "Wicklow",
}


def _canonical_names() -> set[str]:
    with _ROSTER.open(encoding="utf-8") as fh:
        return {row["local_authority"].strip() for row in csv.DictReader(fh)}


def fetch_geojson(dest: Path) -> Path:
    """Page the FeatureServer (server-side generalised, WGS84) into one FeatureCollection."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    feats: list[dict] = []
    offset = 0
    while True:
        r = requests.get(
            _LAYER + "/query",
            params={
                "where": "1=1",
                "outFields": "ENG_NAME_VALUE",
                "returnGeometry": "true",
                "outSR": 4326,
                "maxAllowableOffset": _SERVER_OFFSET_DEG,
                "f": "geojson",
                "resultRecordCount": _PAGE,
                "resultOffset": offset,
            },
            timeout=180,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        page = r.json().get("features", [])
        feats.extend(page)
        print(f"  fetched {len(page)} (offset {offset}); total {len(feats)}")
        if len(page) < _PAGE:
            break
        offset += _PAGE
    dest.write_text(json.dumps({"type": "FeatureCollection", "features": feats}), encoding="utf-8")
    return dest


def build_outlines(geojson_path: Path) -> dict:
    from shapely.geometry import shape
    from shapely.ops import unary_union

    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    by_name: dict[str, list] = {}
    for feat in data.get("features", []):
        raw = str((feat.get("properties") or {}).get("ENG_NAME_VALUE", "")).strip().upper()
        name = _NAME_MAP.get(raw)
        if not name or not feat.get("geometry"):
            continue
        geom = shape(feat["geometry"])
        if not geom.is_valid:
            geom = geom.buffer(0)
        by_name.setdefault(name, []).append(geom)

    shapes = {
        name: unary_union(parts).simplify(_SIMPLIFY_DEG, preserve_topology=True) for name, parts in by_name.items()
    }

    # Shared projection: one bbox over ALL authorities (Ireland extent), lon scaled by
    # cos(mid-lat) so the thumbnail isn't horizontally stretched at 53°N.
    minx = min(s.bounds[0] for s in shapes.values())
    miny = min(s.bounds[1] for s in shapes.values())
    maxx = max(s.bounds[2] for s in shapes.values())
    maxy = max(s.bounds[3] for s in shapes.values())
    cos = math.cos(math.radians((miny + maxy) / 2))
    w = (maxx - minx) * cos
    h = maxy - miny
    scale = _VIEW / max(w, h)
    vb_w = round(w * scale, 1)
    vb_h = round(h * scale, 1)

    def project(x: float, y: float) -> tuple[int, int]:
        px = (x - minx) * cos * scale
        py = (maxy - y) * scale  # flip Y (SVG origin top-left)
        return round(px), round(py)

    def ring_to_path(coords) -> str:
        pts: list[tuple[int, int]] = []
        for x, y in coords:
            p = project(x, y)
            if not pts or pts[-1] != p:
                pts.append(p)
        if len(pts) < 3:
            return ""
        d = f"M{pts[0][0]},{pts[0][1]}"
        d += "".join(f"L{px},{py}" for px, py in pts[1:])
        return d + "Z"

    def geom_to_path(geom) -> str:
        polys = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
        out = []
        for poly in polys:
            if poly.is_empty or poly.area < _MIN_PART_DEG2:  # drop tiny islands
                continue
            out.append(ring_to_path(poly.exterior.coords))  # exterior only — thumbnail
        return "".join(out)

    paths = {name: geom_to_path(s) for name, s in shapes.items()}
    return {
        "viewbox": f"0 0 {vb_w} {vb_h}",
        "source": "Tailte Éireann / OSi — Local Authority Boundaries 2026 (CC-BY-4.0)",
        "local_authorities": paths,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--geojson", type=Path, default=_CACHE)
    args = ap.parse_args()

    if not args.geojson.exists():
        print(f"Fetching local-authority boundaries (paged, generalised) → {args.geojson} ...")
        fetch_geojson(args.geojson)
    print(f"Building outlines from {args.geojson} ...")

    result = build_outlines(args.geojson)
    canon = _canonical_names()
    got = set(result["local_authorities"])
    missing = canon - got
    extra = got - canon
    print(f"  {len(got)}/{len(canon)} authorities have an outline; viewbox={result['viewbox']}")
    if missing:
        print(f"  [FAIL] missing: {sorted(missing)}")
    if extra:
        print(f"  [FAIL] unexpected: {sorted(extra)}")
    avg_len = sum(len(p) for p in result["local_authorities"].values()) // max(len(got), 1)
    print(f"  avg path length: {avg_len} chars")

    if args.write and not missing and not extra:
        _OUT.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        kb = _OUT.stat().st_size / 1024
        print(f"\n  Wrote {_OUT.relative_to(_ROOT)} ({kb:.0f} KB)")
    elif args.write:
        print("\n  REFUSING to write — authority set does not match the roster exactly.")
        sys.exit(1)


if __name__ == "__main__":
    main()
